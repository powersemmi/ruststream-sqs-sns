//! The payments service of the Acknowledgement guide, written without the macros: each handler is
//! a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written
//! by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_acknowledgement --features testing -- run
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:book]
/// How long a payment on an account under review waits before the next try.
const REVIEW: Duration = Duration::from_secs(60);

/// The receive after which a payment under review is given up.
const LAST_RECEIVE: u32 = 3;

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    account: String,
    amount_cents: u64,
    under_review: bool,
}

impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

impl MessageInfo for Payment {
    const NAME: &'static str = "Payment";
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Entry {
    payment: u64,
    account: String,
    amount_cents: u64,
}

impl OutgoingDestination for Entry {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger-entries";
}

impl MessageHeaders for Entry {
    type Contract = NoHeaders;
}

impl MessageInfo for Entry {
    const NAME: &'static str = "Entry";
}

struct Entries;

impl OutSlot for Entries {
    const NAME: &'static str = "Entries";
    type Destination = Reads;
}

impl PublishedThrough<Entries> for Entry {}

/// The ids of the payments already booked.
#[derive(Debug, Default, Clone)]
struct Ledger {
    booked: Arc<Mutex<HashSet<u64>>>,
}

struct Book;

impl<Wire> Handle<Payment, (), Outs<(Wire,)>, (), Ledger> for Book
where
    Wire: OutEntry<Entries, Wire: Publisher>,
{
    async fn handle(
        &self,
        payment: &Payment,
        outs: &Outs<(Wire,)>,
        ctx: &mut Context<'_, (), Ledger>,
    ) -> Result<(), HandlerOutcome> {
        // SQS counts the receives of a message, this one included.
        let receives: u32 = ctx
            .headers()
            .get_str(RECEIVE_COUNT_HEADER)
            .and_then(|count| count.parse().ok())
            .unwrap_or(1);
        if payment.under_review {
            if receives >= LAST_RECEIVE {
                tracing::error!(payment.id, receives, "payment dropped, still under review");
                return Err(HandlerOutcome::drop());
            }
            return Err(HandlerOutcome::retry_after(REVIEW));
        }
        let booked = Arc::clone(&ctx.state().booked);
        if booked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&payment.id)
        {
            tracing::info!(payment.id, receives, "payment already booked");
            return Ok(());
        }
        let entry = Entry {
            payment: payment.id,
            account: payment.account.clone(),
            amount_cents: payment.amount_cents,
        };
        if outs.get(Entries).message(&entry).publish().await.is_err() {
            // Nothing is booked yet: the payment comes back and the work runs again.
            return Err(HandlerOutcome::retry());
        }
        booked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(payment.id);
        tracing::info!(payment.id, "payment booked");
        Ok(())
    }
}
// --8<-- [end:book]

// --8<-- [start:refunds]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Refund {
    payment: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Refund {
    type Form = FixedName;

    const DESTINATION: &'static str = "refunds";
}

impl MessageHeaders for Refund {
    type Contract = NoHeaders;
}

impl MessageInfo for Refund {
    const NAME: &'static str = "Refund";
}

/// The largest refund paid without a person's approval.
const APPROVAL_LIMIT_CENTS: u64 = 50_000;

/// How long a refund waits for approval before it is looked at again.
const APPROVAL: Duration = Duration::from_secs(300);

/// Settles each refund of a batch on its own: a small refund is paid and deleted, a large one
/// waits on the queue for its approval.
struct SettleRefunds;

impl Handle<[Refund], (), (), (), Ledger> for SettleRefunds {
    fn handle(
        &self,
        refunds: &[Refund],
        _outs: &(),
        _ctx: &mut Context<'_, (), Ledger>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let outcomes: Vec<HandlerOutcome> = refunds
            .iter()
            .map(|refund| {
                if refund.amount_cents > APPROVAL_LIMIT_CENTS {
                    tracing::info!(refund.payment, "refund waits for approval");
                    HandlerOutcome::retry_after(APPROVAL)
                } else {
                    tracing::info!(refund.payment, refund.amount_cents, "refund paid");
                    HandlerOutcome::ack()
                }
            })
            .collect();
        ready(Err(outcomes))
    }
}
// --8<-- [end:refunds]

// --8<-- [start:app]
fn app() -> impl App<State = Ledger> {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Ledger::default()))
        .with_broker(
            SqsBroker::new()
                .endpoint("http://localhost:4566")
                .region("us-east-1")
                .test_credentials(),
            |b| {
                b.include(
                    subscriber("payments", Book)
                        .map_source(|_| SqsQueue::new("payments"))
                        .build(),
                )
                .out(Entries, Publish::default())
                .build();
                b.include(
                    subscriber("refunds", SettleRefunds)
                        .batch(nonzero!(10))
                        .map_source(|_| SqsQueue::new("refunds"))
                        .build(),
                );
            },
        )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqs_sns::prelude::*;

    use super::{APPROVAL, Entry, Payment, REVIEW, Refund, app};

    fn payment(id: u64, under_review: bool) -> Payment {
        Payment {
            id,
            account: "acct-7".to_owned(),
            amount_cents: 2_500,
            under_review,
        }
    }

    #[tokio::test]
    async fn a_payment_delivered_twice_is_booked_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for _ in 0..2 {
            tb.broker::<SqsBroker>()
                .message(&payment(1, false))
                .publish()
                .await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqsBroker>()
            .published::<Entry>("ledger-entries")
            .assert_called_once()
            .with(&Entry {
                payment: 1,
                account: "acct-7".to_owned(),
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_payment_under_review_is_dropped_on_its_third_receive() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&payment(2, true))
            .publish()
            .await?;
        tb.advance(REVIEW).await?;
        tb.advance(REVIEW).await?;

        assert_eq!(
            tb.broker::<SqsBroker>().subscriber("payments").outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Drop],
        );
        tb.broker::<SqsBroker>()
            .published::<Entry>("ledger-entries")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_refund_waiting_for_approval_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let refunds = [(1, 500), (2, 90_000), (3, 700)].map(|(payment, amount_cents)| Refund {
            payment,
            amount_cents,
        });

        let broker = tb.broker::<SqsBroker>();
        try_join_all(
            refunds
                .iter()
                .map(|refund| broker.message(refund).publish()),
        )
        .await?;
        tb.advance(APPROVAL).await?;

        let payments: Vec<Vec<u64>> = tb
            .broker::<SqsBroker>()
            .subscriber("refunds")
            .batches::<Refund>()
            .iter()
            .map(|batch| batch.iter().map(|refund| refund.payment).collect())
            .collect();
        assert_eq!(payments, [vec![1, 2, 3], vec![2]]);
        tb.shutdown().await?;
        Ok(())
    }
}
