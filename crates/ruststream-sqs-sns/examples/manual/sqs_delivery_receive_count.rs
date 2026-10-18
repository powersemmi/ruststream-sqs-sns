//! The service of `examples/sqs_delivery_receive_count.rs` without the macros: the handler is a
//! named type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_delivery_receive_count --features testing
//! ```

use std::process::ExitCode;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// Where a payment goes for a person to look at.
struct Review;

impl OutSlot for Review {
    const NAME: &'static str = "Review";
    // `Payment` names no queue, so the call site names one.
    type Destination = Names;
}

impl PublishedThrough<Review> for Payment {}

// --8<-- [start:handler]
/// How many times the queue hands a payment over before it moves it to the dead-letter queue.
const ATTEMPTS: u32 = 3;

struct Charge;

impl<Entry> Handle<Payment, (), Outs<(Entry,)>> for Charge
where
    Entry: OutEntry<Review, Wire: Publisher>,
{
    async fn handle(
        &self,
        payment: &Payment,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if payment.card_accepted {
            return Ok(());
        }
        let receives: u32 = ctx
            .headers()
            .get(RECEIVE_COUNT_HEADER)
            .and_then(|count| str::from_utf8(count).ok()?.parse().ok())
            .unwrap_or(1);
        if receives < ATTEMPTS {
            return Err(HandlerOutcome::retry());
        }
        // The last attempt: a person looks at the payment before the queue gives up on it.
        tracing::warn!(payment.id, receives, "payment declined, sent to review");
        let sent = outs
            .get(Review)
            .message(payment)
            .to("payments-review")
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Charge)
                .build()
                .name("payments"),
        )
        .max_attempts(nonzero!(ATTEMPTS))
        .dead_letter("payments-dead")
        .out(Review, Publish::default())
        .build();
    })
}
// --8<-- [end:handler]

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    card_accepted: bool,
}

// `#[derive(Outgoing)]` by hand: a payment names no queue, so each publish names one.
impl OutgoingDestination for Payment {
    type Form = CallerName;
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Payment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_last_attempt_sends_the_payment_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            id: 7,
            card_accepted: false,
        };
        tb.broker::<SqsBroker>()
            .message(&declined)
            .to("payments")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(3);
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-review")
            .assert_called_once()
            .with(&declined);
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_accepted_payment_is_charged_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<SqsBroker>()
            .message(&Payment {
                id: 8,
                card_accepted: true,
            })
            .to("payments")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
