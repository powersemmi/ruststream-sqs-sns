//! The service of `examples/sqs_settlement_outcomes.rs` written without the macros: `Ok(())`
//! deletes the message, and an `Err` outcome is the settlement call the queue makes instead.
//!
//! ```text
//! cargo test --example manual_sqs_settlement_outcomes --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

/// The gateway declined to answer in time.
#[derive(Debug)]
struct Busy;

/// Charges a payment; stands in for a gateway that is busy on the first try of a large amount.
async fn charge_card(payment: &Payment, attempt: &str) -> Result<(), Busy> {
    tokio::task::yield_now().await;
    if payment.amount_cents > 100_000 && attempt == "1" {
        return Err(Busy);
    }
    Ok(())
}

// --8<-- [start:outcomes]
/// One payment per delivery. A malformed payment is dropped, a busy gateway gets the payment
/// again at once, and a charged payment is acknowledged.
struct Charge;

impl Handle<Payment> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if payment.amount_cents <= 0 {
            tracing::warn!(payment.id, "malformed payment dropped");
            // DeleteMessage: the payment never comes back.
            return Err(HandlerOutcome::drop());
        }
        let attempt = ctx.headers().get_str(RECEIVE_COUNT_HEADER).unwrap_or("1");
        if charge_card(payment, attempt).await.is_err() {
            tracing::warn!(payment.id, attempt, "gateway busy");
            // ChangeMessageVisibility to zero: the next receive hands it out again.
            return Err(HandlerOutcome::retry());
        }
        // DeleteMessage.
        Ok(())
    }
}
// --8<-- [end:outcomes]

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("payments", Charge).build());
    })
}

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::{Outcome, TestApp};

    use super::*;

    /// Longer than the 30 seconds a queue hides a received message for by default, so an
    /// undeleted message would be back by then.
    const PAST_VISIBILITY: Duration = Duration::from_secs(60);

    #[tokio::test(start_paused = true)]
    async fn a_charged_payment_is_deleted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Payment {
                id: 1,
                amount_cents: 500,
            })
            .publish()
            .await?;
        tb.advance(PAST_VISIBILITY).await?;

        assert_eq!(
            tb.broker::<SqsBroker>().subscriber("payments").outcomes(),
            [Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_payment_never_comes_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;
        tb.advance(PAST_VISIBILITY).await?;

        assert_eq!(
            tb.broker::<SqsBroker>().subscriber("payments").outcomes(),
            [Outcome::Drop],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_retried_payment_is_received_again_at_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Payment {
                id: 3,
                amount_cents: 250_000,
            })
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<SqsBroker>().subscriber("payments").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.shutdown().await?;
        Ok(())
    }
}
