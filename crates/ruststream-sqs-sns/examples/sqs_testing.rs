//! Testing a service in process: `TestApp::start` connects `SqsBroker` to an account inside the
//! test, with queues, receive counts and redrive policies.
//!
//! ```text
//! cargo test --example sqs_testing --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    card_accepted: bool,
}

/// How long a declined payment waits before the card is tried again.
const RETRY_DELAY: Duration = Duration::from_mins(1);

#[subscriber(SqsQueue::new("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.card_accepted {
        tracing::info!(payment.id, "payment charged");
        return HandlerOutcome::ack();
    }
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The app `main` runs: three attempts, then the queue moves the payment aside.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments-dead");
    })
}

#[app]
fn service() -> impl App<State = ()> {
    app()
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Payment, RETRY_DELAY, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn an_accepted_card_is_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            card_accepted: true,
        };
        tb.broker::<SqsBroker>().message(&payment).publish().await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:redrive]
    #[tokio::test(start_paused = true)]
    async fn a_declined_card_ends_in_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 8,
            card_accepted: false,
        };
        tb.broker::<SqsBroker>().message(&payment).publish().await?;

        // Each delay hands the payment back; after the third receive the queue moves it.
        for _ in 0..3 {
            tb.advance(RETRY_DELAY).await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(3)
            .settled(HandlerOutcome::retry_after(RETRY_DELAY));
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:redrive]
}
