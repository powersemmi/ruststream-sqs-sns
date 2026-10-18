//! Capping the attempts: the registration declares them and the queue carries a spent delivery
//! to its dead-letter queue. Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_dead_letter --features testing
//! cargo run --example sqs_dead_letter --features testing -- run
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

/// How long an invoice waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(5);

// --8<-- [start:handler]
/// A handler that is never ready. Every delivery asks to come back later, so what ends the
/// message is the cap: the queue hands it over three times and then moves it.
#[subscriber(SqsQueue::new("invoices").create_if_missing())]
async fn settle(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.id, "invoice is not ready yet");
    HandlerOutcome::retry_after(RECHECK)
}

/// The dead-letter queue is an ordinary queue, read by an ordinary subscription.
#[subscriber("invoices-dead")]
async fn escalate(invoice: &Invoice) -> HandlerOutcome {
    tracing::error!(invoice.id, "invoice escalated to a person");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:mount]
fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker, |b| {
        // The two steps are the queue's redrive policy: `maxReceiveCount` and the
        // dead-letter queue it points at. The subscription writes them when it opens.
        b.include(settle)
            .max_attempts(nonzero!(3u32))
            .dead_letter("invoices-dead");
        b.include(escalate);
    })
}
// --8<-- [end:mount]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

#[app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_third_receive_moves_the_invoice_to_the_dead_letter_queue()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Invoice { id: 7 })
            .publish()
            .await?;
        // One advance per lapsed delay: the first two hand the invoice back, the third finds
        // its receives spent and moves it.
        for _ in 0..3 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("invoices")
            .assert_called(3);
        tb.broker::<SqsBroker>()
            .subscriber("invoices-dead")
            .assert_called_once()
            .with(&Invoice { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_cap_without_a_dead_letter_queue_stops_startup() {
        let app = RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(settle).max_attempts(nonzero!(3u32));
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("half a redrive policy is not one");
        assert!(refused.to_string().contains("dead_letter(..)"), "{refused}");
    }
}
