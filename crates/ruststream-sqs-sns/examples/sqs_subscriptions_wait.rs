//! The long poll of a subscription: `wait` is how long one receive waits for a message to
//! arrive. Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_subscriptions_wait --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    id: u64,
}

// --8<-- [start:wait]
/// Each receive on `alerts` waits up to five seconds and returns as soon as an alert arrives.
#[subscriber(SqsQueue::new("alerts").wait(Duration::from_secs(5)))]
async fn page(alert: &Alert) -> HandlerOutcome {
    tracing::info!(alert.id, "on-call paged");
    HandlerOutcome::ack()
}
// --8<-- [end:wait]

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(broker, |b| {
        b.include(page);
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

#[app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn an_alert_is_handled() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Alert { id: 1 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("alerts")
            .assert_called_once()
            .with(&Alert { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_wait_past_twenty_seconds_stops_startup() {
        let app = RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(page.workers(nonzero!(1)).wait(Duration::from_secs(21)));
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("SQS long-polls for at most 20 seconds");
        assert!(refused.to_string().contains("20 second"), "{refused}");
    }
}
