//! A subscription described by `SqsQueue`: the queue, its long poll and its visibility timeout
//! in one value, inline in `#[subscriber(..)]`. Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_subscriptions_queue --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:descriptor]
/// Reads the queue `orders`: each receive waits up to ten seconds for a message, and a received
/// order stays hidden from other consumers for a minute.
#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(10))
        .visibility(Duration::from_secs(60))
)]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:descriptor]

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
    async fn the_descriptor_reads_its_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Order { id: 3 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
