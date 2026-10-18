//! Batches: one handler call per `ReceiveMessage`, so a batch of orders costs one round trip
//! each way. Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_batches --features testing
//! cargo run --example sqs_batches --features testing -- run
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-batches")]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// A batch handler: the slice is one `ReceiveMessage` worth of orders, settled as a whole. It
/// holds at most the size the mount site names, and as few as one when that is all the queue
/// had at the moment of the call.
#[subscriber(SqsQueue::new("orders-batches"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "reconciled a batch of orders");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:mount]
fn service(broker: SqsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders-batches", "0.1.0")).with_broker(broker, |b| {
        // The size is the framework's word and becomes `MaxNumberOfMessages`; the long
        // poll and the queue's provisioning are this crate's, chained after it.
        b.include(
            reconcile
                .batch(nonzero!(10))
                .wait(Duration::from_secs(20))
                .create_if_missing(),
        );
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

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    /// Publishes `count` orders at once, so they wait on the queue together.
    async fn publish_at_once(tb: &TestApp<()>, count: u64) -> Result<(), Box<dyn Error>> {
        let orders: Vec<Order> = (1..=count).map(|id| Order { id }).collect();
        let broker = tb.broker::<SqsBroker>();
        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn three_waiting_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        publish_at_once(&tb, 3).await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders-batches")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_holds_at_most_the_size_the_mount_site_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        publish_at_once(&tb, 12).await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders-batches")
            .assert_batch_sizes(&[10, 2]);
        tb.shutdown().await?;
        Ok(())
    }
}
