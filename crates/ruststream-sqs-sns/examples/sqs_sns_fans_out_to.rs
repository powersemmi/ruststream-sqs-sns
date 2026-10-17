//! A queue an operator subscribed to a topic, named for the tests: in production the service
//! does not know `billing` is subscribed, and its tests see the copy `billing` receives.
//!
//! ```text
//! cargo test --example sqs_sns_fans_out_to --features testing,sns
//! cargo run --example sqs_sns_fans_out_to --features testing,sns -- run
//! ```

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-events")]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue::new("orders"), reply)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    tracing::info!(order.id, "order accepted");
    OrderPlaced { id: order.id }
}

/// Reads `billing`, which the operator subscribed to `orders-events` in AWS.
#[subscriber(SqsQueue::new("billing"))]
async fn bill(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, "order billed");
    HandlerOutcome::ack()
}

// --8<-- [start:policy]
/// The topic's policy. The test build names the queue the operator subscribed.
fn announcements() -> SnsPublish {
    let policy = SnsPublish::default();
    #[cfg(test)]
    let policy = policy.fans_out_to(["billing"]);
    policy
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(accept).out_reply(announcements());
        b.include(bill);
    })
}
// --8<-- [end:policy]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

#[ruststream::app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{OrderPlaced, PlaceOrder, broker, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_operators_queue_receives_the_announcement() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.broker::<SqsBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("billing")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
