//! SNS fan-out: one order accepted on a queue, announced once to a topic, delivered to every
//! queue subscribed to it.
//!
//! ```text
//! cargo test --example sns_fanout --features testing,sns
//! cargo run --example sns_fanout --features testing,sns -- run
//! ```

use std::io;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: u64,
}

// --8<-- [start:reply]
/// The notification the topic fans out. It names the topic, so the handler answers with a bare
/// `reply` clause.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-events")]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue::new("orders").create_if_missing(), reply)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    tracing::info!(order.id, "order accepted");
    OrderPlaced { id: order.id }
}
// --8<-- [end:reply]

#[subscriber(SqsQueue::new("billing").create_if_missing())]
async fn bill(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, "order billed");
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue::new("shipping").create_if_missing())]
async fn ship(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}

// --8<-- [start:wiring]
/// Subscribes both queues to the topic. The subscriptions have opened the queues by the time a
/// startup hook runs.
async fn wire_topology(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    for queue in ["billing", "shipping"] {
        connected
            .subscribe_queue_to_topic("orders-events", queue)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}
// --8<-- [end:wiring]

// --8<-- [start:app]
fn service(broker: SqsBroker) -> impl App {
    // A clone connects through the connection the app's broker holds.
    let topology = broker.clone();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .after_startup(async move |_state| wire_topology(topology).await)
        .with_broker(broker, |b| {
            // One step moves the reply from the queue of that name to the topic.
            b.include(accept).out_reply(SnsPublish::default());
            b.include(bill);
            b.include(ship);
        })
}
// --8<-- [end:app]

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
    async fn an_order_reaches_every_subscribed_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.broker::<SqsBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        for queue in ["billing", "shipping"] {
            tb.broker::<SqsBroker>()
                .subscriber(queue)
                .assert_called_once()
                .with(&OrderPlaced { id: 7 })
                .settled(HandlerOutcome::ack());
        }
        tb.broker::<SqsBroker>()
            .published::<OrderPlaced>("orders-events")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
