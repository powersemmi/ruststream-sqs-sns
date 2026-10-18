//! The shop of the Fan-out with SNS guide: an accepted order is announced once on an SNS topic,
//! and the topic delivers the announcement to the billing queue and the shipping queue.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_fan_out --features testing,sns -- run
//! ```

use std::io;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:accept]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shop-orders")]
struct PlaceOrder {
    id: u64,
    amount_cents: u64,
}

/// The announcement names the topic, so the handler answers with a bare `reply` clause.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shop-order-events")]
struct OrderPlaced {
    id: u64,
    amount_cents: u64,
}

#[subscriber(SqsQueue::new("shop-orders").create_if_missing(), reply)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    tracing::info!(order.id, "order accepted");
    OrderPlaced {
        id: order.id,
        amount_cents: order.amount_cents,
    }
}
// --8<-- [end:accept]

// --8<-- [start:consumers]
#[subscriber(SqsQueue::new("shop-billing").create_if_missing())]
async fn bill(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, order.amount_cents, "order billed");
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue::new("shop-shipping").create_if_missing())]
async fn ship(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, "order shipped");
    HandlerOutcome::ack()
}
// --8<-- [end:consumers]

// --8<-- [start:topology]
/// Subscribes the billing and shipping queues to the topic, with raw delivery.
///
/// `broker` is a clone of the app's broker, so it uses the connection the app already holds.
async fn subscribe_queues(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    for queue in ["shop-billing", "shop-shipping"] {
        connected
            .subscribe_queue_to_topic("shop-order-events", queue)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}
// --8<-- [end:topology]

// --8<-- [start:app]
fn service(broker: SqsBroker) -> RustStream {
    let topology = broker.clone();
    RustStream::new(AppInfo::new("shop", "0.1.0"))
        .after_startup(async move |_state| subscribe_queues(topology).await)
        .with_broker(broker, |b| {
            // The reply goes to the topic named by `OrderPlaced`, through SNS.
            b.include(accept).out_reply(SnsPublish::default());
            b.include(bill);
            b.include(ship);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{OrderPlaced, PlaceOrder, live, service};

    async fn an_order_reaches_billing_and_shipping(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&PlaceOrder {
                id: 7,
                amount_cents: 1_250,
            })
            .publish()
            .await?;

        let placed = OrderPlaced {
            id: 7,
            amount_cents: 1_250,
        };
        tb.broker::<SqsBroker>()
            .published::<OrderPlaced>("shop-order-events")
            .assert_called_once()
            .with(&placed);
        for queue in ["shop-billing", "shop-shipping"] {
            tb.broker::<SqsBroker>()
                .subscriber(queue)
                .assert_called_once()
                .with(&placed)
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_fans_out_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(SqsBroker::new().region("us-east-1"))).await?;
        an_order_reaches_billing_and_shipping(tb).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_fans_out_on_a_live_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        delete_queues(&endpoint).await?;
        let tb = TestApp::start_live(service(live::broker(&endpoint))).await?;
        an_order_reaches_billing_and_shipping(tb).await
    }

    /// Deletes the shop's queues with whatever an earlier run left on them, in flight or not, so
    /// the counts above see this run's messages alone. The service creates them again when it
    /// starts.
    async fn delete_queues(endpoint: &str) -> Result<(), Box<dyn Error>> {
        let admin = live::admin(endpoint).await;
        for queue in ["shop-orders", "shop-billing", "shop-shipping"] {
            // A queue the stack does not have has nothing to delete.
            let Ok(found) = admin.get_queue_url().queue_name(queue).send().await else {
                continue;
            };
            if let Some(url) = found.queue_url() {
                admin.delete_queue().queue_url(url).send().await?;
            }
        }
        Ok(())
    }
}
