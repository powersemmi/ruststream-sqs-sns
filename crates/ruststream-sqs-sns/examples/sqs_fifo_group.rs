//! FIFO message groups: each customer's orders are fulfilled in the order they were placed, and a
//! startup replay rides one group the publish policy fixes.
//!
//! ```text
//! cargo test --example sqs_fifo_group --features testing
//! cargo run --example sqs_fifo_group --features testing -- run
//! ```

use std::io;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "fulfilment.fifo")]
struct OrderPlaced {
    id: u64,
    customer: String,
}

#[derive(OutSlot)]
#[publishes(OrderPlaced)]
struct Placements;

// --8<-- [start:publish]
/// One customer's orders reach fulfilment in the order they were placed; different customers'
/// orders do not wait for each other. The group is the customer.
#[subscriber(SqsQueue::new("orders"))]
async fn place(
    order: &Order,
    Out(placements): Out<impl Publisher<Options = SqsPublishOptions>, Placements>,
) -> HandlerOutcome {
    let placed = OrderPlaced {
        id: order.id,
        customer: order.customer.clone(),
    };
    let sent = placements
        .message(&placed)
        .group_id(&order.customer)
        .publish()
        .await;
    if let Err(err) = sent {
        tracing::warn!(order.id, %err, "order not placed");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:publish]

#[subscriber(SqsQueue::new("fulfilment.fifo"))]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(order.id, order.customer, "order fulfilled");
    HandlerOutcome::ack()
}

/// The orders a previous run accepted and did not hand on; a real service reads its own store.
fn pending() -> Vec<OrderPlaced> {
    vec![OrderPlaced {
        id: 1,
        customer: "acme".to_owned(),
    }]
}

// --8<-- [start:policy]
fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place).out(Placements, Publish::default()).build();
        b.include(fulfil);
        // The replay keeps its own order: every message the hook sends is in the group `replay`.
        b.after_startup(
            Publish::default().group_id("replay"),
            async move |sqs| -> io::Result<()> {
                for order in pending() {
                    sqs.message(&order)
                        .publish()
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            },
        );
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
    use ruststream_sqs_sns::PARTITION_KEY_HEADER;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, OrderPlaced, Placements, broker, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_replay_rides_the_group_the_policy_fixed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.settle().await?;

        // A FIFO delivery carries its message group in the partition-key header.
        tb.broker::<SqsBroker>()
            .published::<OrderPlaced>("fulfilment.fifo")
            .assert_called_once()
            .with_header(PARTITION_KEY_HEADER, "replay");
        tb.broker::<SqsBroker>()
            .subscriber("fulfilment.fifo")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_rides_its_customers_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.settle().await?;
        tb.broker::<SqsBroker>()
            .message(&Order {
                id: 2,
                customer: "globex".to_owned(),
            })
            .publish()
            .await?;

        tb.out::<Placements>()
            .assert_called_once()
            .with_options(&SqsPublishOptions::default().group_id("globex"));
        tb.broker::<SqsBroker>()
            .published::<OrderPlaced>("fulfilment.fifo")
            .assert_called(2)
            .with(&OrderPlaced {
                id: 2,
                customer: "globex".to_owned(),
            })
            .with_header(PARTITION_KEY_HEADER, "globex");
        tb.shutdown().await?;
        Ok(())
    }
}
