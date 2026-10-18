//! The shipping service of the FIFO queues guide: each order becomes a shipment on a FIFO queue,
//! grouped by customer and deduplicated by order, and shipments are handled four groups at a time.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_fifo --features testing -- run
//! ```

use ruststream_sqs_sns::PARTITION_KEY_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:dispatch]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shipments.fifo")]
struct Shipment {
    order: u64,
    customer: String,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber(SqsQueue::new("orders").create_if_missing())]
async fn dispatch(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome {
    let shipment = Shipment {
        order: order.id,
        customer: order.customer.clone(),
    };
    // A customer's shipments form one group, so they are handled in the order they were sent.
    // The order id is the deduplication id, so sending the same order again ships nothing more.
    let sent = shipments
        .message(&shipment)
        .group_id(&order.customer)
        .deduplication_id(order.id.to_string())
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:dispatch]

// --8<-- [start:ship]
/// Ships one shipment. Four groups are handled at once, each group in its own order.
#[subscriber(SqsQueue::new("shipments.fifo").create_if_missing(), workers(4, by_key))]
async fn ship(shipment: &Shipment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let group = ctx
        .headers()
        .get_str(PARTITION_KEY_HEADER)
        .unwrap_or_default();
    tracing::info!(shipment.order, group, "shipment sent");
    HandlerOutcome::ack()
}
// --8<-- [end:ship]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            b.include(dispatch)
                .out(Shipments, Publish::default())
                .build();
            b.include(ship);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::PARTITION_KEY_HEADER;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, Shipment, app};

    fn order(id: u64, customer: &str) -> Order {
        Order {
            id,
            customer: customer.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_shipment_travels_in_its_customers_group() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&order(1, "acme"))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .published::<Shipment>("shipments.fifo")
            .assert_called_once()
            .with_header(PARTITION_KEY_HEADER, "acme");
        tb.broker::<SqsBroker>()
            .subscriber("shipments.fifo")
            .assert_called_once()
            .with(&Shipment {
                order: 1,
                customer: "acme".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_sent_twice_ships_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for _ in 0..2 {
            tb.broker::<SqsBroker>()
                .message(&order(2, "acme"))
                .publish()
                .await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called(2);
        tb.broker::<SqsBroker>()
            .subscriber("shipments.fifo")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn each_customer_keeps_the_order_of_its_shipments() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let orders = [(1, "acme"), (2, "globex"), (3, "acme"), (4, "globex")]
            .map(|(id, customer)| order(id, customer));
        let broker = tb.broker::<SqsBroker>();
        try_join_all(orders.iter().map(|order| broker.message(order).publish())).await?;

        let shipped = tb
            .broker::<SqsBroker>()
            .subscriber("shipments.fifo")
            .received::<Shipment>();
        for (customer, orders) in [("acme", [1, 3]), ("globex", [2, 4])] {
            let of_customer: Vec<u64> = shipped
                .iter()
                .filter(|shipment| shipment.customer == customer)
                .map(|shipment| shipment.order)
                .collect();
            assert_eq!(of_customer, orders, "the shipments of {customer}");
        }
        tb.shutdown().await?;
        Ok(())
    }
}
