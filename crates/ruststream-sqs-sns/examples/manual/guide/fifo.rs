//! The shipping service of the FIFO queues guide, written without the macros: each handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_fifo --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::PARTITION_KEY_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:dispatch]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    customer: String,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order: u64,
    customer: String,
}

impl OutgoingDestination for Shipment {
    type Form = FixedName;

    const DESTINATION: &'static str = "shipments.fifo";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

impl MessageInfo for Shipment {
    const NAME: &'static str = "Shipment";
}

struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

struct Dispatch;

impl<Wire> Handle<Order, (), Outs<(Wire,)>> for Dispatch
where
    Wire: OutEntry<Shipments, Wire: Publisher<Options = SqsPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Wire,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment {
            order: order.id,
            customer: order.customer.clone(),
        };
        // A customer's shipments form one group, so they are handled in the order they were
        // sent. The order id is the deduplication id, so sending the same order again ships
        // nothing more.
        let sent = outs
            .get(Shipments)
            .message(&shipment)
            .group_id(&order.customer)
            .deduplication_id(order.id.to_string())
            .publish()
            .await;
        if sent.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:dispatch]

// --8<-- [start:ship]
/// Ships one shipment.
struct Ship;

impl Handle<Shipment> for Ship {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let group = ctx
            .headers()
            .get_str(PARTITION_KEY_HEADER)
            .unwrap_or_default();
        tracing::info!(shipment.order, group, "shipment sent");
        ready(Ok(()))
    }
}
// --8<-- [end:ship]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            b.include(
                subscriber("orders", Dispatch)
                    .map_source(|_| SqsQueue::new("orders").create_if_missing())
                    .build(),
            )
            .out(Shipments, Publish::default())
            .build();
            // Four groups at once, each group in its own order.
            b.include(
                subscriber("shipments.fifo", Ship)
                    .map_source(|_| SqsQueue::new("shipments.fifo").create_if_missing())
                    .workers_by_key(nonzero!(4))
                    .build(),
            );
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
