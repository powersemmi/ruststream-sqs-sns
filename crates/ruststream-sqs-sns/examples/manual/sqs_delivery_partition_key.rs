//! The service of `examples/sqs_delivery_partition_key.rs` without the macros: the handlers are
//! named types, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_delivery_partition_key --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::PARTITION_KEY_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The slot orders are forwarded through, to the FIFO queue of the packing line.
struct Packing;

impl OutSlot for Packing {
    const NAME: &'static str = "Packing";
    // `Order` names no queue, so the call site names one.
    type Destination = Names;
}

impl PublishedThrough<Packing> for Order {}

// --8<-- [start:handler]
/// Forwards each order to the packing line in the customer's message group, so one customer's
/// orders are packed in the order they were placed.
struct Route;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Route
where
    Entry: OutEntry<Packing, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, order.customer.clone());
        let sent = outs
            .get(Packing)
            .message(order)
            .to("packing.fifo")
            .with_headers(headers)
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}

/// Reads the group back from the delivery.
struct Pack;

impl Handle<Order> for Pack {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let group = ctx
            .headers()
            .get(PARTITION_KEY_HEADER)
            .map(String::from_utf8_lossy);
        tracing::info!(order.id, ?group, "order packed");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("packing", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Route)
                .build()
                .name("orders"),
        )
        .out(Packing, Publish::default())
        .build();
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Pack)
                .build()
                .name("packing.fifo"),
        );
    })
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    customer: String,
}

// `#[derive(Outgoing)]` by hand: an order names no queue, so each publish names one.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::PARTITION_KEY_HEADER;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, app};

    fn order() -> Order {
        Order {
            id: 7,
            customer: "acme".to_owned(),
        }
    }

    #[tokio::test]
    async fn the_customer_names_the_group_on_the_fifo_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<SqsBroker>()
            .message(&order())
            .to("orders")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .published::<Order>("packing.fifo")
            .assert_called_once()
            .with(&order())
            .with_header(PARTITION_KEY_HEADER, "acme");
        tb.broker::<SqsBroker>()
            .subscriber("packing.fifo")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_standard_queue_carries_the_key_as_a_header() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "acme");
        tb.broker::<SqsBroker>()
            .message(&order())
            .to("orders")
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .published::<Order>("orders")
            .assert_called_once()
            .with_header(PARTITION_KEY_HEADER, "acme");
        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
