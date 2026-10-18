//! `PARTITION_KEY_HEADER`: the portable spelling of a FIFO message group. A FIFO queue sends it as
//! the message group and hands it back on delivery; a standard queue ignores it.
//!
//! ```text
//! cargo test --example sqs_delivery_partition_key --features testing
//! ```

use ruststream_sqs_sns::PARTITION_KEY_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The slot orders are forwarded through, to the FIFO queue of the packing line.
#[derive(OutSlot)]
#[publishes(Order)]
struct Packing;

// --8<-- [start:handler]
/// Forwards each order to the packing line in the customer's message group, so one customer's
/// orders are packed in the order they were placed.
#[subscriber(SqsQueue::new("orders"))]
async fn route(order: &Order, Out(packing): Out<impl Publisher, Packing>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, order.customer.clone());
    match packing
        .message(order)
        .to("packing.fifo")
        .with_headers(headers)
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

/// Reads the group back from the delivery.
#[subscriber(SqsQueue::new("packing.fifo"))]
async fn pack(order: &Order, cx: &mut Context<'_>) -> HandlerOutcome {
    let group = cx
        .headers()
        .get(PARTITION_KEY_HEADER)
        .map(String::from_utf8_lossy);
    tracing::info!(order.id, ?group, "order packed");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("packing", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(route).out(Packing, Publish::default()).build();
        b.include(pack);
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    customer: String,
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
    async fn a_standard_queue_ignores_the_key() -> Result<(), Box<dyn Error>> {
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
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
