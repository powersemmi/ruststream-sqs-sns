//! `SqsError` where a service meets it: a publish SQS refuses, matched by its variant, and a
//! startup that fails.
//!
//! ```text
//! cargo test --example sqs_broker_errors --features testing
//! ```

use std::error::Error;

use ruststream_sqs_sns::SqsError;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The slot shipments leave through, to the warehouse queue an order names.
#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:handler]
/// Whether SQS refused a publish because its destination is not a FIFO queue. The `SqsError` is
/// the source of the publish error, or a source further down its chain.
fn not_fifo(err: &(dyn Error + 'static)) -> bool {
    let mut cause = Some(err);
    while let Some(err) = cause {
        if let Some(SqsError::NotFifo { .. }) = err.downcast_ref::<SqsError>() {
            return true;
        }
        cause = err.source();
    }
    false
}

/// Ships an order through its warehouse's FIFO queue, in the customer's message group.
#[subscriber(SqsQueue::new("orders"))]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = shipments
        .message(&shipment)
        .to(order.warehouse.as_str())
        .group_id(&order.customer)
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        // A standard queue has no message groups, and a retry would be refused again.
        Err(err) if not_fifo(&err) => {
            tracing::error!(subscription = "orders", order.id, error = %err, "not a FIFO queue");
            HandlerOutcome::drop()
        }
        Err(err) => {
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not sent");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:handler]

#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(ship).out(Shipments, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
    warehouse: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Shipment {
    order_id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::SqsError;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, Shipment};

    /// The handler of the service with a wait SQS does not allow: a long poll waits 20 seconds
    /// at most.
    #[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(30)))]
    async fn too_patient(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    fn order(warehouse: &str) -> Order {
        Order {
            id: 7,
            customer: "acme".to_owned(),
            warehouse: warehouse.to_owned(),
        }
    }

    #[tokio::test]
    async fn a_shipment_to_a_fifo_queue_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(super::app()).await?;
        tb.broker::<SqsBroker>()
            .message(&order("east.fifo"))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqsBroker>()
            .published::<Shipment>("east.fifo")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_to_a_standard_queue_drops_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(super::app()).await?;
        tb.broker::<SqsBroker>()
            .message(&order("west"))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<SqsBroker>()
            .published::<Shipment>("west")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_sqs_error() {
        let app =
            RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
                b.include(too_patient);
            });

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(sqs) = err.downcast_ref::<SqsError>() {
                assert!(matches!(sqs, SqsError::InvalidQueue(_)), "{sqs}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an SqsError: {failed:?}");
    }
}
