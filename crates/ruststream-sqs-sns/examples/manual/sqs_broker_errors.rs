//! The service of `examples/sqs_broker_errors.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_broker_errors --features testing
//! ```

use std::error::Error;
use std::process::ExitCode;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::SqsError;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The slot shipments leave through, to the warehouse queue an order names.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    // `Shipment` names no queue, so the call site names one.
    type Destination = Names;
}

impl PublishedThrough<Shipments> for Shipment {}

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
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher<Options = SqsPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment { order_id: order.id };
        let sent = outs
            .get(Shipments)
            .message(&shipment)
            .to(order.warehouse.as_str())
            .group_id(&order.customer)
            .publish()
            .await;
        match sent {
            Ok(()) => Ok(()),
            // A standard queue has no message groups, and a retry would be refused again.
            Err(err) if not_fifo(&err) => {
                tracing::error!(subscription = "orders", order.id, error = %err, "not a FIFO queue");
                Err(HandlerOutcome::drop())
            }
            Err(err) => {
                tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not sent");
                Err(HandlerOutcome::retry())
            }
        }
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Ship)
                .build()
                .name("orders"),
        )
        .out(Shipments, Publish::default())
        .build();
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
    warehouse: String,
}

// `#[outgoing(name = "orders")]` by hand: an order names the queue it is sent to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

// `#[derive(Outgoing)]` by hand: a shipment names no queue, so each publish names one.
impl OutgoingDestination for Shipment {
    type Form = CallerName;
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};
    use std::time::Duration;

    use ruststream::Unnamed;
    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::SqsError;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, Shipment};

    /// The handler of the service with a wait SQS does not allow: a long poll waits 20 seconds
    /// at most.
    struct TooPatient;

    impl Handle<Order> for TooPatient {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            let _ = order.id;
            ready(Ok(()))
        }
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
                b.include(
                    subscriber(Unnamed::<SqsQueue>::new(), TooPatient)
                        .build()
                        .name("orders")
                        .wait(Duration::from_secs(30)),
                );
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
