//! The shop of the Fan-out with SNS guide, written without the macros: each handler is a type with
//! an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_fan_out --features testing,sns -- run
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

// --8<-- [start:accept]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PlaceOrder {
    id: u64,
    amount_cents: u64,
}

impl OutgoingDestination for PlaceOrder {
    type Form = FixedName;

    const DESTINATION: &'static str = "shop-orders";
}

impl MessageHeaders for PlaceOrder {
    type Contract = NoHeaders;
}

impl MessageInfo for PlaceOrder {
    const NAME: &'static str = "PlaceOrder";
}

/// The announcement names the topic it is published to.
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderPlaced {
    id: u64,
    amount_cents: u64,
}

impl OutgoingDestination for OrderPlaced {
    type Form = FixedName;

    const DESTINATION: &'static str = "shop-order-events";
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
}

impl MessageInfo for OrderPlaced {
    const NAME: &'static str = "OrderPlaced";
}

struct Accept;

impl Handle<PlaceOrder, OrderPlaced> for Accept {
    fn handle(
        &self,
        order: &PlaceOrder,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderPlaced, HandlerOutcome>> {
        tracing::info!(order.id, "order accepted");
        ready(Ok(OrderPlaced {
            id: order.id,
            amount_cents: order.amount_cents,
        }))
    }
}
// --8<-- [end:accept]

// --8<-- [start:consumers]
struct Bill;

impl Handle<OrderPlaced> for Bill {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.amount_cents, "order billed");
        ready(Ok(()))
    }
}

struct Ship;

impl Handle<OrderPlaced> for Ship {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order shipped");
        ready(Ok(()))
    }
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
            b.include(
                subscriber("shop-orders", Accept)
                    .map_source(|_| SqsQueue::new("shop-orders").create_if_missing())
                    .reply()
                    .build(),
            )
            .out_reply(SnsPublish::default());
            b.include(
                subscriber("shop-billing", Bill)
                    .map_source(|_| SqsQueue::new("shop-billing").create_if_missing())
                    .build(),
            );
            b.include(
                subscriber("shop-shipping", Ship)
                    .map_source(|_| SqsQueue::new("shop-shipping").create_if_missing())
                    .build(),
            );
        })
}

fn app() -> RustStream {
    service(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
    )
}

fn main() -> ExitCode {
    run_main(app)
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
