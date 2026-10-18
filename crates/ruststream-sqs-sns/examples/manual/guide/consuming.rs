//! The warehouse service of the Consuming queues guide, written without the macros: each handler
//! is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is
//! written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_consuming --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:pack]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    items: u32,
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

/// Packs an order.
struct Pack;

impl Handle<Order> for Pack {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.items, "order packed");
        ready(Ok(()))
    }
}
// --8<-- [end:pack]

// --8<-- [start:reconcile]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    order: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

impl MessageInfo for Payment {
    const NAME: &'static str = "Payment";
}

/// Books the payments one receive returned, in one write to the ledger.
struct Reconcile;

impl Handle<[Payment]> for Reconcile {
    fn handle(
        &self,
        payments: &[Payment],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let total: u64 = payments.iter().map(|payment| payment.amount_cents).sum();
        tracing::info!(count = payments.len(), total, "payments booked");
        ready(Ok(()))
    }
}
// --8<-- [end:reconcile]

// --8<-- [start:audit]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Change {
    order: u64,
    by: String,
}

impl OutgoingDestination for Change {
    type Form = CallerName;
}

impl MessageHeaders for Change {
    type Contract = NoHeaders;
}

impl MessageInfo for Change {
    const NAME: &'static str = "Change";
}

/// Records a change to an order, on whichever queue it is mounted.
struct Audit;

impl Handle<Change> for Audit {
    fn handle(
        &self,
        change: &Change,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.order, change.by, "change recorded");
        ready(Ok(()))
    }
}
// --8<-- [end:audit]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            // A minute of visibility per receive, four orders packed at once.
            b.include(
                subscriber("orders", Pack)
                    .map_source(|_| {
                        SqsQueue::new("orders")
                            .visibility(Duration::from_secs(60))
                            .create_if_missing()
                    })
                    .workers(nonzero!(4))
                    .build(),
            );
            // Up to ten payments per call: one receive is one batch.
            b.include(
                subscriber("payments", Reconcile)
                    .batch(nonzero!(10))
                    .map_source(|_| {
                        SqsQueue::new("payments")
                            .wait(Duration::from_secs(20))
                            .create_if_missing()
                    })
                    .build(),
            );
            // One handler, two queues.
            for queue in ["audit-eu", "audit-us"] {
                b.include(
                    subscriber(queue, Audit)
                        .map_source(|_| SqsQueue::new(queue).create_if_missing())
                        .build(),
                );
            }
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
    use ruststream_sqs_sns::prelude::*;

    use super::{Change, Order, Payment, app};

    #[tokio::test]
    async fn an_order_is_packed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&Order { id: 1, items: 3 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1, items: 3 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn waiting_payments_arrive_in_one_batch() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payments: Vec<Payment> = (1..=3)
            .map(|order| Payment {
                order,
                amount_cents: 500,
            })
            .collect();

        let broker = tb.broker::<SqsBroker>();
        try_join_all(
            payments
                .iter()
                .map(|payment| broker.message(payment).publish()),
        )
        .await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn one_handler_reads_both_queues() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for (queue, by) in [("audit-eu", "ana"), ("audit-us", "bob")] {
            tb.broker::<SqsBroker>()
                .message(&Change {
                    order: 1,
                    by: by.to_owned(),
                })
                .to(queue)
                .publish()
                .await?;
        }

        for (queue, by) in [("audit-eu", "ana"), ("audit-us", "bob")] {
            tb.broker::<SqsBroker>()
                .subscriber(queue)
                .assert_called_once()
                .with(&Change {
                    order: 1,
                    by: by.to_owned(),
                })
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }
}
