//! The warehouse service of the Consuming queues guide: orders packed four at a time, payments
//! reconciled a receive at a time, and one audit handler on two queues.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_consuming --features testing -- run
//! ```

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:pack]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: u32,
}

/// Packs an order. Packing takes up to a minute, so each receive asks for a minute of
/// visibility, and four orders are packed at once.
#[subscriber(
    SqsQueue::new("orders")
        .visibility(Duration::from_secs(60))
        .create_if_missing(),
    workers(4)
)]
async fn pack(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.items, "order packed");
    HandlerOutcome::ack()
}
// --8<-- [end:pack]

// --8<-- [start:reconcile]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    order: u64,
    amount_cents: u64,
}

/// Books the payments one receive returned, in one write to the ledger.
#[subscriber(SqsQueue::new("payments"))]
async fn reconcile(payments: &[Payment]) -> HandlerOutcome {
    let total: u64 = payments.iter().map(|payment| payment.amount_cents).sum();
    tracing::info!(count = payments.len(), total, "payments booked");
    HandlerOutcome::ack()
}
// --8<-- [end:reconcile]

// --8<-- [start:audit]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Change {
    order: u64,
    by: String,
}

/// Records a change to an order. The definition fixes the kind of queue and leaves its name
/// to the mount site.
#[subscriber(SqsQueue)]
async fn audit(change: &Change) -> HandlerOutcome {
    tracing::info!(change.order, change.by, "change recorded");
    HandlerOutcome::ack()
}
// --8<-- [end:audit]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            b.include(pack);
            // Up to ten payments per call: one receive is one batch.
            b.include(
                reconcile
                    .batch(nonzero!(10))
                    .wait(Duration::from_secs(20))
                    .create_if_missing(),
            );
            // One definition, two queues.
            b.include(audit.name("audit-eu").create_if_missing());
            b.include(audit.name("audit-us").create_if_missing());
        },
    )
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
    async fn one_definition_reads_both_queues() -> Result<(), Box<dyn Error>> {
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
