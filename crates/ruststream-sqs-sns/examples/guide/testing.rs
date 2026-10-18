//! The service the Testing guide tests: a checkout that confirms orders, drops the empty ones and
//! holds a back-ordered one until the stock comes in.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! ```

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:service]
/// How long a back-ordered order waits before the stock is checked again.
const RESTOCK: Duration = Duration::from_secs(3600);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout-orders")]
struct Order {
    id: u64,
    amount_cents: u64,
    back_ordered: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout-confirmations")]
struct Confirmation {
    order: u64,
    amount_cents: u64,
}

#[subscriber(SqsQueue::new("checkout-orders").create_if_missing(), reply)]
async fn confirm(order: &Order) -> Result<Confirmation, HandlerOutcome> {
    if order.amount_cents == 0 {
        tracing::warn!(order.id, "empty order dropped");
        return Err(HandlerOutcome::drop());
    }
    if order.back_ordered {
        tracing::info!(order.id, "order waits for stock");
        return Err(HandlerOutcome::retry_after(RESTOCK));
    }
    tracing::info!(order.id, "order confirmed");
    Ok(Confirmation {
        order: order.id,
        amount_cents: order.amount_cents,
    })
}

#[subscriber(SqsQueue::new("checkout-confirmations").create_if_missing())]
async fn notify(confirmation: &Confirmation) -> HandlerOutcome {
    tracing::info!(confirmation.order, "customer notified");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker) -> RustStream {
    RustStream::new(AppInfo::new("checkout", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm);
        b.include(notify);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Confirmation, Order, RESTOCK, live, service};

    fn order(id: u64, amount_cents: u64, back_ordered: bool) -> Order {
        Order {
            id,
            amount_cents,
            back_ordered,
        }
    }

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn an_order_is_confirmed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(SqsBroker::new())).await?;
        a_confirmation_follows_the_order(tb).await
    }

    async fn a_confirmation_follows_the_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&order(1, 2_500, false))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("checkout-orders")
            .assert_called_once()
            .with(&order(1, 2_500, false))
            .settled(HandlerOutcome::ack());
        tb.broker::<SqsBroker>()
            .subscriber("checkout-confirmations")
            .assert_called_once()
            .with(&Confirmation {
                order: 1,
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn an_empty_order_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(SqsBroker::new())).await?;

        tb.broker::<SqsBroker>()
            .message(&order(2, 0, false))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("checkout-orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<SqsBroker>()
            .published::<Confirmation>("checkout-confirmations")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:delay]
    #[tokio::test(start_paused = true)]
    async fn a_back_ordered_order_comes_back_after_the_restock() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(SqsBroker::new())).await?;
        let waiting = order(3, 2_500, true);

        tb.broker::<SqsBroker>().message(&waiting).publish().await?;
        tb.advance(RESTOCK).await?;

        tb.broker::<SqsBroker>()
            .subscriber("checkout-orders")
            .assert_called(2)
            .with(&waiting)
            .settled(HandlerOutcome::retry_after(RESTOCK));
        // The same message came back; nothing was published again to bring it.
        tb.broker::<SqsBroker>()
            .published::<Order>("checkout-orders")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:delay]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_is_confirmed_on_a_live_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        delete_queues(&endpoint).await?;
        let tb = TestApp::start_live(service(live::broker(&endpoint))).await?;
        a_confirmation_follows_the_order(tb).await
    }
    // --8<-- [end:live]

    /// Deletes the service's queues with whatever an earlier run left on them, in flight or not,
    /// so the counts see this run's messages alone. The service creates them again when it
    /// starts.
    async fn delete_queues(endpoint: &str) -> Result<(), Box<dyn Error>> {
        let admin = live::admin(endpoint).await;
        for queue in ["checkout-orders", "checkout-confirmations"] {
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
