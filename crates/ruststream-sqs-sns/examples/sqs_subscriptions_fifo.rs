//! A FIFO queue: a name ending in `.fifo` reads each message group in the order it was sent.
//! Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_subscriptions_fifo --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.fifo")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:fifo]
/// Reads the FIFO queue `orders.fifo`. The orders of one customer are one message group, so
/// they arrive in the order they were sent, one at a time.
#[subscriber(SqsQueue::new("orders.fifo"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.customer, "order fulfilled");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}
// --8<-- [end:fifo]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

#[app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;

    use super::*;

    fn order(id: u64, customer: &str) -> Order {
        Order {
            id,
            customer: customer.to_owned(),
        }
    }

    #[tokio::test]
    async fn the_orders_of_one_customer_arrive_in_the_order_sent() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        let orders = [order(1, "ada"), order(2, "ada"), order(3, "ada")];
        // Sent at once, so the three wait on the queue together. The test names no group, so
        // the three share the group `default`.
        let broker = tb.broker::<SqsBroker>();
        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }

        let ids: Vec<u64> = tb
            .broker::<SqsBroker>()
            .subscriber("orders.fifo")
            .received::<Order>()
            .iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(ids, [1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}
