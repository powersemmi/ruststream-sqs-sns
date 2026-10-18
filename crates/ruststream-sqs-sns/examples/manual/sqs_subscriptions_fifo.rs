//! The service of `examples/sqs_subscriptions_fifo.rs` written without the macros: a queue name
//! ending in `.fifo` reads each message group in the order it was sent.
//!
//! ```text
//! cargo test --example manual_sqs_subscriptions_fifo --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders.fifo")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:fifo]
/// Reads the FIFO queue `orders.fifo`. The orders of one customer are one message group, so
/// they arrive in the order they were sent, one at a time.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.customer, "order fulfilled");
        ready(Ok(()))
    }
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders.fifo", Fulfil).build());
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

fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

fn main() -> ExitCode {
    run_main(app)
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
