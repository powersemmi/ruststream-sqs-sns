//! The service of `examples/sqs_batches.rs` written without the macros: a body over `[T]` is a
//! batch handler, and the queue's steps chain after the batch size.
//!
//! ```text
//! cargo test --example manual_sqs_batches --features testing
//! cargo run --example manual_sqs_batches --features testing -- run
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders-batches")]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// A batch handler: the slice is one `ReceiveMessage` worth of orders, and `Ok` settles it as a
/// whole. It holds at most the size the mount site names, and as few as one when that is all the
/// queue had at the moment of the call.
struct Reconcile;

impl Handle<[Order]> for Reconcile {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = orders.len(), "reconciled a batch of orders");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:mount]
fn service(broker: SqsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders-batches", "0.1.0")).with_broker(broker, |b| {
        // The size is the framework's word and becomes `MaxNumberOfMessages`; the long
        // poll and the queue's provisioning are this crate's, chained after it.
        b.include(
            subscriber("orders-batches", Reconcile)
                .batch(nonzero!(10))
                .build()
                .map_source(|_| SqsQueue::new("orders-batches"))
                .wait(Duration::from_secs(20))
                .create_if_missing(),
        );
    })
}
// --8<-- [end:mount]

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

    /// Publishes `count` orders at once, so they wait on the queue together.
    async fn publish_at_once(tb: &TestApp<()>, count: u64) -> Result<(), Box<dyn Error>> {
        let orders: Vec<Order> = (1..=count).map(|id| Order { id }).collect();
        let broker = tb.broker::<SqsBroker>();
        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn three_waiting_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        publish_at_once(&tb, 3).await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders-batches")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_holds_at_most_the_size_the_mount_site_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        publish_at_once(&tb, 12).await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders-batches")
            .assert_batch_sizes(&[10, 2]);
        tb.shutdown().await?;
        Ok(())
    }
}
