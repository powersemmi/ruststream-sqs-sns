//! A broker pointed at a local stack: `endpoint`, `region` and `test_credentials`. The in-process
//! test runs everywhere; the live one runs against the stack behind `SQS_TEST_ENDPOINT` and skips
//! without it.
//!
//! ```text
//! cargo test --example sqs_broker_endpoint --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example sqs_broker_endpoint --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue)]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order received");
    HandlerOutcome::ack()
}

/// The service reading `queue`, which it creates on a fresh stack.
fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(handle.name(queue).create_if_missing());
    })
}

// --8<-- [start:broker]
/// The broker for a local stack at `endpoint`, such as `LocalStack` on port 4566. The stack checks
/// that a request is signed and ignores the key, so test credentials are enough.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials()
}

#[app]
fn app() -> impl App {
    service(broker("http://localhost:4566"), "orders".to_owned())
}
// --8<-- [end:broker]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, broker, live, service};

    /// The body both modes run: an order sent to the queue reaches the handler.
    async fn an_order_reaches_the_handler(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .to(queue.to_owned())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let app = service(broker("http://localhost:4566"), "orders".to_owned());
        an_order_reaches_the_handler(TestApp::start(app).await?, "orders").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("endpoint-orders");
        let app = service(broker(&endpoint), queue.clone());
        an_order_reaches_the_handler(TestApp::start_live(app).await?, &queue).await
    }
}
