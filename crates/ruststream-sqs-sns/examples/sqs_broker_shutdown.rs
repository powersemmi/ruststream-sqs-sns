//! Shutdown: the connection is marked closed, and a publisher the service took from the broker
//! reports `SqsError::NotConnected` afterwards. The in-process test runs everywhere; the live one
//! runs against the stack behind `SQS_TEST_ENDPOINT` and skips without it.
//!
//! ```text
//! cargo test --example sqs_broker_shutdown --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example sqs_broker_shutdown --features testing
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

// --8<-- [start:service]
/// The service, and a publisher for code outside its handlers, such as an HTTP endpoint that
/// takes orders. The publisher shares the broker's connection, from startup to shutdown.
fn service(broker: SqsBroker, queue: String) -> (impl App<State = ()>, SqsPublisher) {
    let intake = broker.publisher();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(handle.name(queue).create_if_missing());
    });
    (app, intake)
}
// --8<-- [end:service]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials()
}

#[app]
fn app() -> impl App {
    let (app, _intake) = service(broker("http://localhost:4566"), "orders".to_owned());
    app
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::PublishError;
    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::SqsError;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, broker, live, service};

    /// The body both modes run: the publisher sends while the service runs, and reports the
    /// closed connection after shutdown instead of sending.
    async fn a_publish_after_shutdown_is_refused(
        tb: TestApp<()>,
        intake: &SqsPublisher,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        intake
            .message(&Order { id: 1 })
            .to(queue.to_owned())
            .publish()
            .await?;
        tb.shutdown().await?;

        let refused = intake
            .message(&Order { id: 2 })
            .to(queue.to_owned())
            .publish()
            .await;
        assert!(
            matches!(refused, Err(PublishError::Publish(SqsError::NotConnected))),
            "{refused:?}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let (app, intake) = service(broker("http://localhost:4566"), "orders".to_owned());
        a_publish_after_shutdown_is_refused(TestApp::start(app).await?, &intake, "orders").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("shutdown-orders");
        let (app, intake) = service(broker(&endpoint), queue.clone());
        a_publish_after_shutdown_is_refused(TestApp::start_live(app).await?, &intake, &queue).await
    }
}
