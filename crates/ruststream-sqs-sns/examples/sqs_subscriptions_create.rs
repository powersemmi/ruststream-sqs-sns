//! A subscription that creates its queue: `create_if_missing` opens the queue at startup when
//! the account does not have it yet. The live tests run against the stack behind
//! `SQS_TEST_ENDPOINT`; in process every queue the service names exists already.
//!
//! ```text
//! cargo test --example sqs_subscriptions_create --features testing
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example sqs_subscriptions_create --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Draft {
    id: u64,
}

// --8<-- [start:create]
/// Reads drafts from a queue the service opens itself on a fresh stack.
#[subscriber(SqsQueue)]
async fn review(draft: &Draft) -> HandlerOutcome {
    tracing::info!(draft.id, "draft reviewed");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("drafts", "0.1.0")).with_broker(broker, |b| {
        b.include(review.name(queue).create_if_missing());
    })
}
// --8<-- [end:create]

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
    service(broker("http://localhost:4566"), "drafts".to_owned())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use aws_sdk_sqs::types::QueueAttributeName;
    use ruststream::testing::TestApp;

    use super::*;

    /// One body for both modes: a draft published to the queue reaches the handler.
    async fn a_draft_reaches_the_handler(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Draft { id: 9 })
            .to(queue.to_owned())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&Draft { id: 9 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process_a_draft_reaches_the_handler() -> Result<(), Box<dyn Error>> {
        let app = service(broker("http://localhost:4566"), "drafts".to_owned());
        a_draft_reaches_the_handler(TestApp::start(app).await?, "drafts").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_missing_queue_is_created_at_startup() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("create-drafts");

        let app = service(broker(&endpoint), queue.clone());
        a_draft_reaches_the_handler(TestApp::start_live(app).await?, &queue).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_fifo_name_creates_a_fifo_queue() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = format!("{}.fifo", live::unique("create-drafts"));

        let tb = TestApp::start_live(service(broker(&endpoint), queue.clone())).await?;

        let fifo = live::queue_attribute(&endpoint, &queue, QueueAttributeName::FifoQueue).await;
        assert_eq!(fifo, "true");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_without_the_step_a_missing_queue_stops_startup() {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return;
        };
        let queue = live::unique("create-absent");
        let app =
            RustStream::new(AppInfo::new("drafts", "0.1.0")).with_broker(broker(&endpoint), |b| {
                b.include(review.name(queue));
            });

        assert!(
            TestApp::start_live(app).await.is_err(),
            "a queue the account does not have is not opened without the step"
        );
    }
}
