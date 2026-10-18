//! The service of `examples/sqs_broker_startup.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_broker_startup --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_sqs_broker_startup --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` by hand: an order names no queue, so each publish names one.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:service]
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order received");
        ready(Ok(()))
    }
}

/// Reads `queue`, which the infrastructure creates. The subscription names no visibility
/// timeout, so startup reads the queue's own.
fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Receive)
                .build()
                .name(queue)
                .wait(Duration::from_secs(20)),
        );
    })
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

fn app() -> impl App {
    service(broker("http://localhost:4566"), "orders".to_owned())
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::SqsError;
    use ruststream_sqs_sns::prelude::*;

    use ruststream::Unnamed;

    use super::{Order, Receive, broker, live, service};

    /// The `SqsError` somewhere in the cause chain of a failed startup.
    fn sqs_error<'a>(failed: &'a (dyn Error + 'static)) -> Option<&'a SqsError> {
        let mut cause = Some(failed);
        while let Some(err) = cause {
            if let Some(sqs) = err.downcast_ref::<SqsError>() {
                return Some(sqs);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn an_existing_queue_is_read_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(
            broker("http://localhost:4566"),
            "orders".to_owned(),
        ))
        .await?;
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_wait_above_twenty_seconds_stops_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(
                    subscriber(Unnamed::<SqsQueue>::new(), Receive)
                        .build()
                        .name("orders")
                        .wait(Duration::from_secs(30)),
                );
            },
        );

        let failed = TestApp::start(app)
            .await
            .err()
            .expect("a long poll waits 20 seconds at most");
        let reason = sqs_error(&failed).expect("the cause is an SqsError");
        assert!(matches!(reason, SqsError::InvalidQueue(_)), "{reason}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_existing_queue_is_read_on_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        // The infrastructure's part: the queue exists before the service starts.
        let queue = live::unique("startup-orders");
        live::admin(&endpoint)
            .await
            .create_queue()
            .queue_name(&queue)
            .send()
            .await?;

        let tb = TestApp::start_live(service(broker(&endpoint), queue.clone())).await?;
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .to(queue.clone())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(&queue)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_queue_stops_startup_on_the_stack() {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return;
        };
        let queue = live::unique("startup-missing");

        let failed = TestApp::start_live(service(broker(&endpoint), queue))
            .await
            .err()
            .expect("a queue the service does not create has to exist");
        let reason = sqs_error(&failed).expect("the cause is an SqsError");
        assert!(matches!(reason, SqsError::Queue { .. }), "{reason}");
    }
}
