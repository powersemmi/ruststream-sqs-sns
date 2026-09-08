//! Conformance: the routing suite against the in-process transport, and the lifecycle check
//! against a local stack (gated behind `SQS_TEST_ENDPOINT`).
//!
//! Start one with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_sqs_sns::testing::SqsTestBroker;
use ruststream_sqs_sns::{SqsBroker, SqsQueue};

fn test_endpoint() -> Option<String> {
    match std::env::var("SQS_TEST_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => Some(endpoint),
        _ => {
            eprintln!("SQS_TEST_ENDPOINT is not set; skipping the live conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_test_broker_passes_conformance_suite() {
    harness::run_suite(SqsTestBroker::new).await;
}

// The closures below cannot become method paths: their bounds are higher-ranked, so a bare path
// would bind one concrete lifetime.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_test_broker_honours_the_batch_size() {
    capabilities::batches(
        SqsTestBroker::new,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The batch size against the real service, where it is `MaxNumberOfMessages` rather than a
/// client-side buffer: the suite opens the subscription smaller than the run, so a receive that
/// ignored the size would come back with a batch too long.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_honours_the_batch_size() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    capabilities::batches(
        || {
            SqsBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| SqsQueue::new(name).create_if_missing(),
        |connected| connected.publisher(),
    )
    .await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_broker_passes_lifecycle() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    harness::lifecycle(
        || {
            SqsBroker::new()
                .endpoint(endpoint.clone())
                .test_credentials()
                .region("us-east-1")
        },
        |name| SqsQueue::new(name).create_if_missing(),
        |connected| connected.publisher(),
    )
    .await;
}
