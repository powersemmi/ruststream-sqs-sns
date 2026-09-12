//! Conformance: every suite this crate's capabilities justify, run twice.
//!
//! Each check runs against the in-process transport, where it is the definition of correct
//! behaviour the stand-in is held to, and again against a local stack (gated behind
//! `SQS_TEST_ENDPOINT`), where the same check is what proves the stand-in is not lying about the
//! service. Dropping either leg leaves one of the two unverified.
//!
//! The suites are the routing contract ([`harness::run_suite`], in process only - it drives the
//! `TestableBroker` surface, which no live broker has), the lifecycle ladder
//! ([`harness::lifecycle`]) and the one capability this crate implements beyond the base,
//! [`capabilities::batches`]. `request_reply`, `transactions`, `owned_transactions` and `seeking`
//! have no suite here because the crate implements none of those capabilities: SQS offers no
//! request-reply channel, no transactions and no cursor to seek.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_sqs_sns::testing::SqsTestBroker;
use ruststream_sqs_sns::{SqsBroker, SqsQueue};

mod live;

/// The stack these checks run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_test_broker_passes_conformance_suite() {
    harness::run_suite(SqsTestBroker::new).await;
}

/// The lifecycle ladder against the in-process transport: the same walk the live leg below
/// makes, so the stand-in is held to the framework's own definition of a well-behaved broker
/// rather than only to the routing suite. It ends on the assertion that matters most here - a
/// publisher that aliased the connection must report the closed transport afterwards instead of
/// routing into a dead router, which is what the real broker does through its `closed` flag.
///
/// The suite opens the subscription through this crate's own descriptor, the same one the live
/// leg uses.
// The closures below cannot become method paths: their bounds are higher-ranked, so a bare path
// would bind one concrete lifetime.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_test_broker_passes_lifecycle() {
    harness::lifecycle(
        SqsTestBroker::new,
        |name| SqsQueue::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_test_broker_honours_the_batch_size() {
    capabilities::batches(
        SqsTestBroker::new,
        |name| SqsQueue::new(name),
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
