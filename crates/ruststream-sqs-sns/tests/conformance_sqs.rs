//! Conformance: every suite this crate's capabilities justify, run twice over the production
//! broker.
//!
//! Each check runs over the in-process transport, through `harness::InProcessBroker` where the
//! suite connects with `connect`, and again against a local stack (gated behind
//! `SQS_TEST_ENDPOINT`). The in-process pass holds the transport tests run on to the framework's
//! own definition of a well-behaved broker; the live pass proves the SQS implementation. Dropping
//! either leg leaves one of the two unverified.
//!
//! The suites are the routing contract ([`harness::run_suite`], in process only - it drives the
//! `TestableBroker` surface, which no live broker has), the lifecycle ladder
//! ([`harness::lifecycle`]) through the crate's descriptor and through a bare name, and the one
//! capability this crate implements beyond the base, [`capabilities::batches`].
//! `redelivery_address`, `request_reply`, `transactions`, `owned_transactions` and `seeking` have
//! no suite here: a queue moves a spent delivery itself, so no subscription reports an address
//! for a copy, and SQS offers no request-reply channel, no transactions and no cursor to seek.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`.

#![cfg(feature = "testing")]

use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream::{BatchSubscriber, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher};
use ruststream_sqs_sns::{SqsBroker, SqsQueue};

mod live;

use live::{RECV_TIMEOUT, connect, unique};

/// The stack these checks run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<SqsBroker> {
    InProcessBroker::new(SqsBroker::new().region("us-east-1"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| SqsBroker::new().region("us-east-1")).await;
}

/// The lifecycle ladder in process: the same walk the live leg below makes. It ends on the
/// assertion that matters most here - a publisher that aliased the connection must report the
/// closed transport afterwards instead of succeeding, which the production broker does through
/// its `closed` flag on both transports.
// The closures below cannot become method paths: their bounds are higher-ranked, so a bare path
// would bind one concrete lifetime.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| SqsQueue::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same ladder over the bare-name form, which resolves through `Subscribe` rather than
/// through the crate's descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle_by_name() {
    harness::lifecycle(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_honours_the_batch_size() {
    capabilities::batches(
        in_process,
        |name| SqsQueue::new(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// A published document is shared, so nothing a broker contributes to it may carry a password.
/// The endpoint is the way one gets in here: a URL like `http://svc:hunter2@sqs.local:4566`
/// carries credentials, and a server description that kept them would hand them to every reader.
#[cfg(feature = "asyncapi")]
#[test]
fn the_broker_describes_itself_without_its_credentials() {
    harness::describes_without_credentials(
        &SqsBroker::new()
            .endpoint("http://svc:hunter2@sqs.local:4566")
            .region("us-east-1"),
        &SqsQueue::new("orders").visibility(Duration::from_secs(30)),
        "hunter2",
    );
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

/// How many messages the clamped run puts on the queue, and the size it asks for.
const QUEUED: usize = 12;

/// The protocol cap on one receive.
const RECEIVE_CAP: usize = 10;

// A mount site may name a batch larger than one `ReceiveMessage` can return, and the crate
// clamps it rather than refusing, so a handler written for a broker with bigger batches still
// compiles onto SQS. What makes the clamp load-bearing is the service: a receive asking for more
// than ten is a protocol error, so a size passed through would fail every poll rather than
// coming back short.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_above_the_protocol_cap_is_clamped_rather_than_refused() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("clamped");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(1)),
        )
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    for index in 0..QUEUED {
        publisher
            .publish(
                OutgoingMessage::new(&queue, index.to_string().as_bytes()),
                None,
            )
            .await
            .expect("publish succeeds");
    }

    let asked = NonZeroUsize::new(QUEUED * 2).expect("a batch size is never zero");
    let mut batches = pin!(subscriber.batches(asked));
    let batch = tokio::time::timeout(RECV_TIMEOUT, batches.next())
        .await
        .expect("a batch arrives")
        .expect("stream is open")
        .expect("the receive asked for a size the service accepts");
    assert!(
        (1..=RECEIVE_CAP).contains(&batch.len()),
        "one receive returned {} messages, and the protocol tops out at {RECEIVE_CAP}",
        batch.len(),
    );
    for message in batch {
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}
