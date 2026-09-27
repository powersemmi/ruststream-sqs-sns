//! Conformance: every suite this crate's capabilities justify, run over the production broker.
//!
//! Each check runs over the in-process transport, through `harness::InProcessBroker` where the
//! suite connects with `connect`, and again against a local stack (gated behind
//! `SQS_TEST_ENDPOINT`). The in-process pass holds the transport tests run on to the framework's
//! own definition of a well-behaved broker; the live pass proves the SQS implementation. The
//! settlement comparison connects both transports, so it runs where the live suite runs.
//!
//! The suites:
//!
//! * the routing contract ([`harness::run_suite`], in process only: it drives the
//!   `TestableBroker` surface, which no live broker has);
//! * the lifecycle ladder ([`harness::lifecycle`]) through the crate's descriptor and through a
//!   bare name, and what a shutdown must finish ([`lifecycle::shutdown_flushes`]);
//! * what a settlement means ([`settlement`]), and the in-process answers held to the server's;
//! * the queue's own redrive ([`retry::broker_moves`]): a queue moves a spent delivery itself, so
//!   no subscription reports an address for a retry copy;
//! * what a message carries: its group as its key ([`message_shape::keyed_order`]), the FIFO
//!   settings of a publish ([`message_shape::publish_options`]), and no credential in anything
//!   the broker describes;
//! * the one capability this crate implements beyond the base, [`capabilities::batches`].
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features`.

#![cfg(feature = "testing")]

use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, harness, lifecycle, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{
    BatchSubscriber, Bytes, ConnectedBroker, HeaderMap, IncomingMessage, Name, OutgoingMessage,
    Publisher,
};
#[cfg(feature = "asyncapi")]
use ruststream_sqs_sns::ConnectedSqsBroker;
use ruststream_sqs_sns::{
    PARTITION_KEY_HEADER, SqsBroker, SqsPublish, SqsPublishOptions, SqsQueue,
};

mod live;

use live::{RECV_TIMEOUT, connect, unique};

/// The stack these checks run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// The production broker, configured for the local stack.
fn live_broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<SqsBroker> {
    InProcessBroker::new(SqsBroker::new().region("us-east-1"))
}

/// The redelivery timeout the settlement checks give their queue, and wait out.
const LEASE: Duration = Duration::from_secs(2);

/// How long a live receive waits for a message before it asks again.
const WAIT: Duration = Duration::from_secs(1);

/// How many deliveries the redrive checks cap a message at.
const ATTEMPTS: NonZeroU32 = NonZeroU32::new(3).expect("three is not zero");

/// The group every message a policy-only publish carries in the options check.
const POLICY_GROUP: &str = "policy-group";

/// A subject of the check's own that names a FIFO queue, whose name ends in `.fifo`.
fn fifo_subject(prefix: &str) -> String {
    format!("{}.fifo", unique_subject(prefix))
}

/// The message group a FIFO delivery reports, which is its key.
fn group_of(delivery: &impl IncomingMessage) -> Option<String> {
    delivery
        .partition_key()
        .map(|key| String::from_utf8_lossy(key).into_owned())
}

/// The key the keyed-order check publishes under, carried the portable way: the
/// `partition-key` header, which a FIFO publish turns into the message group.
fn key_in_header(key: &[u8], headers: &mut HeaderMap) -> Option<SqsPublishOptions> {
    headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    None
}

/// The FIFO settings the options check publishes with: a group of the call's own, a call that
/// names only a deduplication id and so keeps the policy's group.
fn option_cases() -> OptionCases<SqsPublishOptions, Option<String>> {
    OptionCases::new(Some(POLICY_GROUP.to_owned()))
        .overrides(
            SqsPublishOptions::default().group_id("call-group"),
            Some("call-group".to_owned()),
        )
        .overrides(
            SqsPublishOptions::default().deduplication_id("call-dedup"),
            Some(POLICY_GROUP.to_owned()),
        )
}

// ---------------------------------------------------------------------------------------------
// In process
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| SqsBroker::new().region("us-east-1")).await;
}

/// The lifecycle ladder in process: the same walk the live leg below makes, through the crate's
/// descriptor and through the bare-name form, which resolves through `Subscribe`.
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

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_moves_a_spent_delivery_to_the_dead_letter_queue() {
    retry::broker_moves(
        in_process,
        |name| SqsQueue::new(name),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
    retry::broker_moves(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_the_order_of_a_group() {
    message_shape::keyed_order(
        in_process,
        &fifo_subject("conformance.keyed"),
        |name| SqsQueue::new(name),
        |connected| connected.publisher(),
        key_in_header,
    )
    .await;
}

#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_resolves_the_fifo_settings_over_the_policy() {
    message_shape::publish_options(
        in_process,
        &fifo_subject("conformance.options"),
        |name| SqsQueue::new(name),
        SqsPublish::default().group_id(POLICY_GROUP),
        option_cases(),
        |delivery| group_of(delivery),
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
    // The broker takes one endpoint, so of a cluster's addresses a deployment configures one.
    message_shape::describes_addresses_without_credentials(
        |addrs| SqsBroker::new().endpoint(addrs[0]).region("us-east-1"),
        "http",
    );
}

/// The publish policies describe their positions with bindings of their own, which reach the
/// same shared document.
#[cfg(feature = "asyncapi")]
#[test]
fn the_publish_policies_describe_themselves_without_credentials() {
    message_shape::publishes_without_credentials::<ConnectedSqsBroker, _>(
        &SqsPublish::default().group_id("orders"),
        "hunter2",
    );
    #[cfg(feature = "sns")]
    message_shape::publishes_without_credentials::<ConnectedSqsBroker, _>(
        &ruststream_sqs_sns::SnsPublish::default(),
        "hunter2",
    );
}

// ---------------------------------------------------------------------------------------------
// Against the local stack
// ---------------------------------------------------------------------------------------------

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_broker_passes_lifecycle() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    // The suite future is larger than a stack frame should carry; boxing it once costs nothing
    // that matters in a test.
    Box::pin(harness::lifecycle(
        move || live_broker(&endpoint),
        |name| SqsQueue::new(name).create_if_missing(),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_flushes_at_shutdown() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    lifecycle::shutdown_flushes(
        || live_broker(&endpoint),
        |name| SqsQueue::new(name).create_if_missing().visibility(LEASE),
        |connected| connected.publisher(),
        Backlog::Delivered,
    )
    .await;
}

/// The settlement answers against the service, then the same run in process, compared.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_settles_like_the_in_process_mode() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    // The suite future is larger than a stack frame should carry; boxing it once costs nothing
    // that matters in a test.
    Box::pin(settlement::matches_in_process(
        || live_broker(&endpoint),
        |name| {
            SqsQueue::new(name)
                .create_if_missing()
                .visibility(LEASE)
                .wait(WAIT)
        },
        |connected| connected.publisher(),
        LEASE,
    ))
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
    // The suite future is larger than a stack frame should carry; boxing it once costs nothing
    // that matters in a test.
    Box::pin(capabilities::batches(
        move || live_broker(&endpoint),
        |name| SqsQueue::new(name).create_if_missing(),
        |connected| connected.publisher(),
    ))
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_moves_a_spent_delivery_to_the_dead_letter_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    retry::broker_moves(
        || live_broker(&endpoint),
        |name| SqsQueue::new(name).create_if_missing().wait(WAIT),
        |connected| connected.publisher(),
        ATTEMPTS,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_keeps_the_order_of_a_group() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    // The suite future is larger than a stack frame should carry; boxing it once costs nothing
    // that matters in a test.
    Box::pin(message_shape::keyed_order(
        || live_broker(&endpoint),
        &fifo_subject("conformance.keyed"),
        |name| SqsQueue::new(name).create_if_missing().wait(WAIT),
        |connected| connected.publisher(),
        key_in_header,
    ))
    .await;
}

#[allow(clippy::redundant_closure)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_resolves_the_fifo_settings_over_the_policy() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    // The suite future is larger than a stack frame should carry; boxing it once costs nothing
    // that matters in a test.
    Box::pin(message_shape::publish_options(
        || live_broker(&endpoint),
        &fifo_subject("conformance.options"),
        |name| SqsQueue::new(name).create_if_missing().wait(WAIT),
        SqsPublish::default().group_id(POLICY_GROUP),
        option_cases(),
        |delivery| group_of(delivery),
    ))
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
        .subscribe_queue(SqsQueue::new(&queue).create_if_missing().wait(WAIT))
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
