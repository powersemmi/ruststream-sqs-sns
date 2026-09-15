//! FIFO queues against a local stack: the message group, the deduplication id and the portable
//! key a service sets for every broker it publishes to.
//!
//! Every assertion here reads the group back off the delivery, which is the service's own
//! answer: SQS reports `MessageGroupId` as a system attribute and this crate maps it onto the
//! portable `partition-key` header. The in-process stand-in has no message groups and no
//! deduplication window, so this is the only place any of it can be shown.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use aws_sdk_sqs::types::QueueAttributeName;
use futures::StreamExt;
use ruststream::runtime::{PublishError, PublishExt};
use ruststream::{
    ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, PublishPolicy,
    Publisher, Serialized, Subscriber,
};
use ruststream_sqs_sns::{PARTITION_KEY_HEADER, SqsError, SqsPublish, SqsPublishSteps, SqsQueue};

mod live;

use live::{QUIET, RECV_TIMEOUT, connect, queue_attribute, unique};

/// The body these tests publish through the builder: bytes the test already holds encoded, so
/// the type names itself serialized and no codec sits on the path.
#[derive(Outgoing, Serialized)]
struct Body(Vec<u8>);

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// A FIFO queue named for this test, created on subscribe.
fn fifo(name: &str) -> String {
    format!("{}.fifo", unique(name))
}

/// The subscription every test here opens: short polling, so a run does not sit on an empty
/// queue for the protocol maximum.
fn source(queue: &str) -> SqsQueue {
    SqsQueue::new(queue)
        .create_if_missing()
        .wait(Duration::from_secs(1))
}

/// The step on the publish builder reaches the queue as the FIFO message group id: SQS reports
/// it back on the delivery, and this is the only place that can be shown at all - the in-process
/// stand-in has no message groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_id_step_sets_the_fifo_message_group() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("group");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    connected
        .publisher()
        .message(&Body(br#"{"id":1}"#.to_vec()))
        .to(&queue)
        .group_id("user-42")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(
        message.headers().get_str(PARTITION_KEY_HEADER),
        Some("user-42")
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The step is this broker's own word for the group, so it wins over the portable
/// `partition-key` header a service sets for every broker it publishes to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_id_step_wins_over_the_messages_partition_key() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("groupwin");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    connected
        .publisher()
        .message(&Body(br#"{"id":2}"#.to_vec()))
        .with_headers(headers)
        .to(&queue)
        .group_id("user-7")
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(
        message.headers().get_str(PARTITION_KEY_HEADER),
        Some("user-7")
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// The portable spelling on its own: a service that names no SQS step still orders its messages,
// because the header it sets for every broker becomes the message group here. The delivery
// answers it back through the framework's own accessor, which is what a handler reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_partition_key_header_names_the_group_on_its_own() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("portable");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-acme");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(&queue, b"portable".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(
        message.partition_key(),
        Some(b"tenant-acme".as_slice()),
        "the group SQS reports is the key a handler reads",
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// The deduplication id is the idempotency key of one send, and SQS holds it for five minutes.
// Two sends under one id are one message on the queue, whatever their bodies say.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deduplication_id_collapses_the_repeat_publish() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("dedup");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    for body in [b"first".as_slice(), b"second".as_slice()] {
        connected
            .publisher()
            .message(&Body(body.to_vec()))
            .to(&queue)
            .group_id("orders")
            .deduplication_id("order-42")
            .publish()
            .await
            .expect("publish succeeds");
    }

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"first");
    message.ack().await.expect("ack succeeds");

    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "the second send under the same id reached the queue as a message of its own",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// The other side of that window. A FIFO queue this crate creates deduplicates on content, so two
// legitimate identical payloads would collapse into one; every send carrying an id of its own is
// what keeps them apart, and only the service can show it. The queue's own attribute is read
// back first, because the guarantee is worth nothing if the window it is measured against is
// not open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_identical_payloads_stay_two_messages() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("twins");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");
    assert_eq!(
        queue_attribute(
            &endpoint,
            &queue,
            QueueAttributeName::ContentBasedDeduplication
        )
        .await,
        "true",
        "a fifo queue this crate created does not deduplicate on content, so the test below \
         would pass on a queue that never collapses anything",
    );

    for _ in 0..2 {
        connected
            .publisher()
            .message(&Body(b"identical".to_vec()))
            .to(&queue)
            .group_id("orders")
            .publish()
            .await
            .expect("publish succeeds");
    }

    let mut stream = pin!(subscriber.stream());
    for delivery in 1..=2 {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .unwrap_or_else(|_| panic!("delivery {delivery} arrives"))
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), b"identical");
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

// A standard queue cannot order a group, and the crate says so instead of dropping the setting.
// What makes the refusal worth anything is the second half: the message does not reach the queue
// under some other ordering either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_standard_queue_refuses_a_group_id_and_sends_nothing() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("not-fifo");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    let refused = connected
        .publisher()
        .message(&Body(b"unordered".to_vec()))
        .to(&queue)
        .group_id("orders")
        .publish()
        .await
        .expect_err("a standard queue cannot honour a message group");
    assert!(
        matches!(
            refused,
            PublishError::Publish(SqsError::NotFifo { ref destination, .. })
                if destination == &queue
        ),
        "the refusal names the destination, got {refused}",
    );

    let mut stream = pin!(subscriber.stream());
    assert!(
        tokio::time::timeout(QUIET, stream.next()).await.is_err(),
        "the refused publish reached the queue anyway",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// The group a policy fixes belongs to the position, not to the call: every message a mount site
// publishes through it is ordered within that one group without the body naming anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_group_a_policy_fixes_rides_every_publish() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = fifo("policy-group");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");

    let publisher = SqsPublish::default()
        .group_id("shipments")
        .pair(&connected)
        .await
        .expect("the policy pairs with the connected broker");
    publisher
        .publish(OutgoingMessage::new(&queue, b"shipped".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        message.headers().get_str(PARTITION_KEY_HEADER),
        Some("shipments"),
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
