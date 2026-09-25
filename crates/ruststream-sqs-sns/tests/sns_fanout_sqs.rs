//! SNS fan-out into SQS against a local stack.
//!
//! SNS appears in this crate as a publisher only, so what a service is promised is that a
//! notification reaches the queues subscribed to the topic, with its payload and its headers
//! intact. The in-process mode models the fan-out; this suite is what holds that model to the
//! service.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

#![cfg(feature = "sns")]

use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, PublishPolicy,
    Publisher, Serialized, Subscriber,
};
use ruststream_sqs_sns::{PARTITION_KEY_HEADER, SnsPublish, SqsPublishSteps, SqsQueue};

mod live;

use live::{QUIET, RECV_TIMEOUT, connect, unique};

/// The body the builder tests publish: bytes the test already holds encoded, so the type names
/// itself serialized and no codec sits on the path.
#[derive(Outgoing, Serialized)]
struct Body(Vec<u8>);

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// The subscription every test here opens: short polling, so a run does not sit on an empty
/// queue for the protocol maximum.
fn source(queue: &str) -> SqsQueue {
    SqsQueue::new(queue)
        .create_if_missing()
        .wait(Duration::from_secs(1))
}

// Raw message delivery is what `subscribe_queue_to_topic` turns on, and the proof of it is the
// delivery itself: the payload arrives as it was published rather than wrapped in the
// notification envelope, and the attributes arrive as headers - a value the service will not
// carry as text included, which travels as a binary attribute on both hops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sns_fans_out_to_a_subscribed_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("fanout");
    let topic = unique("topic");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");
    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("queue subscribes to topic");

    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "acme");
    headers.insert("x-trace", Bytes::from_static(&[0u8, 1, 2, 3]));
    let sns = connected.sns_publisher();
    sns.publish(
        OutgoingMessage::new(&topic, b"notice".as_slice()).with_headers(headers),
        None,
    )
    .await
    .expect("sns publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"notice");
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(
        message.headers().get("x-trace"),
        Some([0u8, 1, 2, 3].as_slice()),
        "a header the service refuses as text survived the topic as bytes",
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// The policy is how a mount site says "this position fans out" instead of sending to a queue of
// the same name. Paired against the connected broker it has to produce the SNS publisher, and
// the way to tell is the destination: a queue named for the topic stays empty, while the queue
// subscribed to the topic receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fan_out_policy_publishes_through_the_topic() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let name = unique("policy-fanout");
    let queue = unique("policy-fanout-inbox");
    let mut inbox = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("the subscribed queue opens");
    // A queue of the topic's own name, which is where the default policy would have sent this.
    let mut namesake = connected
        .subscribe_queue(source(&name))
        .await
        .expect("the namesake queue opens");
    connected
        .subscribe_queue_to_topic(&name, &queue)
        .await
        .expect("queue subscribes to topic");

    let publisher = SnsPublish::default()
        .pair(&connected)
        .await
        .expect("the policy pairs with the connected broker");
    publisher
        .publish(OutgoingMessage::new(&name, b"fanned".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(inbox.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the fan-out reaches the subscribed queue")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"fanned");
    message.ack().await.expect("ack succeeds");

    let mut namesake_stream = pin!(namesake.stream());
    assert!(
        tokio::time::timeout(QUIET, namesake_stream.next())
            .await
            .is_err(),
        "the fan-out policy sent to a queue of the topic's name instead of the topic",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// A `.fifo` topic is the one SNS refuses to open unless the topic is declared FIFO, which is
// what makes this the only path where the per-message settings reach a notification at all. The
// group the send names has to survive the topic and arrive on the queue's delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fifo_topic_carries_the_message_group_to_the_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let topic = format!("{}.fifo", unique("fifo-topic"));
    let queue = format!("{}.fifo", unique("fifo-topic-inbox"));
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("the fifo queue opens");
    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("the fifo queue subscribes to the fifo topic");

    connected
        .sns_publisher()
        .message(&Body(b"ordered".to_vec()))
        .to(&topic)
        .group_id("user-42")
        .publish()
        .await
        .expect("the notification is published");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the fan-out reaches the subscribed queue")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"ordered");
    assert_eq!(
        message.headers().get_str(PARTITION_KEY_HEADER),
        Some("user-42"),
        "the group the send named did not survive the topic",
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// A framework destination carries dots freely, and neither service takes them in a name. The
// queue side has always mapped them; the topic side has to map them the same way, or a reply
// that lands on a queue named `order-events` cannot be fanned out at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dotted_destination_name_reaches_the_topic() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let topic = format!("{}.events", unique("dotted"));
    let queue = unique("dotted-inbox");
    let mut subscriber = connected
        .subscribe_queue(source(&queue))
        .await
        .expect("subscription opens");
    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("queue subscribes to the dotted topic");

    connected
        .sns_publisher()
        .publish(OutgoingMessage::new(&topic, b"dotted".as_slice()), None)
        .await
        .expect("the notification is published");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the fan-out reaches the subscribed queue")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"dotted");
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
