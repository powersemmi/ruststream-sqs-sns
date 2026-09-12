//! End-to-end checks against a local stack, gated behind `SQS_TEST_ENDPOINT`.
//!
//! Start one with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::types::QueueAttributeName;
use futures::StreamExt;
use ruststream::runtime::PublishExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, Outgoing, OutgoingMessage, Publisher,
    Serialized, Subscriber,
};
use ruststream_sqs_sns::{
    ConnectedSqsBroker, PARTITION_KEY_HEADER, SqsBroker, SqsPublishSteps, SqsQueue,
};

const RECV_TIMEOUT: Duration = Duration::from_secs(20);

/// The body the FIFO tests publish through the builder: bytes the test already holds encoded,
/// so the type names itself serialized and no codec sits on the path.
#[derive(Outgoing, Serialized)]
struct Body(Vec<u8>);

mod live;

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

async fn connect(endpoint: &str) -> ConnectedSqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique queue, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_headers_and_partition_key() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&queue, b"{\"id\":1}".as_slice()).with_headers(headers),
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

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_payloads_survive_the_text_body() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("binary");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
        .await
        .expect("subscription opens");

    let raw = [0u8, 159, 146, 150, 255];
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&queue, raw.as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), raw.as_slice());
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_with_requeue_redelivers() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("requeue");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&queue, b"again".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    first.nack(true).await.expect("requeue succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.payload(), b"again");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// How long the delayed-retry test asks SQS to hold the message.
const RETRY_DELAY: Duration = Duration::from_secs(5);

// A delayed negative acknowledgement must ride the queue's own visibility timeout, which is what
// the delivery advertises with `supports_nack_after`; the runtime's deferred re-publish fallback
// is the alternative the flag turns off. The two are told apart by the clock: an immediate
// requeue comes back on the next poll, while a visibility set to the delay holds the message for
// it. The span is measured from the settle call, so it also contains the receive round trip and
// can only overshoot the delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_after_delays_the_redelivery() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("delay");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(1)),
        )
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&queue, b"not-yet".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(
        first.supports_nack_after(),
        "an SQS delivery sets its own visibility, so it must advertise native delayed redelivery",
    );

    let settled = Instant::now();
    first
        .nack_after(RETRY_DELAY)
        .await
        .expect("delayed requeue succeeds");

    let second = tokio::time::timeout(RETRY_DELAY * 6, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    let elapsed = settled.elapsed();
    assert!(
        elapsed >= RETRY_DELAY,
        "the redelivery came back after {elapsed:?}, before the {RETRY_DELAY:?} the settle asked \
         for; the visibility timeout holds the message for the delay",
    );
    assert_eq!(second.payload(), b"not-yet");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sns_fans_out_to_a_subscribed_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("fanout");
    let topic = unique("topic");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
        .await
        .expect("subscription opens");
    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("queue subscribes to topic");

    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "acme");
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
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
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

    let queue = format!("{}.fifo", unique("group"));
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
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

    let queue = format!("{}.fifo", unique("groupwin"));
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(5)),
        )
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

/// The visibility timeout the operator configures on the queue in this test. Short, so a
/// handler that outlives it does so within a test's patience; nothing else about the number
/// matters.
const OPERATOR_VISIBILITY: Duration = Duration::from_secs(2);

/// How long the handler holds the delivery: several times the queue's timeout, so an extender
/// that re-armed anything but the queue's own value would have let the message back out.
const HOLD: Duration = Duration::from_secs(6);

/// Provisions a queue whose visibility timeout is not the SQS default, the way an operator
/// would, and returns its name.
///
/// The timeout is set in its own call rather than as a create attribute, so a rerun against a
/// stack that still holds the queue configures it instead of colliding with it.
async fn queue_with_visibility(endpoint: &str, name: &str, visibility: Duration) -> String {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    let client = aws_sdk_sqs::Client::new(&config);
    let url = client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .expect("the operator's queue is created")
        .queue_url()
        .expect("CreateQueue returns the URL")
        .to_owned();
    client
        .set_queue_attributes()
        .queue_url(url)
        .attributes(
            QueueAttributeName::VisibilityTimeout,
            visibility.as_secs().to_string(),
        )
        .send()
        .await
        .expect("the operator sets the visibility timeout");
    name.to_owned()
}

// A subscription that names no visibility of its own must hold its deliveries under the queue's
// configured timeout. The crate used to re-arm a hard-coded 30 seconds every 15 instead, which on
// a queue configured for less handed the message back to the queue while the handler still held
// it, and on a queue configured for more silently shortened what the operator set. The clock
// tells the two apart: with the queue's own value the extender re-arms inside the window, so
// nothing redelivers while the delivery is alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_delivery_rides_the_queues_own_visibility_timeout() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let queue = queue_with_visibility(&endpoint, &unique("visibility"), OPERATOR_VISIBILITY).await;
    let connected = connect(&endpoint).await;

    // No `visibility(..)` on the descriptor: the queue's setting is the one under test.
    let mut subscriber = connected
        .subscribe_queue(SqsQueue::new(&queue).wait(Duration::from_secs(1)))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"held".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let held = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(held.payload(), b"held");

    // The handler is still working: the delivery stays unsettled and alive for the whole hold.
    let redelivered = tokio::time::timeout(HOLD, stream.next()).await;
    assert!(
        redelivered.is_err(),
        "the queue took the message back while it was still held, so the extension did not \
         ride the queue's own visibility timeout",
    );

    held.ack().await.expect("ack succeeds");
    connected.shutdown().await.expect("shutdown succeeds");
}
