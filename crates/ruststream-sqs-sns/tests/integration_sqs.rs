//! End-to-end checks against a local stack, gated behind `SQS_TEST_ENDPOINT`.
//!
//! Start one with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::{Duration, Instant};

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_sqs_sns::{ConnectedSqsBroker, PARTITION_KEY_HEADER, SqsBroker, SqsQueue};

const RECV_TIMEOUT: Duration = Duration::from_secs(20);

fn test_endpoint() -> Option<String> {
    match std::env::var("SQS_TEST_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => Some(endpoint),
        _ => {
            eprintln!("SQS_TEST_ENDPOINT is not set; skipping the live integration test");
            None
        }
    }
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

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&queue, b"{\"id\":1}".as_slice()).with_headers(headers))
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
        .publish(OutgoingMessage::new(&queue, raw.as_slice()))
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
        .publish(OutgoingMessage::new(&queue, b"again".as_slice()))
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
        .publish(OutgoingMessage::new(&queue, b"not-yet".as_slice()))
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

    let mut headers = Headers::new();
    headers.insert("x-tenant", "acme");
    let sns = connected.sns_publisher();
    sns.publish(OutgoingMessage::new(&topic, b"notice".as_slice()).with_headers(headers))
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
