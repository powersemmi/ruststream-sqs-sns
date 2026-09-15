//! End-to-end checks against a local stack, gated behind `SQS_TEST_ENDPOINT`.
//!
//! Start one with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::num::NonZeroU32;
use std::pin::pin;
use std::time::{Duration, Instant};

use aws_sdk_sqs::types::QueueAttributeName;
use futures::StreamExt;
use ruststream::{
    ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RetryDeclaration,
    Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_sqs_sns::{ConnectedSqsBroker, PARTITION_KEY_HEADER, SqsError, SqsQueue};

mod live;

use live::{QUIET, RECV_TIMEOUT, admin, connect, queue_attribute, unique};

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
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
    let client = admin(endpoint).await;
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

/// The cap the declaration test puts on its queue.
const DECLARED_ATTEMPTS: u32 = 2;

// The declaration a registration makes is topology on SQS, so what proves it arrived is the
// queue's own redrive policy, read back from the service rather than from the descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_declaration_reaches_the_queue_as_its_redrive_policy() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("redrive");
    let dead_letter = unique("redrive-dead");
    let declaration = RetryDeclaration::new()
        .with_max_attempts(NonZeroU32::new(DECLARED_ATTEMPTS).expect("a cap"))
        .with_dead_letter(dead_letter.clone());
    let source = SubscriptionSource::<ConnectedSqsBroker>::declare_retry(
        SqsQueue::new(&queue)
            .create_if_missing()
            .wait(Duration::from_secs(1)),
        &declaration,
    );
    let subscriber = connected
        .subscribe_queue(source)
        .await
        .expect("subscription opens");

    let policy = queue_attribute(&endpoint, &queue, QueueAttributeName::RedrivePolicy).await;
    assert!(
        policy.contains(&format!("\"maxReceiveCount\":\"{DECLARED_ATTEMPTS}\"")),
        "the cap did not reach the queue, got {policy}",
    );
    assert!(
        policy.contains(&dead_letter),
        "the dead-letter queue did not reach the queue, got {policy}",
    );

    drop(subscriber);
    connected.shutdown().await.expect("shutdown succeeds");
}

// A registration mounted by a bare queue name carries no descriptor, so the broker takes the
// declaration. What proves it arrived is the same thing that proves it for a descriptor: the
// queue's own redrive policy, read back from the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_declaration_reaches_the_queue_as_its_redrive_policy() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    // A bare name opens the queue as it stands, so both queues are stood up first.
    let queue = unique("named-redrive");
    let dead_letter = unique("named-redrive-dead");
    for name in [&queue, &dead_letter] {
        drop(
            connected
                .subscribe_queue(
                    SqsQueue::new(name)
                        .create_if_missing()
                        .wait(Duration::from_secs(1)),
                )
                .await
                .expect("the queue is created"),
        );
    }

    let declaration = RetryDeclaration::new()
        .with_max_attempts(NonZeroU32::new(DECLARED_ATTEMPTS).expect("a cap"))
        .with_dead_letter(dead_letter.clone());
    Subscribe::declare_retry(&connected, &queue, &declaration)
        .expect("the broker takes the declaration a bare name carries");
    let subscriber = Subscribe::subscribe(&connected, &queue)
        .await
        .expect("subscription opens");

    let policy = queue_attribute(&endpoint, &queue, QueueAttributeName::RedrivePolicy).await;
    assert!(
        policy.contains(&format!("\"maxReceiveCount\":\"{DECLARED_ATTEMPTS}\"")),
        "the cap did not reach the queue, got {policy}",
    );
    assert!(
        policy.contains(&dead_letter),
        "the dead-letter queue did not reach the queue, got {policy}",
    );

    drop(subscriber);
    connected.shutdown().await.expect("shutdown succeeds");
}

// Half a declaration over a bare name is half a redrive policy, and the broker refuses it before
// anything subscribes rather than opening a subscription under a cap the queue never received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_bare_name_declaration_is_refused_by_the_broker() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let declaration = RetryDeclaration::new().with_dead_letter(unique("named-half-dead"));
    let refused = Subscribe::declare_retry(&connected, &unique("named-half"), &declaration)
        .expect_err("a destination alone is not a redrive policy");
    let reason = refused.to_string();
    assert!(
        reason.contains("max_attempts(..)"),
        "the refusal names the missing half, got {reason}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// A registration that declares one half of a redrive policy would otherwise run with a cap the
// queue never received, so the subscription refuses to open and says which half is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_declaration_refuses_the_subscription() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let declaration = RetryDeclaration::new()
        .with_max_attempts(NonZeroU32::new(DECLARED_ATTEMPTS).expect("a cap"));
    let source = SubscriptionSource::<ConnectedSqsBroker>::declare_retry(
        SqsQueue::new(unique("half")).create_if_missing(),
        &declaration,
    );
    let refused = connected
        .subscribe_queue(source)
        .await
        .expect_err("a cap the queue never receives is not a cap");
    let reason = refused.to_string();
    assert!(
        reason.contains("dead_letter(..)"),
        "the refusal names the missing half, got {reason}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// The cap counts the queue's own receives, so the delivery has to report them. SQS calls it
// ApproximateReceiveCount and counts the delivery in hand, which is what the first receive
// answering one means.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_reports_the_queues_receive_count() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("receives");
    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .create_if_missing()
                .wait(Duration::from_secs(1)),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"counted".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(first.redelivery_count(), Some(1));
    first.nack(true).await.expect("requeue succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.redelivery_count(), Some(2));
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The timeout an operator leaves on the queue in the test below: long enough that a redelivery
/// under it would not arrive within a test's patience.
const SLOW_QUEUE_VISIBILITY: Duration = Duration::from_secs(60);

/// What the descriptor asks for instead.
const DESCRIBED_VISIBILITY: Duration = Duration::from_secs(2);

// The other half of the visibility decision: a descriptor that names one asks for it on every
// receive, so an unsettled delivery comes back under the descriptor's value rather than the
// queue's. The two are told apart by the clock, and only the service can run it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptors_visibility_timeout_is_what_the_receive_asks_for() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let queue = queue_with_visibility(
        &endpoint,
        &unique("asked-visibility"),
        SLOW_QUEUE_VISIBILITY,
    )
    .await;
    let connected = connect(&endpoint).await;

    let mut subscriber = connected
        .subscribe_queue(
            SqsQueue::new(&queue)
                .visibility(DESCRIBED_VISIBILITY)
                .wait(Duration::from_secs(1)),
        )
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"brief".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    // An unsettled drop stops the extension, so what holds the message now is the timeout the
    // receive asked for.
    drop(first);

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery comes back under the descriptor's timeout, not the queue's")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.payload(), b"brief");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The protocol maximum for a long poll, which is also this crate's default.
const LONG_POLL: Duration = Duration::from_secs(20);

// A long poll at the protocol maximum has to survive on two counts: the receive must not be cut
// short by the client's own per-attempt timeout (a poll that dies reaches the stream as an
// error), and it must hand a message over the moment one arrives rather than at the end of the
// wait. A stack is the only place either can be observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_poll_outlives_its_wait_and_returns_on_arrival() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("long-poll");
    let mut subscriber = connected
        .subscribe_queue(SqsQueue::new(&queue).create_if_missing().wait(LONG_POLL))
        .await
        .expect("subscription opens");

    let mut stream = pin!(subscriber.stream());
    assert!(
        tokio::time::timeout(LONG_POLL + QUIET, stream.next())
            .await
            .is_err(),
        "an empty queue handed something over: either a message or the receive's own failure",
    );

    let published = Instant::now();
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"awaited".as_slice()), None)
        .await
        .expect("publish succeeds");
    let message = tokio::time::timeout(QUIET * 2, stream.next())
        .await
        .expect("the poll in flight hands the message over as it arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"awaited");
    assert!(
        published.elapsed() < LONG_POLL,
        "the message waited {:?} for the poll to run out instead of ending it",
        published.elapsed(),
    );
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// Creating the queue is opt-in, so a subscription that did not ask for it says the queue is
// missing instead of conjuring one. The publish side answers the same way, and neither leaves a
// queue behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_queue_is_reported_rather_than_created() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("absent");
    let refused = connected
        .subscribe_queue(SqsQueue::new(&queue))
        .await
        .expect_err("a queue nobody created cannot be subscribed to");
    assert!(
        matches!(refused, SqsError::Queue { ref name, .. } if name == &queue),
        "the refusal names the queue, got {refused}",
    );

    let refused = connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"nowhere".as_slice()), None)
        .await
        .expect_err("a queue nobody created cannot be published to");
    assert!(
        matches!(refused, SqsError::Queue { ref name, .. } if name == &queue),
        "the refusal names the queue, got {refused}",
    );

    assert!(
        admin(&endpoint)
            .await
            .get_queue_url()
            .queue_name(&queue)
            .send()
            .await
            .is_err(),
        "a refused subscription created the queue anyway",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
