//! What the queue's redrive policy does to a delivery, against a local stack.
//!
//! The policy itself is topology and the suite next door reads it back off the queue. This one
//! asks the question that matters to a service: once a registration's cap is spent, does the
//! message actually end up in the dead-letter queue, or does it quietly disappear? The answer
//! is the service's own, because SQS counts the receives and performs the move; the in-process
//! mode models the rule, and a model is not proof.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

use std::num::NonZeroU32;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, RetryDeclaration, Subscriber,
    SubscriptionSource,
};
use ruststream_sqs_sns::{ConnectedSqsBroker, SqsQueue};

mod live;

use live::{QUIET, RECV_TIMEOUT, connect, unique};

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// The descriptor a registration hands over once it has declared a cap and a destination: the
/// same mapping `.max_attempts(n).dead_letter(q)` goes through at a mount site.
fn declared(queue: &str, attempts: u32, dead_letter: &str) -> SqsQueue {
    let declaration = RetryDeclaration::new()
        .with_max_attempts(NonZeroU32::new(attempts).expect("a cap is never zero"))
        .with_dead_letter(dead_letter.to_owned());
    SubscriptionSource::<ConnectedSqsBroker>::declare_retry(
        SqsQueue::new(queue)
            .create_if_missing()
            .wait(Duration::from_secs(1)),
        &declaration,
    )
}

/// Whether `stream` hands anything over within [`QUIET`].
async fn anything_arrives<S, M, E>(stream: &mut S) -> bool
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
{
    tokio::time::timeout(QUIET, stream.next()).await.is_ok()
}

// The cap a registration declares is the queue's `maxReceiveCount`, and what a service is
// promised for spending it is a message in the dead-letter queue rather than a message gone.
// Only the service can answer that: it counts the receives and performs the move.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_that_spends_the_cap_lands_in_the_dead_letter_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("dlq-spent");
    let dead_letter = unique("dlq-spent-dead");
    let mut subscriber = connected
        .subscribe_queue(declared(&queue, 2, &dead_letter))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    for attempt in 1..=2 {
        let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(
            delivery.redelivery_count(),
            Some(attempt),
            "the queue counts the receive in hand",
        );
        delivery.nack(true).await.expect("requeue succeeds");
    }

    // The receive that would take the count past the cap is the one that moves the message, and
    // the subscription's own polling performs it.
    let mut dead = connected
        .subscribe_queue(SqsQueue::new(&dead_letter).wait(Duration::from_secs(1)))
        .await
        .expect("the dead-letter subscription opens");
    let mut dead_stream = pin!(dead.stream());
    let carried = tokio::time::timeout(RECV_TIMEOUT, dead_stream.next())
        .await
        .expect("the spent delivery reaches the dead-letter queue")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(carried.payload(), b"poison");
    carried.ack().await.expect("ack succeeds");

    assert!(
        !anything_arrives(&mut stream).await,
        "the queue handed the message out again after carrying it off",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

// SQS has no verb for "reject without deleting", so a discard deletes - except on the delivery
// that has run the policy out, where being received once more is how the queue carries it to the
// dead-letter queue and a delete would lose it. The cap is one here, so the first delivery is
// already the spent one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discard_hands_a_spent_delivery_to_the_dead_letter_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("dlq-discard");
    let dead_letter = unique("dlq-discard-dead");
    let mut subscriber = connected
        .subscribe_queue(declared(&queue, 1, &dead_letter))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"spent".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(delivery.redelivery_count(), Some(1));
    delivery.nack(false).await.expect("the discard succeeds");

    let mut dead = connected
        .subscribe_queue(SqsQueue::new(&dead_letter).wait(Duration::from_secs(1)))
        .await
        .expect("the dead-letter subscription opens");
    let mut dead_stream = pin!(dead.stream());
    let carried = tokio::time::timeout(RECV_TIMEOUT, dead_stream.next())
        .await
        .expect("the discarded delivery reaches the dead-letter queue instead of being deleted")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(carried.payload(), b"spent");
    carried.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

// The other half of the same rule: a delivery the cap has not spent is discarded by deleting it,
// so it neither comes back nor lands in the dead-letter queue. Without this the exception above
// would be free to swallow every discard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discard_deletes_a_delivery_the_cap_has_not_spent() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;

    let queue = unique("dlq-kept");
    let dead_letter = unique("dlq-kept-dead");
    let mut subscriber = connected
        .subscribe_queue(declared(&queue, 3, &dead_letter))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"dropped".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(delivery.redelivery_count(), Some(1));
    delivery.nack(false).await.expect("the discard succeeds");

    let mut dead = connected
        .subscribe_queue(SqsQueue::new(&dead_letter).wait(Duration::from_secs(1)))
        .await
        .expect("the dead-letter subscription opens");
    let mut dead_stream = pin!(dead.stream());
    assert!(
        !anything_arrives(&mut dead_stream).await,
        "a delivery with receives to spare went to the dead-letter queue",
    );
    assert!(
        !anything_arrives(&mut stream).await,
        "the discarded delivery came back instead of being deleted",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
