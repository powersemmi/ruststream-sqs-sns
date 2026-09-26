//! What the in-process mode of `SqsBroker` does that SQS and SNS do, and what it refuses because
//! they refuse it.
//!
//! The service-level cases run the production app under `TestApp`. The cases whose subject is the
//! transport itself (a receive, a visibility timeout, a refused send, the routing answer) drive
//! the connected broker the in-process transition produces.

#![cfg(feature = "testing")]

use std::error::Error;
use std::io;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
#[cfg(feature = "sns")]
use ruststream::PublishPolicy;
use ruststream::testing::{InProcess, TestApp, TestableBroker};
use ruststream::{
    BatchSubscriber, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::{ConnectedSqsBroker, RECEIVE_COUNT_HEADER, SqsError};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The broker a service's `main` builds.
fn broker() -> SqsBroker {
    SqsBroker::new().region("eu-west-1")
}

/// The production broker, connected in process.
async fn connected() -> Result<ConnectedSqsBroker, SqsError> {
    broker().connect_in_process().await
}

#[subscriber(SqsQueue)]
async fn take(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue)]
async fn take_too(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Two subscriptions on one queue compete for its messages: the queue hands each one to a single
/// receiver, where a topic would copy it to both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_hands_each_message_to_one_of_its_subscriptions() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(take.name("claims"));
        b.include(take_too.name("claims"));
    });
    let tb = TestApp::start(app).await?;

    for id in [1, 2, 3] {
        tb.broker::<SqsBroker>()
            .message(&Order { id })
            .to("claims")
            .publish()
            .await?;
    }

    tb.broker::<SqsBroker>()
        .subscriber("claims")
        .assert_called(3);
    tb.shutdown().await?;
    Ok(())
}

/// A FIFO queue remembers a deduplication id for five minutes: a second send under the same id
/// is accepted and not delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fifo_queue_delivers_a_deduplication_id_once() -> Result<(), Box<dyn Error>> {
    let app = RustStream::new(AppInfo::new("dedup", "0.1.0")).with_broker(broker(), |b| {
        b.include(take.name("orders.fifo"));
        b.after_startup(Publish::default(), async move |sqs| -> io::Result<()> {
            for _ in 0..2 {
                sqs.message(&Order { id: 1 })
                    .to("orders.fifo")
                    .deduplication_id("order-1")
                    .publish()
                    .await
                    .map_err(io::Error::other)?;
            }
            Ok(())
        });
    });
    let tb = TestApp::start(app).await?;
    tb.settle().await?;

    tb.broker::<SqsBroker>()
        .published::<Order>("orders.fifo")
        .assert_called(2);
    tb.broker::<SqsBroker>()
        .subscriber("orders.fifo")
        .assert_called_once()
        .with(&Order { id: 1 });
    tb.shutdown().await?;
    Ok(())
}

/// SQS takes a dead-letter queue of the queue's own kind only, and refuses the redrive policy
/// otherwise; the subscription does not open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_queue_of_the_other_kind_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(take.name("claims.fifo"))
            .max_attempts(nonzero!(3u32))
            .dead_letter("claims-dead");
    });

    let refused = TestApp::start(app)
        .await
        .expect_err("SQS refuses a standard dead-letter queue for a FIFO queue");
    let reason = refused.to_string();
    assert!(
        reason.contains("FIFO"),
        "the refusal names the kind, got {reason}"
    );
}

/// A redrive policy's `maxReceiveCount` is at most 1000 on SQS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cap_over_a_thousand_receives_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(take.name("claims"))
            .max_attempts(nonzero!(1001u32))
            .dead_letter("claims-dead");
    });

    let refused = TestApp::start(app)
        .await
        .expect_err("SQS refuses a maxReceiveCount over 1000");
    let reason = refused.to_string();
    assert!(
        reason.contains("1000"),
        "the refusal names the bound, got {reason}"
    );
}

/// A queue name is at most 80 characters.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_name_sqs_does_not_take_refuses_to_start() {
    let long = "q".repeat(81);
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(take.name(long));
    });

    let refused = TestApp::start(app)
        .await
        .expect_err("SQS refuses a queue name over 80 characters");
    let reason = refused.to_string();
    assert!(
        reason.contains("80"),
        "the refusal names the bound, got {reason}"
    );
}

/// A message dropped without a settlement is not lost: once its visibility timeout lapses, the
/// queue hands it out again, one receive further on.
#[tokio::test(start_paused = true)]
async fn an_unsettled_message_comes_back_once_its_visibility_lapses() -> Result<(), Box<dyn Error>>
{
    let connected = connected().await?;
    let mut subscriber = connected
        .subscribe_queue(SqsQueue::new("claims").visibility(Duration::from_secs(30)))
        .await?;
    connected
        .publisher()
        .publish(OutgoingMessage::new("claims", b"claim".as_slice()), None)
        .await?;

    let mut stream = pin!(subscriber.stream());
    let first = stream.next().await.ok_or("the stream ended")??;
    assert_eq!(first.redelivery_count(), Some(1));
    drop(first);

    let early = tokio::time::timeout(Duration::from_secs(29), stream.next()).await;
    assert!(
        early.is_err(),
        "the message stays invisible until its timeout lapses"
    );

    let again = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or("the stream ended")??;
    assert_eq!(again.payload(), b"claim");
    assert_eq!(
        again.headers().get(RECEIVE_COUNT_HEADER),
        Some(b"2".as_slice())
    );
    again.ack().await?;
    Ok(())
}

/// A FIFO group with a message in flight hands out nothing more until that message is deleted;
/// another group is not held up, and one receive hands out a free group's messages in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fifo_group_waits_for_its_message_in_flight() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let mut subscriber = connected
        .subscribe_queue(SqsQueue::new("orders.fifo"))
        .await?;
    let publisher = connected.publisher();
    for (body, group) in [("a1", "a"), ("a2", "a"), ("b1", "b"), ("b2", "b")] {
        publisher
            .publish(
                OutgoingMessage::new("orders.fifo", body.as_bytes()),
                Some(&SqsPublishOptions::default().group_id(group)),
            )
            .await?;
    }

    {
        let mut single = pin!(subscriber.batches(nonzero!(1)));
        let a1 = single.next().await.ok_or("the stream ended")??;
        assert_eq!(a1[0].payload(), b"a1");
        let b1 = single.next().await.ok_or("the stream ended")??;
        assert_eq!(
            b1[0].payload(),
            b"b1",
            "group a is held while a1 is in flight"
        );
        for message in a1.into_iter().chain(b1) {
            message.ack().await?;
        }
    }

    let mut whole = pin!(subscriber.batches(nonzero!(10)));
    let rest = whole.next().await.ok_or("the stream ended")??;
    let bodies: Vec<&[u8]> = rest.iter().map(IncomingMessage::payload).collect();
    assert_eq!(bodies, [b"a2".as_slice(), b"b2".as_slice()]);
    Ok(())
}

/// The error a publish of `headers` reports.
async fn refused_headers(headers: HeaderMap) -> Result<String, Box<dyn Error>> {
    let connected = connected().await?;
    let _subscriber = connected.subscribe_queue(SqsQueue::new("orders")).await?;
    let refused = connected
        .publisher()
        .publish(
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .err()
        .ok_or("SQS refuses the message, so the publish must fail")?;
    Ok(refused.to_string())
}

/// Every header becomes a message attribute, and a message carries at most ten of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_with_more_than_ten_headers_is_refused() -> Result<(), Box<dyn Error>> {
    let mut headers = HeaderMap::new();
    for index in 0..11 {
        headers.insert(format!("x-header-{index}"), "value");
    }
    let reason = refused_headers(headers).await?;
    assert!(reason.contains("at most 10"), "got {reason}");
    Ok(())
}

/// An attribute's name is letters, digits, `_`, `-` and `.`, and its value is never empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_sqs_cannot_carry_is_refused() -> Result<(), Box<dyn Error>> {
    let mut spaced = HeaderMap::new();
    spaced.insert("x trace", "1");
    let reason = refused_headers(spaced).await?;
    assert!(reason.contains("\"x trace\""), "got {reason}");

    let mut empty = HeaderMap::new();
    empty.insert("x-trace", "");
    let reason = refused_headers(empty).await?;
    assert!(reason.contains("empty value"), "got {reason}");
    Ok(())
}

/// SQS takes no message with an empty body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_body_is_refused() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let refused = connected
        .publisher()
        .publish(OutgoingMessage::new("orders", b"".as_slice()), None)
        .await
        .err()
        .ok_or("SQS refuses an empty body")?;
    assert!(refused.to_string().contains("empty"), "got {refused}");
    Ok(())
}

/// A FIFO topic drops a repeated deduplication id before its fan-out, so a standard queue
/// subscribed to it, whose copy carries no id of its own, receives the message once.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fifo_topic_fans_a_deduplication_id_out_once() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let mut inbox = connected.subscribe_queue(SqsQueue::new("audit")).await?;
    connected
        .subscribe_queue_to_topic("events.fifo", "audit")
        .await?;
    let options = SqsPublishOptions::default()
        .group_id("orders")
        .deduplication_id("order-1");
    let sns = connected.sns_publisher();
    for _ in 0..2 {
        sns.publish(
            OutgoingMessage::new("events.fifo", b"once".as_slice()),
            Some(&options),
        )
        .await?;
    }

    let mut batches = pin!(inbox.batches(nonzero!(10)));
    let received = batches.next().await.ok_or("the stream ended")??;
    let bodies: Vec<&[u8]> = received.iter().map(IncomingMessage::payload).collect();
    assert_eq!(bodies, [b"once".as_slice()]);
    Ok(())
}

/// SNS refuses to subscribe a FIFO queue to a standard topic.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fifo_queue_cannot_subscribe_to_a_standard_topic() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let refused = connected
        .subscribe_queue_to_topic("events", "orders.fifo")
        .await
        .err()
        .ok_or("SNS refuses the subscription")?;
    assert!(matches!(refused, SqsError::Admin { .. }), "got {refused:?}");
    Ok(())
}

/// A queue hands a message to one receiver, so of the subscriptions reading it the first is the
/// one owed it; a name, a dotted alias of it and its queue URL are one queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_a_queue_routes_to_one_subscription_of_it() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let subscriptions = ["audit", "order-events", "orders", "order-events"];
    assert_eq!(connected.routes("order-events", &subscriptions), [1]);
    assert_eq!(connected.routes("order.events", &subscriptions), [1]);
    assert_eq!(
        connected.routes(
            "https://sqs.eu-west-1.amazonaws.com/000000000000/orders",
            &subscriptions
        ),
        [2],
    );
    assert!(connected.routes("payments", &subscriptions).is_empty());
    Ok(())
}

/// A topic copies a message to every queue subscribed to it.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_a_topic_routes_to_every_subscribed_queue() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    for queue in ["billing", "shipping"] {
        connected.subscribe_queue_to_topic("events", queue).await?;
    }
    let subscriptions = ["billing", "orders", "shipping"];
    assert_eq!(connected.routes("events", &subscriptions), [0, 2]);
    Ok(())
}

/// A topic and a queue may share a name, and a publish is routed by the publisher that sent it:
/// the queue's own subscription is owed what was sent to the queue, the queues subscribed to the
/// topic are owed what was published to the topic. The harness asks once per publish, in publish
/// order.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_and_a_topic_of_one_name_route_by_the_publisher() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    connected
        .subscribe_queue_to_topic("events", "billing")
        .await?;
    let queue = SqsPublish::default().pair(&connected).await?;
    let topic = SnsPublish::default().pair(&connected).await?;
    queue
        .publish(OutgoingMessage::new("events", b"sent".as_slice()), None)
        .await?;
    topic
        .publish(
            OutgoingMessage::new("events", b"published".as_slice()),
            None,
        )
        .await?;

    let subscriptions = ["events", "billing"];
    let mut owed = [0; 2];
    for _ in 0..2 {
        for position in connected.routes("events", &subscriptions) {
            owed[position] += 1;
        }
    }
    assert_eq!(owed, [1, 1]);
    Ok(())
}

/// A topic reaches the queues its policy names as subscribed outside the service, and a publish
/// to a topic never reaches a queue merely because the queue carries the topic's name.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_publish_reaches_the_queues_its_policy_names() -> Result<(), Box<dyn Error>> {
    let connected = connected().await?;
    let topic = SnsPublish::default()
        .fans_out_to(["notify"])
        .pair(&connected)
        .await?;
    topic
        .publish(
            OutgoingMessage::new("events", b"published".as_slice()),
            None,
        )
        .await?;

    let subscriptions = ["events", "notify"];
    assert_eq!(connected.routes("events", &subscriptions), [1]);
    Ok(())
}
