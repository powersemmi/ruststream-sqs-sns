//! The declaration a service ships, run on the production broker in process.
//!
//! What a production routes file writes is `#[subscriber(SqsQueue::new(..))]`, whatever the mount
//! site chains onto it, and the publish policy the reply position is bound to. That exact wiring
//! has to start, receive, reply and settle under the harness - otherwise the thing under test is
//! a rewrite of the service rather than the service.

#![cfg(feature = "testing")]

use std::io;
use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::{PARTITION_KEY_HEADER, RECEIVE_COUNT_HEADER};
use serde::{Deserialize, Serialize};

/// The broker every app below is built on, as a service's `main` builds it.
fn broker() -> SqsBroker {
    SqsBroker::new().region("eu-west-1")
}

/// The payload the handlers below take, and the producer publishes: a decoded type, so the
/// default codec sits on the path the way a service's does.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// What a reply-shaped handler hands back.
#[derive(Debug, PartialEq, Deserialize, Outgoing, Serialize)]
struct OrderPlaced {
    id: u64,
}

/// The polling options are on the descriptor, exactly as the README and the examples write
/// them. In process they resolve to the queue name and nothing else.
#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(20))
        .visibility(Duration::from_secs(30))
)]
async fn handle_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A batch handler whose size and queue options are named at the mount site instead, through
/// this crate's settings trait.
#[subscriber(SqsQueue::new("payments"))]
async fn reconcile(payments: &[Order]) -> HandlerOutcome {
    let _ = payments.len();
    HandlerOutcome::ack()
}

/// A batch handler on a queue whose mount site asks for more than one receive can return.
#[subscriber(SqsQueue::new("ledger"))]
async fn post_ledger(entries: &[Order]) -> HandlerOutcome {
    let _ = entries.len();
    HandlerOutcome::ack()
}

/// A reply-shaped handler: it says where the reply goes, and the mount site says who takes it
/// there. Every mount below reuses this one definition, which is the point - the policy is the
/// only thing that differs.
#[subscriber(SqsQueue::new("accepted"), publish("order-events"))]
async fn accept(order: &Order) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

/// The consumer on the FIFO destination the grouped producer publishes to.
#[subscriber(SqsQueue::new("shipments.fifo"))]
async fn ship(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// How long a deferred delivery is asked to wait, and the window the test advances past.
const RETRY_DELAY: Duration = Duration::from_secs(45);

/// A handler that is never ready: every delivery asks to come back later, so the wait itself is
/// what the test observes.
#[subscriber(SqsQueue::new("invoices"))]
async fn defer(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The third spelling: a bare queue name, with no descriptor between the registration and the
/// broker. It asks for a retry every time, so the cap is what ends the delivery.
#[subscriber("orders")]
async fn reprice(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// The other spelling: the definition fixes the kind and the mount site names the queue, which
/// is what lets one handler run against two queues.
#[subscriber(SqsQueue)]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The slot both bodies below publish through, so the harness records what each publish asked
/// for.
#[derive(OutSlot)]
#[publishes(Order)]
struct Shipments;

/// A body that adjusts a per-message setting: it names this crate's step, so it bounds its slot
/// on this crate's options type.
#[subscriber(SqsQueue::new("dispatch"))]
async fn dispatch(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome {
    if shipments
        .message(order)
        .to("shipments.fifo")
        .group_id("user-9")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The same slot, with nothing adjusted: whatever the mount site fixed is the whole answer.
#[subscriber(SqsQueue::new("forward"))]
async fn forward(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome {
    if shipments
        .message(order)
        .to("shipments.fifo")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_declared_for_sqs_mounts_in_process() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(handle_order);
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_definition_that_fixes_only_the_kind_takes_the_mount_sites_name() {
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker(), |b| {
        b.include(audit.name("audit-trail").wait(Duration::from_secs(10)));
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 8 })
        .to("audit-trail")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .subscriber("audit-trail")
        .assert_called_once()
        .with(&Order { id: 8 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_site_settings_ride_that_descriptor_in_process() {
    let broker = broker();
    // A producer handle taken before the app is built: the harness's own publish drives each
    // message to a standstill, which would close a batch per message and say nothing about the
    // size the mount site named.
    let producer = broker.publisher();
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // The framework's step first, then this crate's - the order a routes file writes.
        b.include(
            reconcile
                .batch(nonzero!(2))
                .wait(Duration::from_secs(20))
                .visibility(Duration::from_secs(30))
                .create_if_missing(),
        );
    });

    let tb = TestApp::start(app).await.expect("the app starts");
    for id in [1, 2] {
        producer
            .message(&Order { id })
            .to("payments")
            .publish()
            .await
            .expect("the publish succeeds");
    }
    tb.settle().await.expect("the batch settles");

    tb.broker::<SqsBroker>()
        .subscriber("payments")
        .assert_called_once()
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

/// One `ReceiveMessage` returns at most ten messages, and one receive is one batch, so a batch
/// never holds more than ten on the queue. A mount site may ask for more; the in-process queue
/// caps the batches where SQS caps them, so a handler that counts on a larger batch finds out here
/// rather than in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_never_holds_more_than_one_receive_returns() {
    let broker = broker();
    // A producer handle taken before the app is built, for the reason the test above gives: the
    // harness's own publish would close a batch per message.
    let producer = broker.publisher();
    let app = RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(broker, |b| {
        b.include(post_ledger.batch(nonzero!(25)));
    });

    let tb = TestApp::start(app).await.expect("the app starts");
    for id in 0..25 {
        producer
            .message(&Order { id })
            .to("ledger")
            .publish()
            .await
            .expect("the publish succeeds");
    }
    tb.settle().await.expect("the batches settle");

    // Where the batches split depends on how the publishes fall against the receive's wait, so
    // the case pins the cap and the total, not the shape.
    let broker = tb.broker::<SqsBroker>();
    let ledger = broker.subscriber("ledger");
    let sizes: Vec<usize> = ledger.batches::<Order>().iter().map(Vec::len).collect();
    assert!(
        sizes.iter().all(|size| *size <= 10),
        "a batch held more than one receive returns: {sizes:?}"
    );
    assert_eq!(sizes.iter().sum::<usize>(), 25, "batches {sizes:?}");
    ledger.settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_takes_the_brokers_default_policy_in_process() {
    let app = RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(broker(), |b| {
        // No `.out(..)`: the reply rides whatever the connected broker names as its default
        // policy, and in process that has to be the production one.
        b.include(accept);
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 1 })
        .to("accepted")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 1 });

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_publish_policy_mounts_in_process() {
    let app = RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(broker(), |b| {
        // The line a routes file writes, unchanged: the broker under it is the only
        // difference between this and production.
        b.include(accept).out_reply(Publish::default());
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 2 })
        .to("accepted")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 2 });

    tb.shutdown().await.expect("the app shuts down");
}

#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fan_out_policy_mounts_the_same_way() {
    let app = RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(broker(), |b| {
        b.include(accept).out_reply(SnsPublish::default());
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 3 })
        .to("accepted")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    // The reply reached the destination the fan-out policy names. Onward delivery to the queues
    // subscribed to that topic is SNS's own work and belongs to the live suite.
    tb.broker::<SqsBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 3 });

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_startup_hook_publishes_under_the_group_the_policy_fixed() {
    let app = RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(broker(), |b| {
        b.include(ship);
        // The hook takes the live form of the production policy, so what it may call on that
        // publisher is what decides whether the service's own startup code compiles here.
        b.after_startup(
            Publish::default().group_id("user-42"),
            async move |sqs| -> io::Result<()> {
                sqs.message(&Order { id: 4 })
                    .to("shipments.fifo")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            },
        );
    });

    let tb = TestApp::start(app).await.expect("the app starts");
    tb.settle().await.expect("the startup publish settles");

    tb.broker::<SqsBroker>()
        .subscriber("shipments.fifo")
        .assert_called_once()
        .with(&Order { id: 4 })
        .settled(HandlerOutcome::ack());
    // The group reaches the delivery where SQS puts it: the partition-key header a FIFO
    // delivery carries its message group id in.
    tb.broker::<SqsBroker>()
        .published::<Order>("shipments.fifo")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "user-42");

    tb.shutdown().await.expect("the app shuts down");
}

/// The step on the publish builder wins over the group the policy fixed, for that one message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_step_wins_over_the_group_the_policy_fixed() {
    let app = RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(broker(), |b| {
        b.include(ship);
        b.after_startup(
            Publish::default().group_id("user-42"),
            async move |sqs| -> io::Result<()> {
                sqs.message(&Order { id: 5 })
                    .to("shipments.fifo")
                    .group_id("user-7")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            },
        );
    });

    let tb = TestApp::start(app).await.expect("the app starts");
    tb.settle().await.expect("the startup publish settles");

    tb.broker::<SqsBroker>()
        .published::<Order>("shipments.fifo")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "user-7");

    tb.shutdown().await.expect("the app shuts down");
}

/// The slot view records what a publish through an `Out` slot asked for, which is where a test
/// reads back a setting the transport has already folded into its own protocol fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_slot_view_reads_back_the_options_a_publish_carried() {
    let app = RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(broker(), |b| {
        b.include(ship);
        b.include(dispatch)
            .out(Shipments, Publish::default().group_id("user-42"))
            .build();
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 6 })
        .to("dispatch")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.out::<Shipments>()
        .assert_called_once()
        .with_options(&SqsPublishOptions::default().group_id("user-9"));

    tb.shutdown().await.expect("the app shuts down");
}

/// A publish that names no step carries no options at all, and the policy's group is the whole
/// answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unstepped_slot_publish_carries_the_policy_defaults() {
    let app = RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(broker(), |b| {
        b.include(ship);
        b.include(forward)
            .out(Shipments, Publish::default().group_id("user-42"))
            .build();
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 7 })
        .to("forward")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.out::<Shipments>()
        .assert_called_once()
        .assert_options_default();
    tb.broker::<SqsBroker>()
        .published::<Order>("shipments.fifo")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "user-42");

    tb.shutdown().await.expect("the app shuts down");
}

/// `retry_after` is a queue operation on SQS, and it has to stay one in process: the delivery
/// comes back on its own once the delay has passed, and nothing is republished to get it there.
///
/// Nothing is bound for it either: the queue carries a spent delivery away itself, so the
/// registration declares a cap and a destination and has no retry publisher to name. The queue's
/// log still holds the one original.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_waits_out_the_delay_in_process() {
    let app = RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker(), |b| {
        b.include(defer);
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 9 })
        .to("invoices")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .subscriber("invoices")
        .assert_called_once()
        .with(&Order { id: 9 })
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await.expect("the delay elapses");

    tb.broker::<SqsBroker>()
        .subscriber("invoices")
        .assert_called(2);
    tb.broker::<SqsBroker>()
        .published::<Order>("invoices")
        .assert_called_once();

    tb.shutdown().await.expect("the app shuts down");
}

/// A handler on a capped queue that never settles a delivery: every one asks to come back after
/// the delay, so the queue's redrive policy is what ends the message.
#[subscriber(SqsQueue::new("claims"))]
async fn appraise(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same on the immediate path: `retry()` asks for the message back at once.
#[subscriber(SqsQueue::new("disputes"))]
async fn arbitrate(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// What one delivery of `count_attempts` reported as its receive count.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Attempt {
    id: u64,
    receives: Option<String>,
}

/// Where `count_attempts` reports each attempt, so the test reads the counts off the broker.
#[derive(OutSlot)]
#[publishes(Attempt)]
struct Attempts;

/// A handler that reads how many times the queue has handed this message over. The header is
/// the crate's own spelling of `ApproximateReceiveCount`, and it reports every reading before it
/// asks for the message again.
#[subscriber(SqsQueue::new("attempts"))]
async fn count_attempts(
    order: &Order,
    cx: &mut Context<'_>,
    Out(attempts): Out<impl Publisher, Attempts>,
) -> HandlerOutcome {
    let receives = cx
        .headers()
        .get(RECEIVE_COUNT_HEADER)
        .map(|value| String::from_utf8_lossy(value).into_owned());
    let attempt = Attempt {
        id: order.id,
        receives,
    };
    if attempts
        .message(&attempt)
        .to("attempt-log")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::retry()
}

/// The declaration is the one spelling of a cap on every broker, and on a queue it is the
/// redrive policy: the message is handed over three times and the queue then carries it to the
/// dead-letter queue itself. Nothing this process publishes is involved, which is why the
/// registration binds no retry publisher.
#[tokio::test(start_paused = true)]
async fn a_capped_delivery_ends_in_the_dead_letter_queue() {
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(appraise)
            .max_attempts(nonzero!(3u32))
            .dead_letter("claims-dead");
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 7 })
        .to("claims")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    // One advance per lapsed visibility timeout: the first two hand the delivery back, the third
    // finds the receives spent and moves the message.
    for _ in 0..3 {
        tb.advance(RETRY_DELAY).await.expect("the delay elapses");
    }

    tb.broker::<SqsBroker>()
        .subscriber("claims")
        .assert_called(3);
    tb.broker::<SqsBroker>()
        .published::<Order>("claims-dead")
        .assert_called_once()
        .with(&Order { id: 7 });

    tb.shutdown().await.expect("the app shuts down");
}

/// The same cap on the immediate path. SQS has no verb for "reject without deleting", so a
/// delivery that has run the policy out is returned rather than deleted, and the queue carries
/// it off on the receive that follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_immediate_retry_obeys_the_same_cap() {
    let app = RustStream::new(AppInfo::new("disputes", "0.1.0")).with_broker(broker(), |b| {
        b.include(arbitrate)
            .max_attempts(nonzero!(3u32))
            .dead_letter("disputes-dead");
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 11 })
        .to("disputes")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .subscriber("disputes")
        .assert_called(3);
    tb.broker::<SqsBroker>()
        .published::<Order>("disputes-dead")
        .assert_called_once()
        .with(&Order { id: 11 });

    tb.shutdown().await.expect("the app shuts down");
}

/// A handler reads the queue's own receive count, so it can tell a first attempt from a last
/// one. The in-process queue counts receives the way SQS does, so the reading is the same here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_reads_the_queues_receive_count() {
    let app = RustStream::new(AppInfo::new("attempts", "0.1.0")).with_broker(broker(), |b| {
        b.include(count_attempts)
            .max_attempts(nonzero!(3u32))
            .dead_letter("attempts-dead")
            .out(Attempts, Publish::default())
            .build();
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 3 })
        .to("attempts")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    let log = tb.broker::<SqsBroker>().published::<Attempt>("attempt-log");
    let attempts: Vec<Attempt> = log
        .messages()
        .iter()
        .map(|message| serde_json::from_slice(message.payload()).expect("an attempt decodes"))
        .collect();
    assert_eq!(
        attempts,
        [1, 2, 3].map(|receives| Attempt {
            id: 3,
            receives: Some(receives.to_string()),
        }),
        "the count follows the queue's own receives, starting at one",
    );

    tb.shutdown().await.expect("the app shuts down");
}

/// A bare name carries no descriptor, so the broker takes the declaration and writes it onto
/// the queue that name opens. The delivery then ends where the descriptor spelling ends it: in
/// the dead-letter queue, on the receive that runs the policy out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_declaration_reaches_the_queues_redrive_policy() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(reprice)
            .max_attempts(nonzero!(5u32))
            .dead_letter("orders-dead");
    });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsBroker>()
        .message(&Order { id: 5 })
        .to("orders")
        .publish()
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsBroker>()
        .subscriber("orders")
        .assert_called(5);
    tb.broker::<SqsBroker>()
        .published::<Order>("orders-dead")
        .assert_called_once()
        .with(&Order { id: 5 });

    tb.shutdown().await.expect("the app shuts down");
}

/// Half a policy is refused on this path too, and before the subscription opens: a bare name has
/// nowhere else to carry a cap, so the service would run with one the queue never received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_bare_name_declaration_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(reprice).max_attempts(nonzero!(2u32));
    });

    let refused = TestApp::start(app)
        .await
        .expect_err("a cap the queue never receives is not a cap");
    let reason = refused.to_string();
    assert!(
        reason.contains("dead_letter(..)"),
        "the refusal names the missing half, got {reason}",
    );
}

/// A redrive policy is one setting with two halves, so half a declaration is refused where the
/// service would otherwise run with a cap the queue never received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_declaration_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("claims", "0.1.0")).with_broker(broker(), |b| {
        b.include(audit.name("half")).max_attempts(nonzero!(2u32));
    });

    let refused = TestApp::start(app)
        .await
        .expect_err("a cap the queue never receives is not a cap");
    let reason = refused.to_string();
    assert!(
        reason.contains("max_attempts(..)") && reason.contains("dead_letter(..)"),
        "the refusal names both halves, got {reason}",
    );
}
