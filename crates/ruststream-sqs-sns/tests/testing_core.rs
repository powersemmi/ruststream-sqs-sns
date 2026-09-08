//! The declaration a service ships, mounted on the in-process transport.
//!
//! What a production routes file writes is `#[subscriber(SqsQueue::new(..))]`, whatever the mount
//! site chains onto it, and the publish policy the reply position is bound to. That exact wiring
//! has to start, receive, reply and settle under the harness - otherwise the thing under test is
//! a rewrite of the service rather than the service.

#![cfg(feature = "testing")]

use std::io;
use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqs_sns::PARTITION_KEY_HEADER;
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;
use serde::{Deserialize, Serialize};

/// The payload the handlers below take, and the producer publishes: a decoded type, so the
/// default codec sits on the path the way a service's does.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// What a reply-shaped handler hands back.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descriptor_declared_for_sqs_mounts_on_the_test_broker() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsTestBroker::new(), |b| {
            b.include(handle_order);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mount_site_settings_ride_that_descriptor_in_process() {
    let broker = SqsTestBroker::new();
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

    tb.broker::<SqsTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_takes_the_brokers_default_policy_in_process() {
    let app =
        RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(SqsTestBroker::new(), |b| {
            // No `.out(..)`: the reply rides whatever the connected broker names as its default
            // policy, and in process that has to be the production one.
            b.include(accept);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsTestBroker>()
        .publish("accepted", &Order { id: 1 })
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsTestBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 1 });

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_publish_policy_mounts_on_the_test_broker() {
    let app =
        RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(SqsTestBroker::new(), |b| {
            // The line a routes file writes, unchanged: the broker under it is the only
            // difference between this and production.
            b.include(accept).out(Reply, Publish);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsTestBroker>()
        .publish("accepted", &Order { id: 2 })
        .await
        .expect("the publish drives the handler to a standstill");

    tb.broker::<SqsTestBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 2 });

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fan_out_policy_mounts_the_same_way() {
    let app =
        RustStream::new(AppInfo::new("accepted", "0.1.0")).with_broker(SqsTestBroker::new(), |b| {
            b.include(accept).out(Reply, SnsPublish);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsTestBroker>()
        .publish("accepted", &Order { id: 3 })
        .await
        .expect("the publish drives the handler to a standstill");

    // The reply reached the destination the fan-out policy names. Onward delivery to the queues
    // subscribed to that topic is SNS's own work and belongs to the live suite.
    tb.broker::<SqsTestBroker>()
        .published::<OrderPlaced>("order-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 3 });

    tb.shutdown().await.expect("the app shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_startup_hook_publishes_under_the_group_it_named() {
    let app = RustStream::new(AppInfo::new("shipments", "0.1.0")).with_broker(
        SqsTestBroker::new(),
        |b| {
            b.include(ship);
            // The hook takes the live form of the production policy, so what it may call on that
            // publisher is what decides whether the service's own startup code compiles here.
            b.after_startup(Publish, async move |sqs| -> io::Result<()> {
                sqs.with_group_id("user-42")
                    .message(&Order { id: 4 })
                    .to("shipments.fifo")
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        },
    );

    let tb = TestApp::start(app).await.expect("the app starts");
    tb.settle().await.expect("the startup publish settles");

    tb.broker::<SqsTestBroker>()
        .subscriber("shipments.fifo")
        .assert_called_once()
        .with(&Order { id: 4 })
        .settled(HandlerOutcome::ack());
    // The group is not dropped on the way through: it travels as the base partition key, the
    // same header the real publishers turn into the FIFO message group id.
    tb.broker::<SqsTestBroker>()
        .published::<Order>("shipments.fifo")
        .assert_called_once()
        .with_header(PARTITION_KEY_HEADER, "user-42");

    tb.shutdown().await.expect("the app shuts down");
}
