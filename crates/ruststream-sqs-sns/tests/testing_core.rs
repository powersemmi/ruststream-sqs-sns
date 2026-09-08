//! The declaration a service ships, mounted on the in-process transport.
//!
//! What a production routes file writes is `#[subscriber(SqsQueue::new(..))]` plus whatever the
//! mount site chains onto it, and that exact wiring has to start, receive and settle under the
//! harness - otherwise the thing under test is a rewrite of the service rather than the service.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;
use serde::{Deserialize, Serialize};

/// The payload both handlers below take, and the producer publishes: a decoded type, so the
/// default codec sits on the path the way a service's does.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
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
