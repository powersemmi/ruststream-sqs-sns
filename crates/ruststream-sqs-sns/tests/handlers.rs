//! Handler-surface checks against the in-process transport.
//!
//! The forms this crate's documentation promises on a queue whose bodies are text: a batch
//! bounded by the size its mount site named, the byte lane a service reads that body through,
//! and both ways a reply reaches its destination.

#![cfg(feature = "testing")]

use std::sync::Mutex;

use ruststream::testing::TestApp;
use ruststream::{Outgoing, Serialized};
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;
use serde::{Deserialize, Serialize};

/// The payload as the queue hands it over: bytes, so the type names itself deserialized and no
/// codec sits on the path.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The wire these tests inject through: bytes they already hold, published as they are.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

static BATCHES: Mutex<Vec<Vec<Vec<u8>>>> = Mutex::new(Vec::new());

/// A batch handler. The size is the mount site's, and the subscription is opened to it: on the
/// real broker it becomes `MaxNumberOfMessages`, and in process the framework's buffer honours
/// the same bound.
#[subscriber]
async fn drain(frames: &[Frame<'_>]) -> HandlerOutcome {
    BATCHES
        .lock()
        .expect("batch log")
        .push(frames.iter().map(|frame| frame.0.to_vec()).collect());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_opens_its_subscription_at_the_size_it_named() {
    let broker = SqsTestBroker::new();
    // A producer handle taken before the app is built: the harness's own injection drives each
    // publish to a standstill, which would close a batch per message and say nothing about the
    // size bound this test is here for.
    let producer = broker.publisher();
    let app = RustStream::new(AppInfo::new("batches", "0.1.0")).with_broker(broker, |b| {
        b.include(drain.name("orders").batch(nonzero!(2)));
    });

    let tb = TestApp::start(app).await.expect("the app starts");
    for body in [b"first".as_slice(), b"second".as_slice()] {
        producer
            .message(&Wire(body.to_vec()))
            .to("orders")
            .publish()
            .await
            .expect("publish succeeds");
    }
    tb.settle().await.expect("the batch settles");

    tb.broker::<SqsTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .assert_batch_sizes(&[2])
        .settled(HandlerOutcome::ack());
    assert_eq!(
        BATCHES.lock().expect("batch log").as_slice(),
        &[vec![b"first".to_vec(), b"second".to_vec()]],
        "one batch closed at the size the mount named, and the bytes crossed untouched",
    );
}

/// The request both replying handlers answer. It declares no destination, so each test names the
/// queue it injects into.
#[derive(Serialize, Deserialize, Outgoing)]
struct Request {
    id: u64,
}

/// A reply type that fixes its destination: on the real broker this name is an SNS topic under
/// `SnsPublish` and a queue under the default policy, and neither reading depends on the mount
/// site.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders-events")]
struct OrderPlaced {
    id: u64,
}

/// A reply type that declares no destination: the mount site owns the name.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[subscriber("orders", publish)]
async fn announce(request: &Request) -> OrderPlaced {
    OrderPlaced { id: request.id }
}

#[subscriber("receipt-requests", publish("receipts"))]
async fn issue(request: &Request) -> Receipt {
    Receipt { id: request.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_declares_a_destination_lands_on_it() {
    let app = RustStream::new(AppInfo::new("declared-reply", "0.1.0")).with_broker(
        SqsTestBroker::new(),
        |b| {
            b.include(announce);
        },
    );

    let tb = TestApp::start(app).await.expect("the app starts");
    tb.broker::<SqsTestBroker>()
        .message(&Request { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("the request reaches the queue");
    tb.settle().await.expect("the reply settles");

    tb.broker::<SqsTestBroker>()
        .subscriber("orders")
        .assert_called_once();
    tb.broker::<SqsTestBroker>()
        .published::<OrderPlaced>("orders-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 1 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_without_a_destination_lands_on_the_mount_site_name() {
    let app = RustStream::new(AppInfo::new("mounted-reply", "0.1.0")).with_broker(
        SqsTestBroker::new(),
        |b| {
            b.include(issue);
        },
    );

    let tb = TestApp::start(app).await.expect("the app starts");
    tb.broker::<SqsTestBroker>()
        .message(&Request { id: 2 })
        .to("receipt-requests")
        .publish()
        .await
        .expect("the request reaches the queue");
    tb.settle().await.expect("the reply settles");

    tb.broker::<SqsTestBroker>()
        .subscriber("receipt-requests")
        .assert_called_once();
    tb.broker::<SqsTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 2 });
}
