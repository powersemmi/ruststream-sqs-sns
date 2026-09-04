//! Handler-surface checks against the in-process transport.
//!
//! The two forms this crate's documentation promises on a queue whose bodies are text: a page
//! bounded by the size its mount site named, and the byte lane a service reads that body
//! through.

#![cfg(feature = "testing")]

use std::sync::Mutex;

use ruststream::testing::TestApp;
use ruststream::{Outgoing, Serialized};
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;

/// The payload as the queue hands it over: bytes, so the type names itself deserialized and no
/// codec sits on the path.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The wire these tests inject through: bytes they already hold, published as they are.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

static PAGES: Mutex<Vec<Vec<Vec<u8>>>> = Mutex::new(Vec::new());

/// A page handler. The size is the mount site's, and the subscription is opened to it: on the
/// real broker it becomes `MaxNumberOfMessages`, and in process the framework's buffer honours
/// the same bound.
#[subscriber]
async fn drain(frames: &[Frame<'_>]) -> HandlerOutcome {
    PAGES
        .lock()
        .expect("page log")
        .push(frames.iter().map(|frame| frame.0.to_vec()).collect());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_handler_opens_its_subscription_at_the_size_it_named() {
    let broker = SqsTestBroker::new();
    // A producer handle taken before the app is built: the harness's own injection drives each
    // publish to a standstill, which would close a page per message and say nothing about the
    // size bound this test is here for.
    let producer = broker.publisher();
    let app = RustStream::new(AppInfo::new("pages", "0.1.0")).with_broker(broker, |b| {
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
    tb.settle().await.expect("the page settles");

    tb.broker::<SqsTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .assert_page_sizes(&[2])
        .settled(HandlerOutcome::ack());
    assert_eq!(
        PAGES.lock().expect("page log").as_slice(),
        &[vec![b"first".to_vec(), b"second".to_vec()]],
        "one page closed at the size the mount named, and the bytes crossed untouched",
    );
}
