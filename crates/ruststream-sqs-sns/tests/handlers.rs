//! Handler-surface checks against the in-process transport.
//!
//! The two forms this crate's documentation promises on a queue that neither batches natively
//! nor carries a structured body: a page assembled by the framework's own buffer, and the byte
//! lane a service reads a text body through.

#![cfg(feature = "testing")]

use std::sync::Mutex;
use std::time::Duration;

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

/// A page handler. SQS delivers and settles one message at a time, so the page comes from the
/// framework's buffer named at the mount site rather than from the transport.
#[subscriber]
async fn drain(frames: &[Frame<'_>]) -> HandlerOutcome {
    PAGES
        .lock()
        .expect("page log")
        .push(frames.iter().map(|frame| frame.0.to_vec()).collect());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_handler_mounts_over_the_frameworks_buffer() {
    let broker = SqsTestBroker::new();
    // A producer handle taken before the app is built: the harness's own injection drives each
    // publish to a standstill, which would close a page per message and say nothing about the
    // size bound this test is here for.
    let producer = broker.publisher();
    let app = RustStream::new(AppInfo::new("pages", "0.1.0")).with_broker(broker, |b| {
        b.include(
            drain
                .name("orders")
                .buffered(nonzero!(2), Duration::from_millis(500)),
        );
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
        .settled(HandlerOutcome::ack());
    assert_eq!(
        PAGES.lock().expect("page log").as_slice(),
        &[vec![b"first".to_vec(), b"second".to_vec()]],
        "the buffer closed one page at its size limit, and the bytes crossed untouched",
    );
}
