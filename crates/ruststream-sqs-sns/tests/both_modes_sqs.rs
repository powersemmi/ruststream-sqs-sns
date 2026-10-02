//! The same test bodies, run on the production app in process and against a local stack.
//!
//! Each body takes the harness it runs under, so only the start call differs: `TestApp::start`
//! connects `SqsBroker` in process, `TestApp::start_live` connects it to the stack behind
//! `SQS_TEST_ENDPOINT`. The in-process leg runs on a paused clock, the live leg on the running
//! one, where a delay is the queue's own timer.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

#![cfg(feature = "testing")]

use std::error::Error;
use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

mod live;

/// The endpoint the service is built with. In process it only shapes the queue URLs; live it is
/// the stack.
fn endpoint() -> String {
    std::env::var("SQS_TEST_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:4566".to_owned())
}

/// The broker the service's `main` builds, for a local stack.
fn broker() -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint())
        .test_credentials()
        .region("us-east-1")
}

// --- A delayed redelivery ------------------------------------------------------------------

/// How long an invoice waits before it comes back.
const DELAY: Duration = Duration::from_secs(2);

const INVOICES: &str = "both-modes-invoices";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Invoice {
    id: u64,
}

/// The first receive asks for the invoice again after [`DELAY`]; the second takes it.
#[subscriber(SqsQueue)]
async fn settle(invoice: &Invoice, cx: &mut Context<'_>) -> HandlerOutcome {
    let _ = invoice.id;
    if cx.headers().get(RECEIVE_COUNT_HEADER) == Some(b"1".as_slice()) {
        return HandlerOutcome::retry_after(DELAY);
    }
    HandlerOutcome::ack()
}

fn invoices() -> RustStream {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker(), |b| {
        b.include(settle.name(INVOICES).create_if_missing());
    })
}

/// `retry_after` is the queue's own visibility timeout: the invoice comes back once the delay has
/// passed, with its receive count moved on, and nothing is republished to get it there.
async fn an_invoice_comes_back_after_the_delay(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
    tb.broker::<SqsBroker>()
        .message(&Invoice { id: 1 })
        .to(INVOICES)
        .publish()
        .await?;
    tb.broker::<SqsBroker>()
        .subscriber(INVOICES)
        .assert_called_once()
        .with(&Invoice { id: 1 })
        .settled(HandlerOutcome::retry_after(DELAY));

    tb.advance(DELAY).await?;

    tb.broker::<SqsBroker>()
        .subscriber(INVOICES)
        .assert_called(2)
        .with(&Invoice { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<SqsBroker>()
        .published::<Invoice>(INVOICES)
        .assert_called_once();
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn in_process_an_invoice_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
    an_invoice_comes_back_after_the_delay(TestApp::start(invoices()).await?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_an_invoice_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
    if live::endpoint("SQS_TEST_ENDPOINT").is_none() {
        return Ok(());
    }
    an_invoice_comes_back_after_the_delay(TestApp::start_live(invoices()).await?).await
}
