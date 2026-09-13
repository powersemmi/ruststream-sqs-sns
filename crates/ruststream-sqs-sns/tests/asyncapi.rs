//! What this broker contributes to the generated `AsyncAPI` document.
//!
//! The excerpt the documentation shows is this test's own expectation, so a reader sees what a
//! service really publishes and a drift in either one fails here.

#![cfg(feature = "asyncapi")]

use std::time::Duration;

use ruststream::DescribeServer;
use ruststream::asyncapi::build_spec;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The excerpt `docs/sqs.md` and its translations include.
const CHANNEL_BINDING: &str = include_str!("asyncapi_channel.json");

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(20))
        .visibility(Duration::from_secs(30))
)]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A queue named for FIFO, and with no visibility timeout of its own.
#[subscriber(SqsQueue::new("shipments.fifo"))]
async fn ship(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The document a service on this broker publishes.
fn document() -> Value {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(SqsBroker::new(), |b| {
        b.include(reconcile)
            .max_attempts(nonzero!(5u32))
            .dead_letter("orders-dead");
        b.include(ship);
    });
    let json = build_spec(&app).to_json().expect("the document serializes");
    serde_json::from_str(&json).expect("the document is JSON")
}

/// A queue's own vocabulary reaches the document through the `sqs` channel binding: its name,
/// whether it is FIFO, and the polling settings the descriptor names.
#[test]
fn a_queue_describes_itself_with_the_sqs_channel_binding() {
    let expected: Value = serde_json::from_str(CHANNEL_BINDING).expect("the excerpt is JSON");
    assert_eq!(
        document()["channels"]["orders"]["bindings"],
        expected,
        "the document and the excerpt the documentation shows have drifted apart",
    );
}

/// The `.fifo` suffix is what makes a queue FIFO on SQS, so the document says so without being
/// told. A descriptor that names no visibility timeout leaves the field out rather than
/// inventing one: the queue's own setting needs a connection to read.
#[test]
fn a_fifo_queue_says_so_and_names_no_timeout_it_was_not_given() {
    let binding = &document()["channels"]["shipments.fifo"]["bindings"]["sqs"];
    assert_eq!(binding["queue"]["fifoQueue"], true);
    assert!(
        binding["queue"].get("visibilityTimeout").is_none(),
        "a timeout the descriptor never named reached the document: {binding}",
    );
}

/// The server coordinate is where clients connect, and nothing else. SNS publishes share it:
/// a broker describes one server, and this crate speaks two protocols through it.
#[test]
fn the_document_names_the_sqs_service() {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
        .server("aws", SqsBroker::new().describe_server())
        .with_broker(SqsBroker::new(), |b| {
            b.include(reconcile);
        });
    let json = build_spec(&app).to_json().expect("the document serializes");
    let value: Value = serde_json::from_str(&json).expect("the document is JSON");

    assert_eq!(value["servers"]["aws"]["protocol"], "sqs");
    assert_eq!(value["servers"]["aws"]["host"], "sqs.amazonaws.com");
}

/// A reply on SQS goes to the queue the declaration names, so the document reports that name
/// rather than a runtime expression a client would have to read a header for.
#[test]
fn a_reply_channel_carries_its_address() {
    #[derive(Debug, Deserialize, Serialize, Outgoing)]
    struct Confirmed {
        id: u64,
    }

    #[subscriber("payments", publish("payments-confirmed"))]
    async fn confirm(order: &Order) -> Confirmed {
        Confirmed { id: order.id }
    }

    let app =
        RustStream::new(AppInfo::new("payments", "1.0.0")).with_broker(SqsBroker::new(), |b| {
            b.include(confirm);
        });
    let json = build_spec(&app).to_json().expect("the document serializes");
    let value: Value = serde_json::from_str(&json).expect("the document is JSON");

    assert_eq!(
        value["channels"]["payments-confirmed"]["address"],
        "payments-confirmed",
    );
    assert!(
        !json.contains("$message.header"),
        "SQS routes no reply through a header, so the document names no reply address: {json}",
    );
}
