//! What this broker contributes to the generated `AsyncAPI` document.
//!
//! The excerpt the documentation shows is this test's own expectation, so a reader sees what a
//! service really publishes and a drift in either one fails here.

#![cfg(feature = "asyncapi")]

use std::time::Duration;

use ruststream::asyncapi::build_spec;
use ruststream::{DescribeServer, PublishPolicy};
use ruststream_sqs_sns::ConnectedSqsBroker;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The excerpt the crate overview reproduces under "The generated document".
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

/// A reply that leaves its destination open, so the clause at each mount site names one.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct OrderPlaced {
    id: u64,
}

/// The reply goes to a queue, so the document describes a queue.
#[subscriber(SqsQueue::new("accepted"), publish("order-events"))]
async fn accept(order: &Order) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

/// The same reply, fanned out from a FIFO topic.
#[subscriber(SqsQueue::new("announced"), publish("order-events.fifo"))]
async fn announce(order: &Order) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

/// The same reply again, fanned out from a standard topic.
#[subscriber(SqsQueue::new("notified"), publish("shipment-events"))]
async fn notify(order: &Order) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

/// The document of a service whose replies leave through all three positions.
fn publish_document() -> Value {
    let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(SqsBroker::new(), |b| {
        b.include(accept).out_reply(Publish::default());
        b.include(announce).out_reply(SnsPublish::default());
        b.include(notify).out_reply(SnsPublish::default());
    });
    let json = build_spec(&app).to_json().expect("the document serializes");
    serde_json::from_str(&json).expect("the document is JSON")
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
    // The protocol name already says everything: SQS has one wire protocol and a client matches
    // no version of it, so a version field would be an invention.
    assert!(
        value["servers"]["aws"].get("protocolVersion").is_none(),
        "the server named a protocol version SQS does not have: {}",
        value["servers"]["aws"],
    );
}

/// A reply on SQS goes to the queue the declaration names, not through a reply-to header, so
/// neither policy answers a reply address and the document reports the name it was given.
#[test]
fn no_policy_of_this_crate_answers_a_reply_address() {
    fn answered<Policy: PublishPolicy<ConnectedSqsBroker>>(
        policy: &Policy,
    ) -> Option<&'static str> {
        policy.reply_address_location()
    }

    assert_eq!(answered(&SqsPublish::default()), None);
    assert_eq!(answered(&SnsPublish::default()), None);
}

/// A policy declares this broker's settings and never a destination, so the queue a reply
/// reaches is the name the mount site resolved, and the binding carries it.
#[test]
fn a_reply_names_its_queue_in_the_sqs_channel_binding() {
    let binding = &publish_document()["channels"]["order-events"]["bindings"]["sqs"];
    assert_eq!(binding["bindingVersion"], "0.3.0");
    assert_eq!(binding["queue"]["name"], "order-events");
    assert_eq!(binding["queue"]["fifoQueue"], false);
}

/// Fan-out reports the same destination as a topic, and the `.fifo` suffix is what puts an
/// order on it - the same rule the subscription side reads off a queue name.
#[test]
fn a_fan_out_reply_names_its_topic_in_the_sns_channel_binding() {
    let binding = &publish_document()["channels"]["order-events.fifo"]["bindings"]["sns"];
    assert_eq!(binding["bindingVersion"], "1.0.0");
    assert_eq!(binding["name"], "order-events.fifo");
    assert_eq!(binding["ordering"]["type"], "FIFO");
}

/// A standard topic has no order to report, and the specification reads an absent ordering
/// object as exactly that, so the document says nothing rather than inventing a setting.
#[test]
fn a_standard_topic_reports_no_ordering() {
    let binding = &publish_document()["channels"]["shipment-events"]["bindings"]["sns"];
    assert_eq!(binding["name"], "shipment-events");
    assert!(
        binding.get("ordering").is_none(),
        "a standard topic was given an order it does not have: {binding}",
    );
}
