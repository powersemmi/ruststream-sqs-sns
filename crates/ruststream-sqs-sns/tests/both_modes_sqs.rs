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
use std::io;
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

// --- SNS fan-out ---------------------------------------------------------------------------

const ORDERS: &str = "both-modes-orders";
const EVENTS: &str = "both-modes-order-events";
const BILLING: &str = "both-modes-billing";
const SHIPPING: &str = "both-modes-shipping";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct PlaceOrder {
    id: u64,
}

/// The notification the topic fans out; it names the topic.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "both-modes-order-events")]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue, publish)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

#[subscriber(SqsQueue)]
async fn bill(order: &OrderPlaced) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue)]
async fn ship(order: &OrderPlaced) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Subscribes both queues to the topic. The broker is a clone of the app's, so it connects to the
/// connection the app's broker already holds: the stack live, the in-process account in a test.
async fn wire_topology(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    for queue in [BILLING, SHIPPING] {
        connected
            .subscribe_queue_to_topic(EVENTS, queue)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn fan_out() -> RustStream {
    let broker = broker();
    let topology = broker.clone();
    RustStream::new(AppInfo::new("fan-out", "0.1.0"))
        .after_startup(async move |_state| wire_topology(topology).await)
        .with_broker(broker, |b| {
            b.include(accept.name(ORDERS).create_if_missing())
                .out_reply(SnsPublish::default());
            b.include(bill.name(BILLING).create_if_missing());
            b.include(ship.name(SHIPPING).create_if_missing());
        })
}

/// The reply fans out from the topic: every queue subscribed to it receives a copy, and the one
/// publish reaches both handlers.
async fn a_reply_fans_out_to_every_subscribed_queue(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
    tb.broker::<SqsBroker>()
        .message(&PlaceOrder { id: 7 })
        .to(ORDERS)
        .publish()
        .await?;

    for queue in [BILLING, SHIPPING] {
        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&OrderPlaced { id: 7 })
            .settled(HandlerOutcome::ack());
    }
    tb.broker::<SqsBroker>()
        .published::<OrderPlaced>(EVENTS)
        .assert_called_once()
        .with(&OrderPlaced { id: 7 });
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_a_reply_fans_out_to_every_subscribed_queue() -> Result<(), Box<dyn Error>> {
    a_reply_fans_out_to_every_subscribed_queue(TestApp::start(fan_out()).await?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_reply_fans_out_to_every_subscribed_queue() -> Result<(), Box<dyn Error>> {
    if live::endpoint("SQS_TEST_ENDPOINT").is_none() {
        return Ok(());
    }
    a_reply_fans_out_to_every_subscribed_queue(TestApp::start_live(fan_out()).await?).await
}

// --- A topic and a queue under one name ----------------------------------------------------

/// The name a queue and a topic share: a parcel arrives on the queue, and its announcement fans
/// out from the topic.
const PARCELS: &str = "both-modes-parcels";
const AUDIT: &str = "both-modes-parcels-audit";
const NOTIFY: &str = "both-modes-parcels-notify";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Parcel {
    id: u64,
}

/// The announcement names the topic, which carries the queue's name.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "both-modes-parcels")]
struct ParcelAnnounced {
    parcel: u64,
}

#[subscriber(SqsQueue, publish)]
async fn receive(parcel: &Parcel) -> ParcelAnnounced {
    ParcelAnnounced { parcel: parcel.id }
}

#[subscriber(SqsQueue)]
async fn audit(announced: &ParcelAnnounced) -> HandlerOutcome {
    let _ = announced.parcel;
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue)]
async fn notify(announced: &ParcelAnnounced) -> HandlerOutcome {
    let _ = announced.parcel;
    HandlerOutcome::ack()
}

/// Subscribes the audit and notify queues to the topic that shares the parcels queue's name.
async fn wire_parcels(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    for queue in [AUDIT, NOTIFY] {
        connected
            .subscribe_queue_to_topic(PARCELS, queue)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn parcels() -> RustStream {
    let broker = broker();
    let topology = broker.clone();
    RustStream::new(AppInfo::new("parcels", "0.1.0"))
        .after_startup(async move |_state| wire_parcels(topology).await)
        .with_broker(broker, |b| {
            b.include(receive.name(PARCELS).create_if_missing())
                .out_reply(SnsPublish::default());
            b.include(audit.name(AUDIT).create_if_missing());
            b.include(notify.name(NOTIFY).create_if_missing());
        })
}

/// A publish goes where its publisher sends it, whatever else carries the name: the parcel sent to
/// the queue reaches the queue's handler alone, and the announcement published to the topic of
/// the same name reaches the two queues subscribed to it.
async fn a_queue_and_a_topic_of_one_name_reach_their_own_handlers(
    tb: TestApp<()>,
) -> Result<(), Box<dyn Error>> {
    tb.broker::<SqsBroker>()
        .message(&Parcel { id: 3 })
        .to(PARCELS)
        .publish()
        .await?;

    tb.broker::<SqsBroker>()
        .subscriber(PARCELS)
        .assert_called_once()
        .with(&Parcel { id: 3 })
        .settled(HandlerOutcome::ack());
    for queue in [AUDIT, NOTIFY] {
        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&ParcelAnnounced { parcel: 3 })
            .settled(HandlerOutcome::ack());
    }
    tb.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_a_queue_and_a_topic_of_one_name_reach_their_own_handlers()
-> Result<(), Box<dyn Error>> {
    a_queue_and_a_topic_of_one_name_reach_their_own_handlers(TestApp::start(parcels()).await?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_queue_and_a_topic_of_one_name_reach_their_own_handlers()
-> Result<(), Box<dyn Error>> {
    if live::endpoint("SQS_TEST_ENDPOINT").is_none() {
        return Ok(());
    }
    a_queue_and_a_topic_of_one_name_reach_their_own_handlers(TestApp::start_live(parcels()).await?)
        .await
}
