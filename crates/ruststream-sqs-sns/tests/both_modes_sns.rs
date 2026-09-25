//! The same SNS test bodies, run on the production app in process and against a local stack.
//!
//! Each body takes the harness it runs under, so only the start call differs: `TestApp::start`
//! connects `SqsBroker` in process, `TestApp::start_live` connects it to the stack behind
//! `SQS_TEST_ENDPOINT`.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

#![cfg(all(feature = "testing", feature = "sns"))]

use std::error::Error;
use std::io;

use ruststream::testing::TestApp;
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
/// Subscribed to the topic by the service itself.
const AUDIT: &str = "both-modes-parcels-audit";
/// Subscribed to the topic outside the service, the way an operator provisions it.
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

/// Subscribes the audit queue to the topic, the one subscription the service makes itself.
async fn wire_parcels(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    connected
        .subscribe_queue_to_topic(PARCELS, AUDIT)
        .await
        .map_err(io::Error::other)
}

/// The announcement position names the queue the operator subscribed to the topic, so a test
/// knows the whole group the topic fans out to.
fn announcements() -> SnsPublish {
    SnsPublish::default().fans_out_to([NOTIFY])
}

fn parcels() -> RustStream {
    let broker = broker();
    let topology = broker.clone();
    RustStream::new(AppInfo::new("parcels", "0.1.0"))
        .after_startup(async move |_state| wire_parcels(topology).await)
        .with_broker(broker, |b| {
            b.include(receive.name(PARCELS).create_if_missing())
                .out_reply(announcements());
            b.include(audit.name(AUDIT).create_if_missing());
            b.include(notify.name(NOTIFY).create_if_missing());
        })
}

/// A publish goes where its publisher sends it, whatever else carries the name: the parcel sent
/// to the queue reaches the queue's handler alone, and the announcement published to the topic of
/// the same name reaches every queue subscribed to the topic, the one the service subscribed and
/// the one the operator did.
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

/// What the operator provisions outside the service: the notify queue, subscribed to the topic.
/// A broker of its own does it, so the service's broker learns of it only from the policy.
async fn provision_outside_the_service(endpoint: &str) -> Result<(), Box<dyn Error>> {
    live::admin(endpoint)
        .await
        .create_queue()
        .queue_name(NOTIFY)
        .send()
        .await?;
    live::connect(endpoint)
        .await
        .subscribe_queue_to_topic(PARCELS, NOTIFY)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_a_queue_and_a_topic_of_one_name_reach_their_own_handlers()
-> Result<(), Box<dyn Error>> {
    let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
        return Ok(());
    };
    provision_outside_the_service(&endpoint).await?;
    a_queue_and_a_topic_of_one_name_reach_their_own_handlers(TestApp::start_live(parcels()).await?)
        .await
}
