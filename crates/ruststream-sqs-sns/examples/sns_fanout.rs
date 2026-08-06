//! SNS fan-out: publish once to a topic, deliver to every subscribed queue.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sns_fanout`

use std::io;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher, subscriber};
use ruststream_sqs_sns::{SnsPublish, SqsBroker, SqsQueue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue::new("billing").create_if_missing())]
async fn bill(order: &OrderPlaced) -> HandlerResult {
    println!("billing got order {}", order.id);
    HandlerResult::Ack
}

#[subscriber(SqsQueue::new("shipping").create_if_missing())]
async fn ship(order: &OrderPlaced) -> HandlerResult {
    println!("shipping got order {}", order.id);
    HandlerResult::Ack
}

fn broker() -> SqsBroker {
    SqsBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1")
}

/// Wires both queues to the topic with raw delivery, so payloads and headers arrive unwrapped.
///
/// Stays on the broker's own lifecycle ladder: topology administration is provisioning work
/// (terraform or the console in production), and the app builder has no vocabulary for it. The
/// queues themselves already exist by now - the subscriptions opened them.
// --8<-- [start:wiring]
async fn wire_topology() -> io::Result<()> {
    let connected = broker().connect().await.map_err(io::Error::other)?;
    for queue in ["billing", "shipping"] {
        connected
            .subscribe_queue_to_topic("orders-events", queue)
            .await
            .map_err(io::Error::other)?;
    }
    connected.shutdown().await.map_err(io::Error::other)
}
// --8<-- [end:wiring]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("sns-fanout", "0.1.0"))
        // Registration order is run order across both hook levels, so the wiring lands before
        // the first notification is published.
        .after_startup(async move |_state| wire_topology().await)
        .with_broker(broker(), |b| {
            b.include(bill);
            b.include(ship);
            b.after_startup(SnsPublish, async move |sns| -> io::Result<()> {
                let payload = JsonCodec
                    .encode(&OrderPlaced { id: 1 })
                    .map_err(io::Error::other)?;
                sns.publish(OutgoingMessage::new("orders-events", payload.as_ref()))
                    .await
                    .map_err(io::Error::other)
            });
        })
}
// --8<-- [end:app]
