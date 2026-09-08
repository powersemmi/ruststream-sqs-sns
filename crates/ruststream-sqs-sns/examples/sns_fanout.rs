//! SNS fan-out: one order accepted on a queue, announced once to a topic, delivered to every
//! subscribed queue.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sns_fanout`

use std::io;

use ruststream::ConnectedBroker;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The order as it arrives on the service's own queue. It declares that queue, so the publish
/// call site names no destination and the generated document carries the channel.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: u64,
}

/// The notification the topic fans out.
#[derive(Debug, Deserialize, Serialize)]
struct OrderPlaced {
    id: u64,
}

// --8<-- [start:reply]
/// The handler names where its reply goes; the mount site names who takes it there. Without an
/// `.out(Reply, ..)` the reply would ride the broker's default policy and land on a queue of that
/// name, so fan-out is one step on the mount chain rather than a different handler.
#[subscriber(SqsQueue::new("orders").create_if_missing(), publish("orders-events"))]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    println!("accepted order {}", order.id);
    OrderPlaced { id: order.id }
}
// --8<-- [end:reply]

#[subscriber(SqsQueue::new("billing").create_if_missing())]
async fn bill(order: &OrderPlaced) -> HandlerOutcome {
    println!("billing got order {}", order.id);
    HandlerOutcome::ack()
}

#[subscriber(SqsQueue::new("shipping").create_if_missing())]
async fn ship(order: &OrderPlaced) -> HandlerOutcome {
    println!("shipping got order {}", order.id);
    HandlerOutcome::ack()
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
#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("sns-fanout", "0.1.0"))
        // Registration order is run order across both hook levels, so the wiring lands before
        // the first order is placed.
        .after_startup(async move |_state| wire_topology().await)
        .with_broker(broker(), |b| {
            // One verb binds the reply position: the marker names which publish the policy is
            // for, and `SnsPublish` names the fan-out.
            b.include(accept).out(Reply, SnsPublish);
            b.include(bill);
            b.include(ship);
            b.after_startup(Publish, async move |sqs| -> io::Result<()> {
                sqs.message(&PlaceOrder { id: 1 })
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        })
}
// --8<-- [end:app]
