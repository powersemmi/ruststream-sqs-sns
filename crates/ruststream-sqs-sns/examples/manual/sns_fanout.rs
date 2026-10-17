//! The service of `examples/sns_fanout.rs` written without the macros: the handlers implement
//! `Handle`, the descriptors join the chain through `map_source`, and the reply position takes
//! `SnsPublish`.
//!
//! ```text
//! cargo test --example manual_sns_fanout --features testing,sns
//! cargo run --example manual_sns_fanout --features testing,sns -- run
//! ```

use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: u64,
}

// --8<-- [start:reply]
/// The notification the topic fans out. It names the topic, so the reply takes no destination
/// at the mount site.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "orders-events")]
struct OrderPlaced {
    id: u64,
}

struct Accept;

impl Handle<PlaceOrder, OrderPlaced> for Accept {
    fn handle(
        &self,
        order: &PlaceOrder,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderPlaced, HandlerOutcome>> {
        tracing::info!(order.id, "order accepted");
        ready(Ok(OrderPlaced { id: order.id }))
    }
}
// --8<-- [end:reply]

/// A handler of the announcement; billing and shipping each mount one on their own queue.
struct Record(&'static str);

impl Handle<OrderPlaced> for Record {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, queue = self.0, "order recorded");
        ready(Ok(()))
    }
}

// --8<-- [start:wiring]
/// Subscribes both queues to the topic. The subscriptions have opened the queues by the time a
/// startup hook runs.
async fn wire_topology(broker: SqsBroker) -> io::Result<()> {
    let connected = broker.connect().await.map_err(io::Error::other)?;
    for queue in ["billing", "shipping"] {
        connected
            .subscribe_queue_to_topic("orders-events", queue)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}
// --8<-- [end:wiring]

// --8<-- [start:app]
fn service(broker: SqsBroker) -> impl App {
    // A clone connects through the connection the app's broker holds.
    let topology = broker.clone();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .after_startup(async move |_state| wire_topology(topology).await)
        .with_broker(broker, |b| {
            let accept = subscriber("orders", Accept)
                .map_source(|_| SqsQueue::new("orders").create_if_missing());
            // One step moves the reply from the queue of that name to the topic.
            b.include(accept.reply().build())
                .out_reply(SnsPublish::default());
            for queue in ["billing", "shipping"] {
                let record = subscriber(queue, Record(queue))
                    .map_source(move |_| SqsQueue::new(queue).create_if_missing());
                b.include(record.build());
            }
        })
}
// --8<-- [end:app]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{OrderPlaced, PlaceOrder, broker, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_reaches_every_subscribed_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.broker::<SqsBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        for queue in ["billing", "shipping"] {
            tb.broker::<SqsBroker>()
                .subscriber(queue)
                .assert_called_once()
                .with(&OrderPlaced { id: 7 })
                .settled(HandlerOutcome::ack());
        }
        tb.broker::<SqsBroker>()
            .published::<OrderPlaced>("orders-events")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
