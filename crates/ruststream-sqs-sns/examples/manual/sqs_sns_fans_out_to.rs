//! The service of `examples/sqs_sns_fans_out_to.rs` written without the macros: the same policy
//! function, with the handlers as named types.
//!
//! ```text
//! cargo test --example manual_sqs_sns_fans_out_to --features testing,sns
//! cargo run --example manual_sqs_sns_fans_out_to --features testing,sns -- run
//! ```

use std::future::{Future, ready};
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

/// Reads `billing`, which the operator subscribed to `orders-events` in AWS.
struct Bill;

impl Handle<OrderPlaced> for Bill {
    fn handle(
        &self,
        order: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order billed");
        ready(Ok(()))
    }
}

// --8<-- [start:policy]
/// The topic's policy. The test build names the queue the operator subscribed.
fn announcements() -> SnsPublish {
    let policy = SnsPublish::default();
    #[cfg(test)]
    let policy = policy.fans_out_to(["billing"]);
    policy
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Accept).reply().build())
            .out_reply(announcements());
        b.include(subscriber("billing", Bill).build());
    })
}
// --8<-- [end:policy]

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
    async fn the_operators_queue_receives_the_announcement() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        tb.broker::<SqsBroker>()
            .message(&PlaceOrder { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("billing")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
