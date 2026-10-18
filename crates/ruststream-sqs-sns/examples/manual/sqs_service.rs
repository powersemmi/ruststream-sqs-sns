//! The service of `examples/sqs_service.rs` without the macros: the handler is a named type and
//! `main` is written by hand.
//!
//! ```text
//! just brokers-up
//! AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
//!     AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//!     cargo run --example manual_sqs_service --features testing -- run
//! cargo test --example manual_sqs_service --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the queue it is sent to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
/// Logs each order it receives.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order received");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
/// Reads the queue `orders`, each receive waiting up to 20 seconds for a message. A fresh local
/// stack has no queues, so the subscription creates the queue at startup.
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Receive)
                .build()
                .name("orders")
                .wait(Duration::from_secs(20))
                .create_if_missing(),
        );
    })
}
// --8<-- [end:app]

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_on_the_queue_is_received() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
