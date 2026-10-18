//! The service of `examples/sqs_broker_prelude.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_broker_prelude --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_sqs_sns::prelude::*;

struct Accept;

impl Handle<Order, OrderAccepted> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderAccepted, HandlerOutcome>> {
        tracing::info!(order.id, "order accepted");
        ready(Ok(OrderAccepted { id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Accept)
                .reply()
                .build()
                .name("orders")
                .wait(Duration::from_secs(20)),
        )
        .out_reply(Publish::default());
    })
}
// --8<-- [end:app]

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderAccepted {
    id: u64,
}

// `#[outgoing(name = "orders-accepted")]` by hand: the reply names the queue it is sent to.
impl OutgoingDestination for OrderAccepted {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders-accepted";
}

impl MessageHeaders for OrderAccepted {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, OrderAccepted, app};

    #[tokio::test]
    async fn an_accepted_order_is_sent_to_its_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .published::<OrderAccepted>("orders-accepted")
            .assert_called_once()
            .with(&OrderAccepted { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
