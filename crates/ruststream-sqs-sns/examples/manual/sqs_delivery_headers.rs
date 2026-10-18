//! The service of `examples/sqs_delivery_headers.rs` without the macros: the handler is a named
//! type that takes the payload and its header contract together, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_sqs_delivery_headers --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

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

// `#[outgoing(name = "orders", headers = OrderMeta)]` by hand: an order names its queue and the
// headers that travel with it.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = WithHeaders<OrderMeta>;
}

// --8<-- [start:handler]
/// The headers an order carries: two message attributes on the queue.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderMeta {
    tenant: String,
    trace: Option<String>,
}

struct Bill;

impl Handle<Message<OrderMeta, Order>> for Bill {
    fn handle(
        &self,
        msg: &Message<OrderMeta, Order>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let (order, meta) = (&msg.body, &msg.headers);
        tracing::info!(order.id, meta.tenant, trace = ?meta.trace, "order billed");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Bill)
                .build()
                .name("orders"),
        );
    })
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, OrderMeta, app};

    #[tokio::test]
    async fn the_handler_reads_the_headers_it_declared() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let meta = OrderMeta {
            tenant: "acme".to_owned(),
            trace: Some("4bf92f35".to_owned()),
        };
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .with_headers(&meta)
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.broker::<SqsBroker>()
            .published::<Order>("orders")
            .assert_called_once()
            .with_header("tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }

    /// The harness refuses to inject what SQS refuses to send.
    #[tokio::test]
    #[should_panic(expected = "at most 10 message attributes")]
    async fn a_message_with_eleven_headers_is_refused() {
        let tb = TestApp::start(app()).await.expect("the service starts");
        let crowded: BTreeMap<String, String> = (1..=11)
            .map(|n| (format!("tag-{n}"), "on".to_owned()))
            .collect();

        let _ = tb
            .broker::<SqsBroker>()
            .publish_with_headers("orders", &Order { id: 8 }, &crowded)
            .await;
    }
}
