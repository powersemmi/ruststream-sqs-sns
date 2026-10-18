//! Headers on SQS: each header travels as one message attribute, and a handler reads them as a
//! typed contract.
//!
//! ```text
//! cargo test --example sqs_delivery_headers --features testing
//! ```

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders", headers = OrderMeta)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// The headers an order carries: two message attributes on the queue.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct OrderMeta {
    tenant: String,
    trace: Option<String>,
}

#[subscriber(SqsQueue::new("orders"))]
async fn bill(order: &Order, Headers(meta): Headers<OrderMeta>) -> HandlerOutcome {
    tracing::info!(order.id, meta.tenant, trace = ?meta.trace, "order billed");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(bill);
    })
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
