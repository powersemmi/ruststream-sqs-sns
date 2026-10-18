//! A subscription by queue name: `#[subscriber("orders")]` long-polls the queue `orders`, and
//! `#[subscriber(SqsQueue)]` leaves the name to the mount site, so one definition reads two
//! queues. Each scenario is a test.
//!
//! ```text
//! cargo test --example sqs_subscriptions_name --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Entry {
    id: u64,
}

// --8<-- [start:name]
/// Reads the queue `orders` with the descriptor's defaults: a 20 second long poll and the
/// queue's own visibility timeout.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:name]

// --8<-- [start:kind]
/// The definition fixes the kind of subscription and leaves the queue to the mount site.
#[subscriber(SqsQueue)]
async fn audit(entry: &Entry) -> HandlerOutcome {
    tracing::info!(entry.id, "entry audited");
    HandlerOutcome::ack()
}

fn audits(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include(audit.name("audit-eu"));
        b.include(audit.name("audit-us"));
    })
}
// --8<-- [end:kind]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

#[app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn the_queue_the_attribute_names_is_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

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

    #[tokio::test]
    async fn one_definition_reads_both_queues() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(audits(broker("http://localhost:4566"))).await?;

        for (id, queue) in [(1, "audit-eu"), (2, "audit-us")] {
            tb.broker::<SqsBroker>()
                .message(&Entry { id })
                .to(queue)
                .publish()
                .await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("audit-eu")
            .assert_called_once()
            .with(&Entry { id: 1 });
        tb.broker::<SqsBroker>()
            .subscriber("audit-us")
            .assert_called_once()
            .with(&Entry { id: 2 });
        tb.shutdown().await?;
        Ok(())
    }
}
