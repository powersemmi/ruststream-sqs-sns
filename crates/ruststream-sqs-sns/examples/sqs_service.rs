//! A minimal SQS service: one `#[subscriber]` handler that long-polls the queue `orders`.
//!
//! `SqsBroker::new()` takes the region, the credentials and the endpoint from the environment,
//! so the same binary runs against AWS and against a local stack.
//!
//! ```text
//! just brokers-up
//! AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
//!     AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//!     cargo run --example sqs_service --features testing -- run
//! cargo test --example sqs_service --features testing
//! ```

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// Reads the queue `orders`, each receive waiting up to 20 seconds for a message.
///
/// A fresh local stack has no queues, so the descriptor creates the queue at startup. A
/// production service drops that step and takes the queue from its infrastructure.
#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(20))
        .create_if_missing()
)]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order received");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(handle);
    })
}
// --8<-- [end:app]

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
