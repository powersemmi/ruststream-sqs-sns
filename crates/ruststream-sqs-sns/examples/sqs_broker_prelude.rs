//! The prelude: one glob brings the framework's prelude, the broker, the queue descriptor and
//! the publish policy under the name `Publish`.
//!
//! ```text
//! cargo test --example sqs_broker_prelude --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_sqs_sns::prelude::*;

#[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)), reply)]
async fn accept(order: &Order) -> OrderAccepted {
    tracing::info!(order.id, "order accepted");
    OrderAccepted { id: order.id }
}

#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(accept).out_reply(Publish::default());
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-accepted")]
struct OrderAccepted {
    id: u64,
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
