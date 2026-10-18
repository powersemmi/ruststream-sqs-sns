//! The service of the Running in production guide: the broker is built from an AWS configuration
//! of the service's own, with a pinned behaviour version, a region and the credentials of a named
//! profile, and it reads a queue its infrastructure owns.
//!
//! ```text
//! cargo run --example guide_production --features testing -- run
//! ```

use std::time::Duration;

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::config::SharedCredentialsProvider;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:config]
/// The AWS configuration of the service: a pinned behaviour version, its region, the
/// credentials of the `orders-service` profile, read when the first request is signed, and a
/// request timeout above the 20 seconds a long poll waits.
fn aws_config() -> SdkConfig {
    let credentials = ProfileFileCredentialsProvider::builder()
        .profile_name("orders-service")
        .build();
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new("eu-west-1"))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(Duration::from_secs(25))
                .build(),
        )
        .build()
}
// --8<-- [end:config]

// --8<-- [start:service]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    amount_cents: u64,
}

// The queue belongs to the infrastructure, so the descriptor does not create it. It names its
// visibility, so the subscription does not read the queue's own.
#[subscriber(SqsQueue::new("orders").visibility(Duration::from_secs(120)))]
async fn take_order(order: &Order) -> HandlerOutcome {
    if order.amount_cents == 0 {
        tracing::warn!(order.id, "order without an amount dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(order.id, order.amount_cents, "order taken");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        SqsBroker::from_config(aws_config()),
        |b| {
            b.include(take_order);
        },
    )
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_configured_broker_takes_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&Order {
                id: 1,
                amount_cents: 1_250,
            })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&Order {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
