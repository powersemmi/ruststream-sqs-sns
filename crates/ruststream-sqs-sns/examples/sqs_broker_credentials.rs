//! A broker built from an AWS config the service assembles itself: a named profile, a region and
//! an attempt timeout. The in-process test runs everywhere; the live one builds the same config
//! for the stack behind `SQS_TEST_ENDPOINT` and skips without it.
//!
//! ```text
//! cargo test --example sqs_broker_credentials --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example sqs_broker_credentials --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::config::SharedCredentialsProvider;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue)]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order received");
    HandlerOutcome::ack()
}

/// The service reading `queue`, which it creates on a fresh stack.
fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(handle.name(queue).create_if_missing());
    })
}

// --8<-- [start:config]
/// The credentials of the `orders` profile in the shared AWS files, in `eu-west-1`. The broker
/// uses the config as it is, so the config carries the attempt timeout: above the 20 seconds a
/// long poll waits.
fn config() -> SdkConfig {
    let profile = ProfileFileCredentialsProvider::builder()
        .profile_name("orders")
        .build();
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("eu-west-1"))
        .credentials_provider(SharedCredentialsProvider::new(profile))
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(Duration::from_secs(25))
                .build(),
        )
        .build()
}

#[app]
fn app() -> impl App {
    service(SqsBroker::from_config(config()), "orders".to_owned())
}
// --8<-- [end:config]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use aws_config::timeout::TimeoutConfig;
    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use aws_sdk_sqs::config::{Credentials, SharedCredentialsProvider};
    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Order, config, live, service};

    /// The body both modes run: an order sent to the queue reaches the handler.
    async fn an_order_reaches_the_handler(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Order { id: 7 })
            .to(queue.to_owned())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let app = service(SqsBroker::from_config(config()), "orders".to_owned());
        an_order_reaches_the_handler(TestApp::start(app).await?, "orders").await
    }

    /// The same config, pointed at the stack with the static key a stack accepts.
    fn stack_config(endpoint: &str) -> SdkConfig {
        let key = Credentials::new("test", "test", None, None, "stack");
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(key))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(Duration::from_secs(25))
                    .build(),
            )
            .build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("credentials-orders");
        let app = service(
            SqsBroker::from_config(stack_config(&endpoint)),
            queue.clone(),
        );
        an_order_reaches_the_handler(TestApp::start_live(app).await?, &queue).await
    }
}
