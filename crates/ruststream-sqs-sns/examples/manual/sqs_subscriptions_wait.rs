//! The service of `examples/sqs_subscriptions_wait.rs` written without the macros: the long poll
//! is a step of the descriptor the chain maps its source to.
//!
//! ```text
//! cargo test --example manual_sqs_subscriptions_wait --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[outgoing(name = "alerts")]
struct Alert {
    id: u64,
}

struct Page;

impl Handle<Alert> for Page {
    fn handle(
        &self,
        alert: &Alert,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(alert.id, "on-call paged");
        ready(Ok(()))
    }
}

// --8<-- [start:wait]
/// Each receive on `alerts` waits up to five seconds and returns as soon as an alert arrives.
fn alerts() -> SqsQueue {
    SqsQueue::new("alerts").wait(Duration::from_secs(5))
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("alerts", Page).build().map_source(|_| alerts()));
    })
}
// --8<-- [end:wait]

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

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    #[tokio::test]
    async fn an_alert_is_handled() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Alert { id: 1 })
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("alerts")
            .assert_called_once()
            .with(&Alert { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_wait_past_twenty_seconds_stops_startup() {
        let app = RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(
                    subscriber("alerts", Page)
                        .build()
                        .map_source(|_| alerts())
                        .wait(Duration::from_secs(21)),
                );
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("SQS long-polls for at most 20 seconds");
        assert!(refused.to_string().contains("20 second"), "{refused}");
    }
}
