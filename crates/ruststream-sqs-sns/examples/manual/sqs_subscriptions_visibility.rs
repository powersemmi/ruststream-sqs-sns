//! The service of `examples/sqs_subscriptions_visibility.rs` written without the macros: the
//! visibility timeout is a step of the chain once `name` has built the queue's descriptor.
//!
//! ```text
//! cargo test --example manual_sqs_subscriptions_visibility --features testing
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_sqs_subscriptions_visibility --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Report {
    id: u64,
}

/// How long rendering one report takes: longer than a receive hides it.
const RENDERING: Duration = Duration::from_secs(5);

/// Renders a report; stands in for the slow work a real service does.
async fn render(report: &Report) {
    tracing::debug!(report.id, "rendering");
    tokio::time::sleep(RENDERING).await;
}

// --8<-- [start:visibility]
/// Rendering a report takes longer than the two seconds a receive hides it for. The crate
/// re-arms the visibility every second while the handler runs, so no other consumer gets the
/// report in the meantime.
struct PublishReport;

impl Handle<Report> for PublishReport {
    async fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        render(report).await;
        tracing::info!(report.id, "report published");
        Ok(())
    }
}

fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), PublishReport)
                .build()
                .name(queue)
                .visibility(Duration::from_secs(2)),
        );
    })
}
// --8<-- [end:visibility]

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

fn app() -> impl App {
    service(broker("http://localhost:4566"), "reports".to_owned())
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// One body for both modes: the report is handled once, and nothing comes back after the
    /// handler has outlived the visibility timeout twice over.
    async fn a_report_outlasting_its_visibility_is_handled_once(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Report { id: 4 })
            .to(queue.to_owned())
            .publish()
            .await?;
        tb.advance(Duration::from_secs(3)).await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&Report { id: 4 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn in_process_a_report_outlasting_its_visibility_is_handled_once()
    -> Result<(), Box<dyn Error>> {
        let app = service(broker("http://localhost:4566"), "reports".to_owned());
        a_report_outlasting_its_visibility_is_handled_once(TestApp::start(app).await?, "reports")
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_report_outlasting_its_visibility_is_handled_once() -> Result<(), Box<dyn Error>>
    {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("visibility-reports");
        live::admin(&endpoint)
            .await
            .create_queue()
            .queue_name(&queue)
            .send()
            .await?;

        let app = service(broker(&endpoint), queue.clone());
        a_report_outlasting_its_visibility_is_handled_once(TestApp::start_live(app).await?, &queue)
            .await
    }

    #[tokio::test]
    async fn a_visibility_past_twelve_hours_stops_startup() {
        let app = RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(
                    subscriber(Unnamed::<SqsQueue>::new(), PublishReport)
                        .build()
                        .name("reports")
                        .visibility(Duration::from_hours(13)),
                );
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("SQS hides a message for at most 12 hours");
        assert!(refused.to_string().contains("visibility"), "{refused}");
    }
}
