//! The service of `examples/sqs_settlement_retry_after.rs` written without the macros: an
//! `Err(retry_after(delay))` sets the message's visibility timeout to the delay.
//!
//! ```text
//! cargo test --example manual_sqs_settlement_retry_after --features testing
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_sqs_settlement_retry_after --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Invoice {
    id: u64,
}

// --8<-- [start:retry_after]
/// How long an invoice whose payment has not cleared waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(5);

/// The payment of an invoice clears within seconds, so the first receive asks for the invoice
/// again later and the second one settles it.
struct Settle;

impl Handle<Invoice> for Settle {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.headers().get_str(RECEIVE_COUNT_HEADER) == Some("1") {
            tracing::info!(invoice.id, "payment not cleared yet");
            // ChangeMessageVisibility to five seconds: nothing is republished.
            return ready(Err(HandlerOutcome::retry_after(RECHECK)));
        }
        tracing::info!(invoice.id, "invoice settled");
        ready(Ok(()))
    }
}
// --8<-- [end:retry_after]

fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Settle)
                .build()
                .name(queue),
        );
    })
}

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

fn app() -> impl App {
    service(broker("http://localhost:4566"), "invoices".to_owned())
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;

    use super::*;

    /// One body for both modes: the invoice comes back once the delay has passed, and the queue
    /// holds the one message the test published.
    async fn an_invoice_comes_back_after_the_delay(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Invoice { id: 1 })
            .to(queue.to_owned())
            .publish()
            .await?;
        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called(2)
            .with(&Invoice { id: 1 })
            .settled(HandlerOutcome::ack());
        tb.broker::<SqsBroker>()
            .published::<Invoice>(queue)
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn in_process_an_invoice_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let app = service(broker("http://localhost:4566"), "invoices".to_owned());
        an_invoice_comes_back_after_the_delay(TestApp::start(app).await?, "invoices").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_an_invoice_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("retry-after-invoices");
        live::admin(&endpoint)
            .await
            .create_queue()
            .queue_name(&queue)
            .send()
            .await?;

        let app = service(broker(&endpoint), queue.clone());
        an_invoice_comes_back_after_the_delay(TestApp::start_live(app).await?, &queue).await
    }
}
