//! The service of `examples/sqs_dead_letter.rs` written without the macros: the cap and the
//! dead-letter queue are the same steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_sqs_dead_letter --features testing
//! cargo run --example manual_sqs_dead_letter --features testing -- run
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
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

/// How long an invoice waits before it is checked again.
const RECHECK: Duration = Duration::from_secs(5);

// --8<-- [start:handler]
/// A handler that is never ready. Every delivery asks to come back later, so what ends the
/// message is the cap: the queue hands it over three times and then moves it.
struct Settle;

impl Handle<Invoice> for Settle {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.id, "invoice is not ready yet");
        ready(Err(HandlerOutcome::retry_after(RECHECK)))
    }
}

/// The dead-letter queue is an ordinary queue, read by an ordinary subscription.
struct Escalate;

impl Handle<Invoice> for Escalate {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(invoice.id, "invoice escalated to a person");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

// --8<-- [start:mount]
fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker, |b| {
        // The two steps are the queue's redrive policy: `maxReceiveCount` and the
        // dead-letter queue it points at. The subscription writes them when it opens.
        b.include(
            subscriber("invoices", Settle)
                .build()
                .map_source(|_| SqsQueue::new("invoices").create_if_missing()),
        )
        .max_attempts(nonzero!(3u32))
        .dead_letter("invoices-dead");
        b.include(subscriber("invoices-dead", Escalate).build());
    })
}
// --8<-- [end:mount]

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

    #[tokio::test(start_paused = true)]
    async fn the_third_receive_moves_the_invoice_to_the_dead_letter_queue()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;

        tb.broker::<SqsBroker>()
            .message(&Invoice { id: 7 })
            .publish()
            .await?;
        // One advance per lapsed delay: the first two hand the invoice back, the third finds
        // its receives spent and moves it.
        for _ in 0..3 {
            tb.advance(RECHECK).await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("invoices")
            .assert_called(3);
        tb.broker::<SqsBroker>()
            .subscriber("invoices-dead")
            .assert_called_once()
            .with(&Invoice { id: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_cap_without_a_dead_letter_queue_stops_startup() {
        let app = RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
            broker("http://localhost:4566"),
            |b| {
                b.include(subscriber("invoices", Settle).build())
                    .max_attempts(nonzero!(3u32));
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("half a redrive policy is not one");
        assert!(refused.to_string().contains("dead_letter(..)"), "{refused}");
    }
}
