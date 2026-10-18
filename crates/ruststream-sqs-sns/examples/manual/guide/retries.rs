//! The invoices service of the Retries and dead-letter queues guide, written without the macros:
//! each handler is a type with an `impl Handle`, mounted with the `subscriber` constructor, and
//! `main` is written by hand.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_retries --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:settle]
/// The largest invoice charged without a credit check.
const CREDIT_LIMIT_CENTS: u64 = 100_000;

/// How long an invoice waits for the credit check before it comes back.
const CREDIT_CHECK: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    id: u64,
    customer: String,
    amount_cents: u64,
}

impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

impl MessageInfo for Invoice {
    const NAME: &'static str = "Invoice";
}

struct Charge;

impl Handle<Invoice> for Charge {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if invoice.customer.is_empty() {
            // Another try gets the same answer.
            tracing::warn!(invoice.id, "invoice without a customer dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        if invoice.amount_cents > CREDIT_LIMIT_CENTS {
            tracing::info!(invoice.id, "invoice waits for the credit check");
            return ready(Err(HandlerOutcome::retry_after(CREDIT_CHECK)));
        }
        tracing::info!(invoice.id, invoice.amount_cents, "invoice charged");
        ready(Ok(()))
    }
}
// --8<-- [end:settle]

// --8<-- [start:review]
/// Reads the invoices SQS moved away after their last delivery.
struct Review;

impl Handle<Invoice> for Review {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(invoice.id, invoice.customer, "invoice needs a review");
        ready(Ok(()))
    }
}
// --8<-- [end:review]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            // Four deliveries of one invoice, the first included; then SQS moves it.
            b.include(
                subscriber("invoices", Charge)
                    .map_source(|_| SqsQueue::new("invoices").create_if_missing())
                    .build(),
            )
            .max_attempts(nonzero!(4u32))
            .dead_letter("invoices-dead");
            b.include(
                subscriber("invoices-dead", Review)
                    .map_source(|_| SqsQueue::new("invoices-dead").create_if_missing())
                    .build(),
            );
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{CREDIT_CHECK, Charge, Invoice, app};

    fn invoice(id: u64, customer: &str, amount_cents: u64) -> Invoice {
        Invoice {
            id,
            customer: customer.to_owned(),
            amount_cents,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_invoice_that_keeps_waiting_ends_in_the_dead_letter_queue()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let large = invoice(1, "acme", 250_000);

        tb.broker::<SqsBroker>().message(&large).publish().await?;
        for _ in 0..4 {
            tb.advance(CREDIT_CHECK).await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("invoices")
            .assert_called(4)
            .settled(HandlerOutcome::retry_after(CREDIT_CHECK));
        tb.broker::<SqsBroker>()
            .subscriber("invoices-dead")
            .assert_called_once()
            .with(&large)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_dropped_invoice_does_not_reach_the_dead_letter_queue() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;

        tb.broker::<SqsBroker>()
            .message(&invoice(2, "", 1_000))
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<SqsBroker>()
            .subscriber("invoices-dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_cap_without_a_dead_letter_queue_refuses_to_start() {
        let app = RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
            SqsBroker::new().region("us-east-1"),
            |b| {
                b.include(
                    subscriber("invoices", Charge)
                        .map_source(|_| SqsQueue::new("invoices"))
                        .build(),
                )
                .max_attempts(nonzero!(4u32));
            },
        );

        let refused = TestApp::start(app)
            .await
            .expect_err("half a redrive policy is refused at startup");
        assert!(
            refused.to_string().contains("dead_letter"),
            "the refusal names the missing half, got {refused}",
        );
    }
}
