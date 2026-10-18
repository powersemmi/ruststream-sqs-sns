//! The invoices service of the Retries and dead-letter queues guide: an invoice the customer's
//! credit does not cover waits and comes back, and after four deliveries SQS moves it to a
//! dead-letter queue that a second handler reads.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_retries --features testing -- run
//! ```

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:settle]
/// The largest invoice charged without a credit check.
const CREDIT_LIMIT_CENTS: u64 = 100_000;

/// How long an invoice waits for the credit check before it comes back.
const CREDIT_CHECK: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    customer: String,
    amount_cents: u64,
}

#[subscriber(SqsQueue::new("invoices").create_if_missing())]
async fn charge(invoice: &Invoice) -> HandlerOutcome {
    if invoice.customer.is_empty() {
        // Another try gets the same answer.
        tracing::warn!(invoice.id, "invoice without a customer dropped");
        return HandlerOutcome::drop();
    }
    if invoice.amount_cents > CREDIT_LIMIT_CENTS {
        tracing::info!(invoice.id, "invoice waits for the credit check");
        return HandlerOutcome::retry_after(CREDIT_CHECK);
    }
    tracing::info!(invoice.id, invoice.amount_cents, "invoice charged");
    HandlerOutcome::ack()
}
// --8<-- [end:settle]

// --8<-- [start:review]
/// Reads the invoices SQS moved away after their last delivery.
#[subscriber(SqsQueue::new("invoices-dead").create_if_missing())]
async fn review(invoice: &Invoice) -> HandlerOutcome {
    tracing::error!(invoice.id, invoice.customer, "invoice needs a review");
    HandlerOutcome::ack()
}
// --8<-- [end:review]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .region("us-east-1")
            .test_credentials(),
        |b| {
            // Four deliveries of one invoice, the first included; then SQS moves it.
            b.include(charge)
                .max_attempts(nonzero!(4u32))
                .dead_letter("invoices-dead");
            b.include(review);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{CREDIT_CHECK, Invoice, app, charge};

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
                b.include(charge).max_attempts(nonzero!(4u32));
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
