//! A deduplication id on a FIFO queue: a payment SQS delivers twice leads to one charge, because
//! both sends of the charge carry the payment's id as their idempotency key.
//!
//! ```text
//! cargo test --example sqs_publishing_deduplication --features testing
//! cargo run --example sqs_publishing_deduplication --features testing -- run
//! ```

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    account: String,
    amount: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "charges.fifo")]
struct Charge {
    payment_id: u64,
    amount: u64,
}

#[derive(OutSlot)]
#[publishes(Charge)]
struct Charges;

// --8<-- [start:deduplication]
/// SQS delivers a message at least once, so this handler may run twice for one payment. Both
/// runs send the charge under the payment's id, and the FIFO queue keeps the first.
#[subscriber(SqsQueue::new("payments"))]
async fn bill(
    payment: &Payment,
    Out(charges): Out<impl Publisher<Options = SqsPublishOptions>, Charges>,
) -> HandlerOutcome {
    let charge = Charge {
        payment_id: payment.id,
        amount: payment.amount,
    };
    let sent = charges
        .message(&charge)
        .group_id(&payment.account)
        .deduplication_id(format!("payment-{}", payment.id))
        .publish()
        .await;
    if let Err(err) = sent {
        tracing::warn!(payment.id, %err, "charge not sent");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:deduplication]

#[subscriber(SqsQueue::new("charges.fifo"))]
async fn post(charge: &Charge) -> HandlerOutcome {
    tracing::info!(charge.payment_id, charge.amount, "charge posted");
    HandlerOutcome::ack()
}

fn service(broker: SqsBroker) -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(bill).out(Charges, Publish::default()).build();
        b.include(post);
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

#[ruststream::app]
fn app() -> impl App {
    service(broker("http://localhost:4566"))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Charge, Charges, Payment, broker, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_delivered_twice_is_charged_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        let payment = Payment {
            id: 7,
            account: "acme".to_owned(),
            amount: 120,
        };
        for _ in 0..2 {
            tb.broker::<SqsBroker>().message(&payment).publish().await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(2);
        tb.out::<Charges>().assert_called(2).with_options(
            &SqsPublishOptions::default()
                .group_id("acme")
                .deduplication_id("payment-7"),
        );
        tb.broker::<SqsBroker>()
            .subscriber("charges.fifo")
            .assert_called_once()
            .with(&Charge {
                payment_id: 7,
                amount: 120,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_payments_are_two_charges() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker("http://localhost:4566"))).await?;
        for id in [7, 8] {
            tb.broker::<SqsBroker>()
                .message(&Payment {
                    id,
                    account: "acme".to_owned(),
                    amount: 120,
                })
                .publish()
                .await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("charges.fifo")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }
}
