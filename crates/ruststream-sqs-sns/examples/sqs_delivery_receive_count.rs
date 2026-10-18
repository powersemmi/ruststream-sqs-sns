//! `RECEIVE_COUNT_HEADER`: the queue's own count of how many times it handed a message over, so
//! a handler can tell its last attempt from the first.
//!
//! ```text
//! cargo test --example sqs_delivery_receive_count --features testing
//! ```

use ruststream_sqs_sns::RECEIVE_COUNT_HEADER;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// Where a payment goes for a person to look at.
#[derive(OutSlot)]
#[publishes(Payment)]
struct Review;

// --8<-- [start:handler]
/// How many times the queue hands a payment over before it moves it to the dead-letter queue.
const ATTEMPTS: u32 = 3;

#[subscriber(SqsQueue::new("payments"))]
async fn charge(
    payment: &Payment,
    cx: &mut Context<'_>,
    Out(review): Out<impl Publisher, Review>,
) -> HandlerOutcome {
    if payment.card_accepted {
        return HandlerOutcome::ack();
    }
    let receives: u32 = cx
        .headers()
        .get(RECEIVE_COUNT_HEADER)
        .and_then(|count| str::from_utf8(count).ok()?.parse().ok())
        .unwrap_or(1);
    if receives < ATTEMPTS {
        return HandlerOutcome::retry();
    }
    // The last attempt: a person looks at the payment before the queue gives up on it.
    tracing::warn!(payment.id, receives, "payment declined, sent to review");
    match review
        .message(payment)
        .to("payments-review")
        .publish()
        .await
    {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

#[app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(charge)
            .max_attempts(nonzero!(ATTEMPTS))
            .dead_letter("payments-dead")
            .out(Review, Publish::default())
            .build();
    })
}
// --8<-- [end:handler]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    card_accepted: bool,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Payment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_last_attempt_sends_the_payment_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Payment {
            id: 7,
            card_accepted: false,
        };
        tb.broker::<SqsBroker>()
            .message(&declined)
            .to("payments")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(3);
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-review")
            .assert_called_once()
            .with(&declined);
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_accepted_payment_is_charged_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<SqsBroker>()
            .message(&Payment {
                id: 8,
                card_accepted: true,
            })
            .to("payments")
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
