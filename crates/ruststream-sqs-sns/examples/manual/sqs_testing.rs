//! The service of `examples/sqs_testing.rs` without the macros, and the same tests: a test
//! addresses the service the same way in either form.
//!
//! ```text
//! cargo test --example manual_sqs_testing --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    card_accepted: bool,
}

// `#[outgoing(name = "payments")]` by hand: a payment names the queue it is sent to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

/// How long a declined payment waits before the card is tried again.
const RETRY_DELAY: Duration = Duration::from_mins(1);

struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.card_accepted {
            tracing::info!(payment.id, "payment charged");
            return ready(Ok(()));
        }
        ready(Err(HandlerOutcome::retry_after(RETRY_DELAY)))
    }
}

/// The app `main` runs: three attempts, then the queue moves the payment aside.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Charge)
                .build()
                .name("payments"),
        )
        .max_attempts(nonzero!(3u32))
        .dead_letter("payments-dead");
    })
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Payment, RETRY_DELAY, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn an_accepted_card_is_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 7,
            card_accepted: true,
        };
        tb.broker::<SqsBroker>().message(&payment).publish().await?;

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&payment)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:redrive]
    #[tokio::test(start_paused = true)]
    async fn a_declined_card_ends_in_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let payment = Payment {
            id: 8,
            card_accepted: false,
        };
        tb.broker::<SqsBroker>().message(&payment).publish().await?;

        // Each delay hands the payment back; after the third receive the queue moves it.
        for _ in 0..3 {
            tb.advance(RETRY_DELAY).await?;
        }

        tb.broker::<SqsBroker>()
            .subscriber("payments")
            .assert_called(3)
            .settled(HandlerOutcome::retry_after(RETRY_DELAY));
        tb.broker::<SqsBroker>()
            .published::<Payment>("payments-dead")
            .assert_called_once()
            .with(&payment);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:redrive]
}
