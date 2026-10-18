//! The service of `examples/sqs_testing_live.rs` without the macros, and the same tests: one
//! body, in process and against a running stack.
//!
//! ```text
//! cargo test --example manual_sqs_testing_live --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_sqs_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[derive(Outgoing)]` by hand: a payment names no queue, so each publish names one.
impl OutgoingDestination for Payment {
    type Form = CallerName;
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

/// The service on the stack at `endpoint`, reading `queue`, which it creates on a fresh stack.
fn app(endpoint: &str, queue: String) -> impl App<State = ()> {
    let broker = SqsBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials();
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Accept)
                .build()
                .name(queue)
                .create_if_missing(),
        );
    })
}

fn service() -> impl App<State = ()> {
    app("http://localhost:4566", "payments".to_owned())
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(service)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{Payment, app, live};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>, queue: &str) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Payment { amount: 100 })
            .to(queue.to_owned())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let app = app("http://localhost:4566", "payments".to_owned());
        accepts_a_payment(TestApp::start(app).await?, "payments").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        // A queue of this run's own, so nothing an earlier run left on the stack is read.
        let queue = live::unique("payments");
        accepts_a_payment(
            TestApp::start_live(app(&endpoint, queue.clone())).await?,
            &queue,
        )
        .await
    }
    // --8<-- [end:start-live]
}
