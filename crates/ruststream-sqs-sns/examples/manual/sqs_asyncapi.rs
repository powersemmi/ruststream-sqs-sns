//! The service of `examples/sqs_asyncapi.rs` without the macros: the handlers are named types,
//! the descriptors join the chain through `map_source`, and `main` is written by hand. The tests
//! read the same document.
//!
//! ```text
//! cargo run --example manual_sqs_asyncapi --features asyncapi,sns -- asyncapi gen
//! cargo test --example manual_sqs_asyncapi --features asyncapi,sns
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

#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing, JsonSchema)]
struct Confirmation {
    id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct Payment {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing, JsonSchema)]
struct PaymentSettled {
    id: u64,
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct Ship;

impl Handle<Order> for Ship {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "shipped");
        ready(Ok(()))
    }
}

struct Settle;

impl Handle<Payment, PaymentSettled> for Settle {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<PaymentSettled, HandlerOutcome>> {
        ready(Ok(PaymentSettled { id: payment.id }))
    }
}

// --8<-- [start:channels]
/// The queue `orders`, polled for up to ten seconds per receive, each message hidden for thirty
/// seconds while it is handled.
fn orders() -> SqsQueue {
    SqsQueue::new("orders")
        .wait(Duration::from_secs(10))
        .visibility(Duration::from_secs(30))
}

/// A FIFO queue: the `.fifo` suffix says so.
fn shipments() -> SqsQueue {
    SqsQueue::new("shipments.fifo")
}
// --8<-- [end:channels]

// --8<-- [start:server]
/// The service's region, through its regional endpoint.
fn broker() -> SqsBroker {
    SqsBroker::new()
        .endpoint("https://sqs.eu-west-1.amazonaws.com")
        .region("eu-west-1")
}
// --8<-- [end:server]

// --8<-- [start:app]
// --8<-- [start:topics]
fn service(broker: SqsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("sqs", broker, |b| {
        let confirm = subscriber("orders", Confirm).map_source(|_| orders());
        b.include(confirm.reply().to("confirmations").build());
        b.include(
            subscriber("shipments.fifo", Ship)
                .map_source(|_| shipments())
                .build(),
        );
        // The reply fans out from a FIFO topic.
        let settle = subscriber("payments", Settle);
        b.include(settle.reply().to("payment-events.fifo").build())
            .out_reply(SnsPublish::default());
    })
}
// --8<-- [end:topics]

fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker())).to_json()?,
        )?)
    }

    #[test]
    fn the_server_is_the_endpoints_host() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["sqs"],
            json!({
                "host": "sqs.eu-west-1.amazonaws.com",
                "protocol": "sqs",
            })
        );
        Ok(())
    }

    #[test]
    fn a_subscription_reports_its_queue_and_polling() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["channels"]["orders"]["bindings"],
            json!({
                "sqs": {
                    "bindingVersion": "0.3.0",
                    "queue": {
                        "name": "orders",
                        "fifoQueue": false,
                        "visibilityTimeout": 30,
                        "receiveMessageWaitTime": 10,
                    },
                },
            })
        );
        assert_eq!(
            spec["channels"]["shipments.fifo"]["bindings"]["sqs"]["queue"],
            json!({ "name": "shipments.fifo", "fifoQueue": true, "receiveMessageWaitTime": 20 })
        );
        Ok(())
    }

    #[test]
    fn a_reply_reports_the_queue_it_reaches() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["channels"]["confirmations"]["bindings"]["sqs"]["queue"],
            json!({ "name": "confirmations", "fifoQueue": false })
        );
        Ok(())
    }

    #[test]
    fn a_fan_out_reply_reports_its_topic() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["channels"]["payment-events.fifo"]["bindings"],
            json!({
                "sns": {
                    "bindingVersion": "1.0.0",
                    "name": "payment-events.fifo",
                    "ordering": { "type": "FIFO", "contentBasedDeduplication": false },
                },
            })
        );
        Ok(())
    }
}
