//! The `AsyncAPI` document of an SQS service: the server it connects to, the queue behind each
//! channel, and the topic of a fan-out reply. The tests read the document the app generates.
//!
//! ```text
//! cargo run --example sqs_asyncapi --features asyncapi,sns -- asyncapi gen
//! cargo test --example sqs_asyncapi --features asyncapi,sns
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
struct PaymentSettled {
    id: u64,
}

// --8<-- [start:server]
/// The service's region, through its regional endpoint.
fn broker() -> SqsBroker {
    SqsBroker::new()
        .endpoint("https://sqs.eu-west-1.amazonaws.com")
        .region("eu-west-1")
}
// --8<-- [end:server]

// --8<-- [start:channels]
#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(10))
        .visibility(Duration::from_secs(30)),
    reply("confirmations")
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[subscriber(SqsQueue::new("shipments.fifo"))]
async fn ship(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "shipped");
    HandlerOutcome::ack()
}
// --8<-- [end:channels]

// --8<-- [start:topics]
/// The reply names a FIFO topic.
#[subscriber(SqsQueue::new("payments"), reply("payment-events.fifo"))]
async fn settle(payment: &Payment) -> PaymentSettled {
    PaymentSettled { id: payment.id }
}

// --8<-- [start:app]
fn service(broker: SqsBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker_labeled("sqs", broker, |b| {
        b.include(confirm);
        b.include(ship);
        // The reply fans out from the topic its clause names.
        b.include(settle).out_reply(SnsPublish::default());
    })
}
// --8<-- [end:topics]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker())
}
// --8<-- [end:app]

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
