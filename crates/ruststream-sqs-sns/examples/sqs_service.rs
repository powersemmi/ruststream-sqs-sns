//! A minimal SQS service: consume orders with long polling.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sqs_service`

// --8<-- [start:handler]
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// The local stack this example runs against has nothing provisioned, so the descriptor opens the
// queue itself. A production service drops that step and takes the queue from its infrastructure.
#[subscriber(
    SqsQueue::new("orders")
        .wait(Duration::from_secs(20))
        .create_if_missing()
)]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| b.include(handle),
    )
}
// --8<-- [end:app]
