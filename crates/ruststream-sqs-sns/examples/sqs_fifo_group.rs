//! FIFO message groups: ordering per customer, with the group carried by the publisher.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sqs_fifo_group`

use std::io;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

/// The message declares both where it goes and which headers travel with it.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-groups.fifo", headers = Shipment)]
struct OrderPlaced {
    id: u64,
    customer: String,
}

#[derive(Debug, Serialize)]
struct Shipment {
    carrier: String,
}

#[subscriber(SqsQueue::new("orders-groups.fifo").create_if_missing())]
async fn handle(order: &OrderPlaced) -> HandlerOutcome {
    println!("order {} for {}", order.id, order.customer);
    HandlerOutcome::ack()
}

// --8<-- [start:publish]
async fn place(publisher: &SqsPublisher, order: &OrderPlaced) -> io::Result<()> {
    let meta = Shipment {
        carrier: "dhl".to_owned(),
    };
    // Ordering is per customer: the handle carries the group, the contract carries the headers.
    publisher
        .with_group_id(&order.customer)
        .message(order)
        .with_headers(&meta)
        .publish()
        .await
        .map_err(io::Error::other)
}
// --8<-- [end:publish]

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("sqs-fifo-group", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            b.include(handle);
            b.after_startup(Publish, async move |sqs| -> io::Result<()> {
                let order = OrderPlaced {
                    id: 1,
                    customer: "acme".to_owned(),
                };
                place(&sqs, &order).await
            });
        },
    )
}
