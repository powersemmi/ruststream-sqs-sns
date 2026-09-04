//! Pages: one handler call per `ReceiveMessage`, so a batch of orders costs one round trip
//! each way.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sqs_pages`

use std::io;
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders-pages")]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// A page handler: the slice is one `ReceiveMessage` worth of orders, settled as a whole. It
/// holds at most the size the mount site names, and as few as one when that is all the queue
/// had at the moment of the call.
#[subscriber(SqsQueue::new("orders-pages"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    println!("reconciling a page of {} orders", orders.len());
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:mount]
#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders-pages", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            // The size is the framework's word and becomes `MaxNumberOfMessages`; the long
            // poll and the queue's provisioning are this crate's, chained after it.
            b.include(
                reconcile
                    .batch(nonzero!(10))
                    .wait(Duration::from_secs(20))
                    .create_if_missing(),
            );
            b.after_startup(Publish, async move |sqs| -> io::Result<()> {
                for id in 0..10 {
                    sqs.message(&Order { id })
                        .publish()
                        .await
                        .map_err(io::Error::other)?;
                }
                Ok(())
            });
        },
    )
}
// --8<-- [end:mount]
