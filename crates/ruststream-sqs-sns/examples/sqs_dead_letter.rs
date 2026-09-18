//! Capping the attempts: the registration declares them and the queue carries a spent delivery
//! to its dead-letter queue.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sqs_dead_letter`

use std::io;
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

// --8<-- [start:handler]
/// A handler that is never ready. Every delivery asks to come back later, so what ends the
/// message is the cap: the queue hands it over three times and then moves it.
#[subscriber(SqsQueue::new("invoices").create_if_missing())]
async fn settle(invoice: &Invoice) -> HandlerOutcome {
    println!("invoice {} is not ready yet", invoice.id);
    HandlerOutcome::retry_after(Duration::from_secs(5))
}
// --8<-- [end:handler]

// --8<-- [start:mount]
#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(
        SqsBroker::new()
            .endpoint("http://localhost:4566")
            .test_credentials()
            .region("us-east-1"),
        |b| {
            // The two steps are the queue's redrive policy: `maxReceiveCount` and the
            // dead-letter queue it points at. The subscription writes them when it opens.
            b.include(settle)
                .max_attempts(nonzero!(3u32))
                .dead_letter("invoices-dead");
            b.after_startup(Publish::default(), async move |sqs| -> io::Result<()> {
                sqs.message(&Invoice { id: 1 })
                    .publish()
                    .await
                    .map_err(io::Error::other)
            });
        },
    )
}
// --8<-- [end:mount]
