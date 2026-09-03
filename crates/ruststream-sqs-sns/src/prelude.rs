//! The imports a service on SQS/SNS writes every time, in one glob.
//!
//! `use ruststream_sqs_sns::prelude::*;` carries the framework's own prelude plus this crate's
//! broker, queue descriptor, publish policies and live publishers.
//!
//! Everything re-exported here is the framework's own item, so a service on two brokers may glob
//! both preludes and what they share resolves to one item.
//!
//! The publish policies keep their prefixed names, [`SqsPublish`] and [`SnsPublish`]. The bare
//! capability names belong to the framework - `Publish` is the bound a handler body writes on an
//! injected publisher - and an alias here would win over the glob rather than sit beside it.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//!
//! use ruststream_sqs_sns::prelude::*;
//!
//! // A payload the service parses itself, so the glob really is the whole import list: a
//! // decoded payload would add its own `serde` derive on top.
//! #[derive(Deserialized)]
//! struct Order<'a>(&'a [u8]);
//!
//! #[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)))]
//! async fn handle(order: &Order<'_>) -> HandlerOutcome {
//!     let _ = order.0.len();
//!     HandlerOutcome::ack()
//! }
//!
//! #[app]
//! fn service() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0"))
//!         .with_broker(SqsBroker::new(), |b| b.include(handle))
//! }
//! ```

pub use ruststream::prelude::*;

// `Partitioned` stays out although this crate implements it: `IncomingMessage::partition_key` is
// already in scope through the framework's prelude, so re-exporting the trait would make
// `msg.partition_key()` on a delivery ambiguous (E0034).

pub use crate::{SnsPublish, SnsPublisher, SqsBroker, SqsPublish, SqsPublisher, SqsQueue};
