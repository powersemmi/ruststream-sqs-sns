//! The imports a service on SQS/SNS writes every time, in one glob.
//!
//! `use ruststream_sqs_sns::prelude::*;` carries the framework's own prelude plus this crate's
//! broker, queue descriptor, publish policies and live publishers.
//!
//! Everything re-exported here is the framework's own item, so a service on two brokers may glob
//! both preludes and what they share resolves to one item.
//!
//! [`Publish`] is this crate's publish policy; the framework's `runtime::Publish` is the publish
//! builder, a different type.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//!
//! use ruststream_sqs_sns::prelude::*;
//!
//! #[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)))]
//! async fn handle(order: &[u8]) -> HandlerResult {
//!     let _ = order.len();
//!     HandlerResult::Ack
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

/// The publish policy a mount site hands to `include` and the lifecycle hooks, [`SqsPublish`]
/// under the name every broker crate gives it.
pub use crate::SqsPublish as Publish;
