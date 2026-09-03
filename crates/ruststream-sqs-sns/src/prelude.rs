//! The imports a service on SQS/SNS writes every time, in one glob.
//!
//! `use ruststream_sqs_sns::prelude::*;` carries the framework's own prelude plus this crate's
//! broker, queue descriptor, publish policies and live publishers.
//!
//! Everything re-exported here is the framework's own item, so a service on two brokers may glob
//! both preludes and what they share resolves to one item.
//!
//! A service writes two kinds of file, and this glob is for one of them. A handler file names the
//! capabilities it needs and globs the framework's prelude alone, so `Publisher` there is the
//! framework's trait, the bound an injected publisher carries. A routes file names the broker it
//! mounts on and globs this one, where [`Publish`] is the value a mount site or a lifecycle hook
//! hands over. The two vocabularies never meet in one file, which is what keeps the uniform
//! mount-site name free for the policy.
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

/// The publish policy a mount site hands to `include` and the lifecycle hooks, [`SqsPublish`]
/// under the name every broker crate gives it. `SnsPublish` keeps its own name: fan-out is the
/// departure from the default, not the mount site's default choice.
pub use crate::SqsPublish as Publish;
