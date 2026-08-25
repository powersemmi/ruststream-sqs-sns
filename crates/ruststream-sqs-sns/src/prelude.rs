//! The imports a service on SQS/SNS writes every time, in one glob.
//!
//! `use ruststream_sqs_sns::prelude::*;` brings in the framework's own prelude and this crate's
//! user-facing surface: the broker, the queue descriptor, the publish policies, and the live
//! publishers a handle-taking helper names. The publish policy is also available under the
//! uniform policy vocabulary as [`Publish`], the name every broker crate gives the value a mount
//! site hands to `include` and the lifecycle hooks.
//!
//! The framework's prelude stops short of brokers on purpose - which broker a service runs on is
//! the one thing every service states for itself. This glob is that statement: the broker
//! specificity lives in the crate path, so a file that writes this import has already said which
//! broker it is on, and the framework's prelude can ride along on the same line.
//!
//! It is also a capability manifest: the glob carries the framework's capability traits this
//! broker's connected and delivered forms implement, and only those. For SQS that set is empty,
//! which is an honest statement rather than an omission - the one capability the crate does
//! implement reaches services through a method the framework's prelude already brings, and the
//! rest SQS has no verb for. The comment on the re-exports below records both halves, and the
//! crate's guide has the full table.
//!
//! Because everything re-exported here is the framework's own item rather than an alias, a
//! service on two brokers may glob both preludes: what they share resolves to one item, and the
//! compiler is the one checking that.
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

// The capability manifest is empty, for two different reasons.
//
// Not implemented, so not here: SQS has no transactional send, no reply inbox, no batch handler
// surface, and a queue is not a replayable log - `TransactionalPublisher`, `OwnedTransactions`,
// `RequestReply`, `BatchSubscriber`, `Seekable` and `Positioned` are absent from the glob because
// they are absent from the crate.
//
// Implemented, and still not here: `Partitioned`. `SqsMessage` carries the FIFO message group
// back as the partition key, but the framework surfaces that through `IncomingMessage`'s
// defaulted `partition_key`, which its own prelude already brings - and this crate's impl
// overrides it to delegate. Re-exporting the trait would put two applicable methods in scope, so
// the natural `msg.partition_key()` on a delivery would stop compiling (E0034) and every caller
// would owe a UFCS spelling. A service reaching for the trait itself imports it by name.

pub use crate::{SnsPublish, SnsPublisher, SqsBroker, SqsPublish, SqsPublisher, SqsQueue};

/// The include-site name for this broker's plain publish policy, [`SqsPublish`].
///
/// The policy vocabulary is uniform across broker crates: a concept keeps one name, with the
/// broker prefix stripped, so a mount site reads the same whichever broker it is on. It is a
/// manifest on the policy layer too - a concept name this prelude does not export is one this
/// broker has no policy for. `Publish` is the whole of it here: SQS has no transactional send and
/// no reply inbox, so there is no transactional or request policy to name.
///
/// [`SnsPublish`] is deliberately outside that vocabulary rather than a second `Publish`. It is
/// not another form: it implements `PublishPolicy` for the very same `ConnectedSqsBroker`, brings
/// no subscription descriptor of its own, and fans out to queues that ordinary [`SqsQueue`]
/// subscriptions consume. So it is a second policy on one form, and keeping it prefixed is what
/// says so - reaching for fan-out stays the deliberate choice it is.
///
/// The prefixed originals stay available at the crate root (and here) for a file that mixes both.
///
/// Not to be confused with the framework's `runtime::Publish`, which is the publish builder a
/// call site chains; this is the policy value handed to `include` and the lifecycle hooks.
pub use crate::SqsPublish as Publish;

// Deliberately absent, and why:
//
// - `testing::SqsTestBroker` and the rest of the `testing` module: feature-gated broker-author
//   tooling rather than service API, so a test that reaches for it imports it by name.
// - `SqsMessage`, `SqsSubscriber`, `ConnectedSqsBroker` and the header-name constants: the
//   message-level machinery, on the framework prelude's own reasoning - a service publishes and
//   consumes through the builder and the handler surface, and code working a layer below says so
//   by importing the type it works with.
// - `SqsError`: a service names the error where it handles it, not everywhere it imports.
