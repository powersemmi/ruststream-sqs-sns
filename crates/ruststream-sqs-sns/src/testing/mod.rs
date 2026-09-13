//! In-process test support, behind the `testing` feature.
//!
//! [`SqsTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate the queue's own bookkeeping (dead-letter policies, credit, a visibility timeout
//! that lapses on its own); those are verified end to end against a real broker. What a
//! settlement asks for it does answer, delay included: `retry_after` redelivers once the delay
//! has passed, as `ChangeMessageVisibility` makes it on the queue.
//!
//! The crate's own types are what mount on it: [`SqsQueue`](crate::SqsQueue) opens a subscription
//! here, and [`SqsPublish`](crate::SqsPublish) and [`SnsPublish`](crate::SnsPublish) pair into
//! [`SqsTestPublisher`], so a service's routes file is tested as written rather than rewritten.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedSqsTestBroker, SqsTestBroker, SqsTestPublisher};
pub use subscriber::{SqsTestMessage, SqsTestSubscriber};
