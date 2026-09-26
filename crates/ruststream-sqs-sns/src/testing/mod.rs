//! In-process test support, behind the `testing` feature.
//!
//! [`SqsTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and counts
//! the receives of every delivery the way the queue counts them. A registration's redrive policy
//! moves a delivery that has spent its receives to the dead-letter queue, where the harness reads
//! it. What a settlement asks for it answers, delay included: `retry_after` redelivers once the
//! delay has passed, as `ChangeMessageVisibility` makes it on the queue. A batch is capped where
//! one `ReceiveMessage` caps it, at ten messages, whatever size the registration named. A delivery
//! dropped without a settlement is gone here, where the queue returns it once its visibility
//! timeout lapses; that timeout, FIFO ordering and the onward delivery of an SNS fan-out are
//! verified end to end against a real broker.
//!
//! The crate's own types are what mount on it: [`SqsQueue`](crate::SqsQueue) opens a subscription
//! here, and [`SqsPublish`](crate::SqsPublish) and [`SnsPublish`](crate::SnsPublish) pair into
//! [`SqsTestPublisher`], so a service's routes file is tested as written rather than rewritten.

mod broker;
mod router;
mod subscriber;

pub use broker::{ConnectedSqsTestBroker, SqsTestBroker, SqsTestPublisher};
pub use subscriber::{SqsTestMessage, SqsTestSubscriber};
