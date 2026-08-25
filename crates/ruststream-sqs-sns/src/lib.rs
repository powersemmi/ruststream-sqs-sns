//! Amazon SQS broker implementation for `RustStream`, with SNS fan-out publishing.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
//! transport over the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) and
//! [`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) clients.
//!
//! - Deleting a message is the acknowledgement, zeroing its visibility is the requeue, and a
//!   retry with a delay sets the visibility timeout to that delay - the framework's deferred
//!   retry is native, not emulated.
//! - A handler outliving the visibility timeout is protected by crate-owned background
//!   extension for as long as it holds the message.
//! - The queue's redrive policy provides dead-lettering; FIFO message group ids map onto the
//!   partition key.
//! - SNS appears only as a publisher (fan-out to queues and other endpoints), as a distinct
//!   [`SnsPublish`] policy.

#![forbid(unsafe_code)]

mod broker;
mod error;
mod message;
pub mod prelude;
mod publisher;
mod queue;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ConnectedSqsBroker, SqsBroker};
pub use error::SqsError;
pub use message::{PARTITION_KEY_HEADER, RECEIVE_COUNT_HEADER, SqsMessage};
pub use publisher::{SnsPublish, SnsPublisher, SqsPublish, SqsPublisher};
pub use queue::SqsQueue;
pub use subscriber::SqsSubscriber;
