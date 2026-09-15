#![doc = include_str!("README.md")]
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
pub use publisher::{
    SnsPublish, SnsPublisher, SqsPublish, SqsPublishOptions, SqsPublishSteps, SqsPublisher,
};
pub use queue::{SqsQueue, SqsSubscription};
pub use subscriber::SqsSubscriber;
