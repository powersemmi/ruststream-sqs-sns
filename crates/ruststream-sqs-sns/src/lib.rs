#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod broker;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
mod publisher;
mod queue;
#[cfg(feature = "sns")]
mod sns;
mod subscriber;

pub use broker::{ConnectedSqsBroker, SqsBroker};
pub use error::SqsError;
pub use message::{PARTITION_KEY_HEADER, RECEIVE_COUNT_HEADER, SqsMessage};
pub use publisher::{SqsPublish, SqsPublishOptions, SqsPublishSteps, SqsPublisher};
pub use queue::{SqsQueue, SqsSubscription};
#[cfg(feature = "sns")]
pub use sns::{SnsPublish, SnsPublisher};
pub use subscriber::SqsSubscriber;
