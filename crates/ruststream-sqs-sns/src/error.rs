//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the Amazon SQS/SNS broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors so the public API does not leak the SDK's
/// layered error types (they are formatted with their full cause chain).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqsError {
    /// Loading the AWS configuration failed.
    #[error("aws config error: {0}")]
    Config(String),

    /// Resolving a queue name to its URL (or creating the queue) failed.
    #[error("sqs queue error for '{name}': {source}")]
    Queue {
        /// The queue name or URL the call was about.
        name: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Receiving from a queue failed.
    #[error("sqs receive error on '{queue}': {source}")]
    Receive {
        /// The queue URL the receive targeted.
        queue: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Sending a message (SQS) or publishing a notification (SNS) failed.
    #[error("publish error to '{destination}': {source}")]
    Publish {
        /// The queue or topic the message targeted.
        destination: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// A topic admin call (create, subscribe) failed.
    #[error("sns admin error for '{name}': {source}")]
    Admin {
        /// The topic or subscription the call was about.
        name: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("sqs broker is not connected")]
    NotConnected,

    /// A queue descriptor is invalid.
    #[error("invalid sqs queue descriptor: {0}")]
    InvalidQueue(String),
}

/// Formats an SDK error with its full cause chain and boxes it, so transport failures stay
/// distinguishable from service errors in logs (`DisplayErrorContext` walks the chain).
pub(crate) fn sdk_err<E, R>(
    err: &aws_sdk_sqs::error::SdkError<E, R>,
) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    Box::from(aws_sdk_sqs::error::DisplayErrorContext(err).to_string())
}
