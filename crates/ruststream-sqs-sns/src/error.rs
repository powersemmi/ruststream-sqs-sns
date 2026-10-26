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

    /// A header cannot travel as a message attribute, so the publish refuses the message instead
    /// of sending it without the header.
    #[error("header '{header}' cannot travel to '{destination}': {source}")]
    InvalidHeader {
        /// The queue or topic the message targeted.
        destination: String,
        /// The header's name.
        header: String,
        /// Why the header has no attribute form.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// A message needs more message attributes than SQS and SNS carry on one message.
    ///
    /// Every header but [`PARTITION_KEY_HEADER`](crate::PARTITION_KEY_HEADER) becomes an
    /// attribute, and the crate adds `ruststream-payload-encoding` to a payload that travels
    /// base64.
    #[error(
        "'{destination}' takes at most {limit} message attributes, and this message needs \
         {count}: every header but 'partition-key' becomes one{}",
        added_note(*.crate_added)
    )]
    TooManyAttributes {
        /// The queue or topic the message targeted.
        destination: String,
        /// The attributes the message needs.
        count: usize,
        /// The most a message carries.
        limit: usize,
        /// The attribute the crate itself added to the count, if it added one.
        crate_added: Option<&'static str>,
    },

    /// A topic admin call (create, subscribe) failed.
    #[cfg(feature = "sns")]
    #[error("sns admin error for '{name}': {source}")]
    Admin {
        /// The topic or subscription the call was about.
        name: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Granting a topic the right to deliver into a queue failed.
    ///
    /// SNS delivers to a queue only when the queue's access policy allows it, so a subscription
    /// whose grant could not be written would drop every notification.
    #[cfg(feature = "sns")]
    #[error("granting topic '{topic}' the right to send to queue '{queue}' failed: {source}")]
    QueuePolicy {
        /// The topic the grant is for.
        topic: String,
        /// The queue whose access policy carries the grant.
        queue: String,
        /// The SDK's failure, with its cause chain, or why the queue's policy could not be read.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("sqs broker is not connected")]
    NotConnected,

    /// A queue descriptor is invalid.
    #[error("invalid sqs queue descriptor: {0}")]
    InvalidQueue(String),

    /// A registration declared one half of what the queue's redrive policy needs.
    ///
    /// SQS moves a spent delivery itself, and the policy that makes it do so carries the limit
    /// and the dead-letter queue together. Half a policy is not one, so the subscription refuses
    /// to open rather than running with a declaration the queue never received.
    #[error(
        "the registration on '{queue}' declared {declared} without {missing}; the queue's \
         redrive policy needs max_attempts(..) and dead_letter(..) together"
    )]
    IncompleteRedrive {
        /// The queue the registration subscribes to.
        queue: String,
        /// The half the mount site declared.
        declared: &'static str,
        /// The half it left out.
        missing: &'static str,
    },

    /// A publish named a FIFO-only setting for a destination that is not a FIFO queue or topic.
    ///
    /// The ordering the caller asked for would not happen there, and a value dropped in silence
    /// is the worse answer.
    #[error("'{destination}' is not a FIFO destination, so it cannot honour {setting}")]
    NotFifo {
        /// The queue or topic the message targeted.
        destination: String,
        /// The setting that has no meaning there.
        setting: &'static str,
    },
}

/// The tail of a [`SqsError::TooManyAttributes`] message: which attribute the crate added.
fn added_note(crate_added: Option<&str>) -> String {
    crate_added.map_or_else(
        || ", and the crate added none of its own".to_owned(),
        |name| format!(", and the crate added '{name}' because the payload travels base64"),
    )
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
