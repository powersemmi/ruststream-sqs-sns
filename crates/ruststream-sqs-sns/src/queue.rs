//! [`SqsQueue`]: the subscription descriptor.
//!
//! The polling parameters that decide cost and latency are explicit: `wait` (long polling),
//! `batch` (messages per receive call), and `visibility` (the redelivery timeout the crate
//! keeps extending while a handler holds a message).

use std::time::Duration;

use ruststream::SubscriptionSource;

use crate::broker::ConnectedSqsBroker;
use crate::error::SqsError;
use crate::subscriber::SqsSubscriber;

/// The protocol cap on long polling.
const MAX_WAIT: Duration = Duration::from_secs(20);

/// A subscription descriptor for one SQS queue.
///
/// Accepts a queue URL or a queue name (resolved through `GetQueueUrl` on subscribe).
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use std::time::Duration;
/// use ruststream_sqs_sns::SqsQueue;
///
/// let source = SqsQueue::new("orders")
///     .wait(Duration::from_secs(20))
///     .batch(10)
///     .visibility(Duration::from_secs(30));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct SqsQueue {
    queue: String,
    wait: Duration,
    batch: i32,
    visibility: Option<Duration>,
    create_if_missing: bool,
}

impl SqsQueue {
    /// Names the queue by URL (`https://sqs...`) or by name.
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            wait: MAX_WAIT,
            batch: 10,
            visibility: None,
            create_if_missing: false,
        }
    }

    /// Long-polling wait per receive call. Defaults to the protocol maximum of 20 seconds;
    /// values above it are rejected before any I/O.
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Messages per receive call (1..=10, the protocol cap). Defaults to 10.
    pub fn batch(mut self, batch: i32) -> Self {
        self.batch = batch;
        self
    }

    /// The visibility timeout requested per receive; the crate extends it in the background
    /// while a handler holds the message. Defaults to the queue's configured timeout.
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = Some(visibility);
        self
    }

    /// Creates the queue on subscribe when it does not exist yet (a name ending in `.fifo`
    /// creates a FIFO queue with content-based deduplication). Meant for local development and
    /// tests; production queues are usually managed as infrastructure.
    pub fn create_if_missing(mut self) -> Self {
        self.create_if_missing = true;
        self
    }

    /// The queue URL or name this descriptor resolves.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub(crate) fn wait_value(&self) -> Duration {
        self.wait
    }

    pub(crate) fn batch_value(&self) -> i32 {
        self.batch
    }

    pub(crate) fn visibility_value(&self) -> Option<Duration> {
        self.visibility
    }

    pub(crate) fn create_value(&self) -> bool {
        self.create_if_missing
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), SqsError> {
        if self.queue.is_empty() {
            return Err(SqsError::InvalidQueue("queue must be non-empty".into()));
        }
        if self.wait > MAX_WAIT {
            return Err(SqsError::InvalidQueue(
                "wait exceeds the 20 second long-polling cap".into(),
            ));
        }
        if !(1..=10).contains(&self.batch) {
            return Err(SqsError::InvalidQueue(
                "batch must be within 1..=10 (the receive cap)".into(),
            ));
        }
        if let Some(visibility) = self.visibility
            && (visibility.is_zero() || visibility > Duration::from_hours(12))
        {
            return Err(SqsError::InvalidQueue(
                "visibility must be within 1s..=12h".into(),
            ));
        }
        Ok(())
    }
}

impl SubscriptionSource<ConnectedSqsBroker> for SqsQueue {
    type Subscriber = SqsSubscriber;

    fn name(&self) -> &str {
        self.queue()
    }

    async fn subscribe(self, connected: &ConnectedSqsBroker) -> Result<SqsSubscriber, SqsError> {
        connected.subscribe_queue(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_queue_is_rejected_before_io() {
        assert!(matches!(
            SqsQueue::new("").validate(),
            Err(SqsError::InvalidQueue(_))
        ));
    }

    #[test]
    fn overlong_wait_is_rejected_before_io() {
        assert!(matches!(
            SqsQueue::new("q").wait(Duration::from_secs(21)).validate(),
            Err(SqsError::InvalidQueue(_))
        ));
    }

    #[test]
    fn out_of_range_batch_is_rejected_before_io() {
        assert!(matches!(
            SqsQueue::new("q").batch(11).validate(),
            Err(SqsError::InvalidQueue(_))
        ));
        assert!(matches!(
            SqsQueue::new("q").batch(0).validate(),
            Err(SqsError::InvalidQueue(_))
        ));
    }
}
