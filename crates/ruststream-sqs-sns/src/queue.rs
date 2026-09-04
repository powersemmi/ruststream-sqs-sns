//! [`SqsQueue`]: the subscription descriptor, and [`SqsSubscription`]: its mount-site spelling.
//!
//! The polling parameters that decide cost and latency are explicit: `wait` (long polling) and
//! `visibility` (the redelivery timeout the crate keeps extending while a handler holds a
//! message). How many messages one receive call asks for is not among them - that is the page
//! size, which a page handler names at the mount site with `batch(n)` and the subscriber maps
//! onto `MaxNumberOfMessages`.

use std::time::Duration;

use ruststream::SubscriptionSource;
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

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
///     .visibility(Duration::from_secs(30));
/// # let _ = source;
/// ```
///
/// The same options are also reachable at the mount site through [`SqsSubscription`], which is
/// where they go when the registration names a page size first.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct SqsQueue {
    queue: String,
    wait: Duration,
    visibility: Option<Duration>,
    create_if_missing: bool,
}

impl SqsQueue {
    /// Names the queue by URL (`https://sqs...`) or by name.
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            wait: MAX_WAIT,
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

/// The queue options in mount-site spelling, for a registration whose source is an
/// [`SqsQueue`].
///
/// The framework's own steps come first - the name builds the source, and `batch(n)` names the
/// page size - and these chain after them, in this crate's vocabulary. The bound on the source
/// type is what keeps them off a builder for another broker.
///
/// The trait is in the [prelude](crate::prelude); a file that does not glob it imports the
/// trait to reach the methods, as with any extension trait.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_sqs_sns::prelude::*;
/// # #[derive(Deserialized)]
/// # struct Order<'a>(&'a [u8]);
///
/// #[subscriber(SqsQueue::new("orders"))]
/// async fn reconcile(orders: &[Order<'_>]) -> HandlerOutcome {
///     let _ = orders.len();
///     HandlerOutcome::ack()
/// }
///
/// # fn wire() {
/// let _mountable = reconcile.batch(nonzero!(6)).wait(Duration::from_secs(20));
/// # }
/// ```
pub trait SqsSubscription: Sized {
    /// Long-polling wait per receive call. See [`SqsQueue::wait`].
    #[must_use]
    fn wait(self, wait: Duration) -> Self;

    /// The visibility timeout requested per receive. See [`SqsQueue::visibility`].
    #[must_use]
    fn visibility(self, visibility: Duration) -> Self;

    /// Creates the queue on subscribe when it is missing. See [`SqsQueue::create_if_missing`].
    #[must_use]
    fn create_if_missing(self) -> Self;
}

impl<Def, State, DefCodec> SqsSubscription for SubscriberBuilder<Def, SqsQueue, State, DefCodec>
where
    Def: Declared,
{
    fn wait(self, wait: Duration) -> Self {
        self.map_source(|source| source.wait(wait))
    }

    fn visibility(self, visibility: Duration) -> Self {
        self.map_source(|source| source.visibility(visibility))
    }

    fn create_if_missing(self) -> Self {
        self.map_source(SqsQueue::create_if_missing)
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
    fn out_of_range_visibility_is_rejected_before_io() {
        assert!(matches!(
            SqsQueue::new("q").visibility(Duration::ZERO).validate(),
            Err(SqsError::InvalidQueue(_))
        ));
        assert!(matches!(
            SqsQueue::new("q")
                .visibility(Duration::from_hours(13))
                .validate(),
            Err(SqsError::InvalidQueue(_))
        ));
    }
}
