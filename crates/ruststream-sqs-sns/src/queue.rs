//! [`SqsQueue`]: the subscription descriptor, and [`SqsSubscription`]: its mount-site spelling.
//!
//! The polling parameters that decide cost and latency are explicit: `wait` (long polling) and
//! `visibility` (the redelivery timeout the crate keeps extending while a handler holds a
//! message). How many messages one receive call asks for is not among them - that is the batch
//! size, which a batch handler names at the mount site with `batch(n)` and the subscriber maps
//! onto `MaxNumberOfMessages`.

use std::borrow::Cow;
use std::num::NonZeroU32;
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};
use ruststream::{BrokerMoves, FromName, RetryDeclaration, SubscriptionSource};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::broker::ConnectedSqsBroker;
use crate::error::SqsError;
#[cfg(feature = "asyncapi")]
use crate::publisher::is_fifo;
use crate::subscriber::SqsSubscriber;

/// The protocol cap on long polling.
const MAX_WAIT: Duration = Duration::from_secs(20);

/// The registration's retry declaration in the queue's own vocabulary.
///
/// A redrive policy is one setting with two halves: after `max_receive_count` receives SQS moves
/// the delivery to `dead_letter` on its own. Holding them in one value is what keeps half a
/// policy off the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Redrive {
    pub(crate) max_receive_count: NonZeroU32,
    pub(crate) dead_letter: String,
}

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
/// where they go when the registration names a batch size first.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct SqsQueue {
    queue: String,
    wait: Duration,
    visibility: Option<Duration>,
    create_if_missing: bool,
    max_attempts: Option<NonZeroU32>,
    dead_letter: Option<Cow<'static, str>>,
}

impl SqsQueue {
    /// Names the queue by URL (`https://sqs...`) or by name.
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            wait: MAX_WAIT,
            visibility: None,
            create_if_missing: false,
            max_attempts: None,
            dead_letter: None,
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

    /// Records what a registration declared, in the descriptor's own fields.
    ///
    /// Every mount path goes through this one mapping: the descriptor's own `declare_retry`, and
    /// the bare-name path, where the broker maps the same declaration onto the queue the name
    /// opens. A single mapping is what keeps the two spellings from drifting apart.
    pub(crate) fn with_declaration(mut self, declaration: &RetryDeclaration) -> Self {
        self.max_attempts = declaration.max_attempts();
        self.dead_letter = declaration
            .dead_letter()
            .map(|destination| Cow::Owned(destination.to_owned()));
        self
    }

    /// Puts a redrive policy the broker already holds back onto a descriptor.
    ///
    /// The bare-name path needs it: the declaration reaches the broker before there is a
    /// descriptor to take it, and `subscribe` builds one from the name alone.
    pub(crate) fn with_redrive(mut self, redrive: Option<Redrive>) -> Self {
        if let Some(redrive) = redrive {
            self.max_attempts = Some(redrive.max_receive_count);
            self.dead_letter = Some(Cow::Owned(redrive.dead_letter));
        }
        self
    }

    /// The redrive policy the registration declared, or nothing where it declared nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::IncompleteRedrive`] when only one half was declared.
    pub(crate) fn redrive(&self) -> Result<Option<Redrive>, SqsError> {
        match (self.max_attempts, self.dead_letter.as_deref()) {
            (Some(max_receive_count), Some(dead_letter)) => Ok(Some(Redrive {
                max_receive_count,
                dead_letter: dead_letter.to_owned(),
            })),
            (None, None) => Ok(None),
            (Some(_), None) => Err(SqsError::IncompleteRedrive {
                queue: self.queue.clone(),
                declared: "max_attempts(..)",
                missing: "dead_letter(..)",
            }),
            (None, Some(_)) => Err(SqsError::IncompleteRedrive {
                queue: self.queue.clone(),
                declared: "dead_letter(..)",
                missing: "max_attempts(..)",
            }),
        }
    }

    /// What this subscription adds to its channel in the generated `AsyncAPI` document.
    ///
    /// Everything here is read off the descriptor, because the document is built before anything
    /// connects: the queue's name, whether it is FIFO (the `.fifo` suffix says so) and the
    /// polling settings the descriptor names. A queue's ARN, its retention period and the
    /// timeout it carries when the descriptor names none are the service's to tell, and stay
    /// out.
    #[cfg(feature = "asyncapi")]
    fn channel_binding(&self) -> Bindings {
        let body = SqsChannel {
            queue: QueueObject {
                name: &self.queue,
                fifo_queue: is_fifo(&self.queue),
                visibility_timeout: self.visibility.map(|visibility| visibility.as_secs()),
                receive_message_wait_time: self.wait.as_secs(),
            },
        };
        // A binding that fails to build is a binding the document goes without: a broker never
        // holds up a service over a description of itself.
        Binding::new("sqs", SQS_BINDING_VERSION, &body)
            .map(|binding| Bindings::new().with(binding))
            .unwrap_or_default()
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
        // The declaration arrives as data at startup, so its two halves are checked here rather
        // than at the mount site: what the chain offers is the core's, one step per half, and
        // the pairing a redrive policy needs is this broker's alone.
        self.redrive()?;
        Ok(())
    }
}

/// The binding version this crate writes for the `sqs` protocol, on both sides of a channel.
#[cfg(feature = "asyncapi")]
pub(crate) const SQS_BINDING_VERSION: &str = "0.3.0";

/// The `sqs` channel binding: what a reader of the document learns about the queue behind this
/// channel.
///
/// The specification's `deadLetterQueue` and the queue's `redrivePolicy` are absent, and not for
/// want of the values: the framework reads a descriptor's bindings where the handler is included,
/// which is before the registration declares its cap and its destination. It reports the
/// declaration itself on the operation instead, and the dead-letter queue as a channel the
/// registration sends to.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct SqsChannel<'a> {
    queue: QueueObject<'a>,
}

/// The specification's Queue object. Only the fields a descriptor can answer without a
/// connection are here; a queue's ARN, its retention period and its access policy are not among
/// them, and the last of those is infrastructure rather than a description of a service.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct QueueObject<'a> {
    name: &'a str,
    #[serde(rename = "fifoQueue")]
    fifo_queue: bool,
    #[serde(rename = "visibilityTimeout", skip_serializing_if = "Option::is_none")]
    visibility_timeout: Option<u64>,
    #[serde(rename = "receiveMessageWaitTime")]
    receive_message_wait_time: u64,
}

/// A queue is named and nothing more, so a definition may fix the kind and leave the name to the
/// mount site: `#[subscriber(SqsQueue)]` on the handler, `.name("orders")` where it is included.
/// Every polling option keeps its default there, and the mount-site steps of [`SqsSubscription`]
/// change them.
impl FromName for SqsQueue {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name.into())
    }
}

impl SubscriptionSource<ConnectedSqsBroker> for SqsQueue {
    type Subscriber = SqsSubscriber;
    // The queue carries a spent delivery away itself. Its redrive policy counts the receives and
    // moves the message to the dead-letter queue once they run out, so this process publishes no
    // copy and `.out_retry(..)` does not compile on this descriptor.
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        self.queue()
    }

    async fn subscribe(self, connected: &ConnectedSqsBroker) -> Result<SqsSubscriber, SqsError> {
        connected.subscribe_queue(self).await
    }

    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        // Recorded only: the redrive policy is written on the queue, and there is no connection
        // here to write it through. `subscribe` applies it.
        self.with_declaration(declaration)
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.channel_binding()
    }
}

/// The queue options in mount-site spelling, for a registration whose source is an
/// [`SqsQueue`].
///
/// The framework's own steps come first - the name builds the source, and `batch(n)` names the
/// batch size - and these chain after them, in this crate's vocabulary. The bound on the source
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

    /// The descriptor as it reaches `subscribe`: what the registration declared is on it.
    fn declared(declaration: &RetryDeclaration) -> SqsQueue {
        SubscriptionSource::<ConnectedSqsBroker>::declare_retry(
            SqsQueue::new("orders"),
            declaration,
        )
    }

    /// A cap of `attempts`, as a mount site declares it.
    fn cap(attempts: u32) -> NonZeroU32 {
        NonZeroU32::new(attempts).expect("a cap is never zero")
    }

    /// The declaration reaches the queue as one policy, so both halves have to be there.
    #[test]
    fn a_full_declaration_becomes_the_queues_redrive_policy() {
        let redrive = declared(
            &RetryDeclaration::new()
                .with_max_attempts(cap(4))
                .with_dead_letter("orders-dead"),
        )
        .redrive()
        .expect("both halves are declared")
        .expect("a full declaration is a policy");
        assert_eq!(redrive.max_receive_count.get(), 4);
        assert_eq!(redrive.dead_letter, "orders-dead");
    }

    #[test]
    fn a_registration_that_declares_nothing_writes_no_policy() {
        let written = declared(&RetryDeclaration::new()).redrive();
        assert!(
            matches!(written, Ok(None)),
            "a silent registration writes nothing"
        );
    }

    /// Half a policy is not one, and the subscription says which half is missing rather than
    /// running with a cap the queue never received.
    #[test]
    fn half_a_declaration_is_refused_before_io() {
        let refused = declared(&RetryDeclaration::new().with_max_attempts(cap(4)))
            .validate()
            .expect_err("a cap alone is not a redrive policy");
        assert!(
            matches!(
                refused,
                SqsError::IncompleteRedrive { missing, .. } if missing == "dead_letter(..)"
            ),
            "the refusal names the missing half",
        );

        let refused = declared(&RetryDeclaration::new().with_dead_letter("orders-dead"))
            .validate()
            .expect_err("a destination alone is not a redrive policy");
        assert!(
            matches!(
                refused,
                SqsError::IncompleteRedrive { missing, .. } if missing == "max_attempts(..)"
            ),
            "the refusal names the missing half",
        );
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
