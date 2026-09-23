//! [`SqsTestBroker`]: the in-process transport and its connected form.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, BrokerMoves, ConnectedBroker, DeclareRetryError, DefaultPublish, HeaderMap,
    OutgoingFor, OutgoingMessage, Publisher, RawMessage, RetryDeclaration, Str, Subscribe, Take,
};

use crate::error::SqsError;
use crate::message::PARTITION_KEY_HEADER;
use crate::publisher::{fifo_settings, is_fifo};
use crate::queue::{Redrive, SqsQueue};
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::SqsTestSubscriber;
use crate::{SqsPublish, SqsPublishOptions};

/// Shared state of one in-process broker: the router, the harness coordinator, and whether the
/// transport has been shut down.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// The same runtime flag the real broker keeps, for the same reason: handles that alias the
    /// connection - a publisher handed out earlier, a clone of the connected form - outlive the
    /// consuming `shutdown` the ladder makes unrepresentable for the owner, and must report a
    /// dead transport rather than route into a cleared router.
    closed: AtomicBool,
    /// What registrations mounted by a bare queue name declared, held until `subscribe` opens
    /// that queue - the same wait the real broker's connection makes.
    declared_redrives: StdMutex<HashMap<String, Redrive>>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// `Ok` while the transport is live, [`SqsError::NotConnected`] once it has shut down -
    /// mirroring `Core::ensure_open` on the real broker, so a test sees the error a service
    /// would see.
    fn ensure_open(&self) -> Result<(), SqsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqsError::NotConnected);
        }
        Ok(())
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }

    /// Holds what a bare-name registration declared for `queue`, clearing the entry where it
    /// declared nothing.
    fn record_redrive(&self, queue: &str, redrive: Option<Redrive>) {
        let mut declared = self
            .declared_redrives
            .lock()
            .expect("sqs test declaration mutex poisoned");
        match redrive {
            Some(redrive) => drop(declared.insert(queue.to_owned(), redrive)),
            None => drop(declared.remove(queue)),
        }
    }

    /// The redrive policy a bare-name registration declared for `queue`.
    fn declared_redrive(&self, queue: &str) -> Option<Redrive> {
        self.declared_redrives
            .lock()
            .expect("sqs test declaration mutex poisoned")
            .get(queue)
            .cloned()
    }
}

/// An in-process stand-in for [`SqsBroker`](crate::SqsBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::testing::SqsTestBroker;
///
/// let broker = SqsTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct SqsTestBroker {
    state: Arc<TestState>,
}

impl SqsTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> SqsTestPublisher {
        SqsTestPublisher::new(Arc::clone(&self.state))
    }
}

impl Broker for SqsTestBroker {
    type Error = SqsError;
    type Connected = ConnectedSqsTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedSqsTestBroker { state: self.state }))
    }
}

/// The connected form of [`SqsTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedSqsTestBroker {
    state: Arc<TestState>,
}

impl ConnectedSqsTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> SqsTestPublisher {
        SqsTestPublisher::new(Arc::clone(&self.state))
    }

    /// Opens the subscription described by `queue`, carrying the registration's redrive policy
    /// onto it: the stand-in counts receives and moves a spent delivery to the dead-letter queue
    /// itself, as SQS does.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the registration declared half a redrive policy, or the
    /// transport has shut down.
    pub(crate) fn subscribe_queue(&self, queue: &SqsQueue) -> Result<SqsTestSubscriber, SqsError> {
        self.open(queue.queue(), queue.redrive()?)
    }

    /// Opens one subscription, or reports the closed transport. Split out so the [`Subscribe`]
    /// impl stays a `ready(..)`: the in-process transport never awaits.
    fn open(&self, name: &str, redrive: Option<Redrive>) -> Result<SqsTestSubscriber, SqsError> {
        self.state.ensure_open()?;
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(SqsTestSubscriber::new(
            name.to_owned(),
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            redrive,
            self.state.coordinator().cloned(),
        ))
    }
}

impl ConnectedBroker for ConnectedSqsTestBroker {
    type Error = SqsError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        // Flag before clearing, so a handle racing the teardown reports the closed transport
        // rather than finding an empty router and reporting success.
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedSqsTestBroker {
    type Subscriber = SqsTestSubscriber;
    // The same answer the real broker gives, so a mount chain that compiles here compiles
    // against SQS too.
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(self.open(name, self.state.declared_redrive(name)))
    }

    /// The real broker's answer, so a bare-name registration that starts here starts against
    /// SQS: the declaration becomes the queue's redrive policy, and half a declaration is
    /// refused with the missing half named.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareRetryError::Broker`] carrying
    /// [`SqsError::IncompleteRedrive`](crate::SqsError::IncompleteRedrive) when only one half
    /// was declared.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        let declared = SqsQueue::new(name)
            .with_declaration(declaration)
            .redrive()
            .map_err(|incomplete| DeclareRetryError::Broker(Box::new(incomplete)))?;
        self.state.record_redrive(name, declared);
        Ok(())
    }
}

impl TestableBroker for ConnectedSqsTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let (name, payload, headers) = message.into_parts();
        self.state
            .publish(name, Bytes::copy_from_slice(payload), headers);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedSqsTestBroker);

/// Publisher for the in-process broker: the live form both [`SqsPublish`] and
/// [`SnsPublish`](crate::SnsPublish) pair into here.
///
/// It carries the same surface a service uses on the real publishers, so code written against
/// [`SqsPublisher`](crate::SqsPublisher) compiles against the stand-in unchanged.
#[derive(Debug, Clone)]
pub struct SqsTestPublisher {
    state: Arc<TestState>,
    default_group: Option<String>,
}

impl SqsTestPublisher {
    fn new(state: Arc<TestState>) -> Self {
        Self {
            state,
            default_group: None,
        }
    }

    /// The publisher the policy paired: the same router, under the group the policy fixed.
    pub(crate) fn with_default_group(mut self, group: Option<String>) -> Self {
        self.default_group = group;
        self
    }

    /// Routes one message, or reports the closed transport. Split out so the [`Publisher`] impl
    /// stays a `ready(..)`: the in-process transport never awaits.
    ///
    /// The FIFO settings resolve exactly as they do on the real publishers, and the answer goes
    /// where SQS puts it: a `.fifo` destination delivers the resolved group in the
    /// `partition-key` header, and a standard destination delivers no such header at all,
    /// because SQS never carries it as an attribute. What the stand-in cannot show is the
    /// ordering the group buys - the router has no message groups, only the header.
    fn route(
        &self,
        msg: OutgoingFor<'_, Take>,
        options: Option<&SqsPublishOptions>,
    ) -> Result<(), SqsError> {
        self.state.ensure_open()?;
        let (destination, payload, mut headers) = msg.into_parts();
        let payload = payload.freeze();
        let partition_key = headers
            .remove(PARTITION_KEY_HEADER)
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        if let Some(settings) = fifo_settings(
            destination,
            is_fifo(destination),
            options,
            partition_key,
            self.default_group.as_deref(),
        )? {
            headers.insert(Str::from_static(PARTITION_KEY_HEADER), settings.group);
        }
        self.state.publish(destination, payload, headers);
        Ok(())
    }
}

impl Publisher for SqsTestPublisher {
    /// The real publishers' form: the in-process router keeps the payload as well.
    type Payload = Take;
    type Error = SqsError;
    type Options = SqsPublishOptions;

    fn publish(
        &self,
        msg: OutgoingFor<'_, Take>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route(msg, options))
    }
}

impl DefaultPublish for ConnectedSqsTestBroker {
    // The production policy, not a stand-in of it: a `publish("dest")` handler mounted here
    // replies through the same type it would in production.
    type Policy = SqsPublish;
}
