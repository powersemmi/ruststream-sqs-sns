//! [`SqsTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, HeaderMap, OutgoingMessage, Publisher, RawMessage,
    Subscribe,
};

use crate::SqsPublish;
use crate::error::SqsError;
use crate::publisher::group_headers;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::SqsTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
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
}

impl ConnectedBroker for ConnectedSqsTestBroker {
    type Error = SqsError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedSqsTestBroker {
    type Subscriber = SqsTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        ready(Ok(SqsTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        )))
    }
}

impl TestableBroker for ConnectedSqsTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
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
    base: Option<HeaderMap>,
}

impl SqsTestPublisher {
    fn new(state: Arc<TestState>) -> Self {
        Self { state, base: None }
    }

    /// Returns a handle whose sends carry `group` as the FIFO message group id, mirroring
    /// [`SqsPublisher::with_group_id`](crate::SqsPublisher::with_group_id).
    ///
    /// The group travels as a base `partition-key` header, exactly as it does on the real
    /// publishers, so it reaches the handler as that header and a message naming the header
    /// itself still wins. What the stand-in cannot show is the ordering the group buys: the
    /// router has no message groups, only the header.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Outgoing, Serialized};
    /// use ruststream_sqs_sns::testing::SqsTestBroker;
    ///
    /// // The order is already encoded, so it names itself serialized and leaves byte for byte.
    /// #[derive(Outgoing, Serialized)]
    /// struct Order(Vec<u8>);
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// SqsTestBroker::new()
    ///     .publisher()
    ///     .with_group_id("user-42")
    ///     .message(&Order(br#"{"id":1}"#.to_vec()))
    ///     .to("orders.fifo")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_group_id(&self, group: impl Into<String>) -> Self {
        Self {
            state: Arc::clone(&self.state),
            base: Some(group_headers(group)),
        }
    }
}

impl Publisher for SqsTestPublisher {
    type Error = SqsError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.base.as_ref()
    }
}

impl DefaultPublish for ConnectedSqsTestBroker {
    // The production policy, not a stand-in of it: a `publish("dest")` handler mounted here
    // replies through the same type it would in production.
    type Policy = SqsPublish;
}
