//! [`SqsPublisher`] (direct-to-queue) and [`SnsPublisher`] (topic fan-out), with their
//! policies, their per-message settings and the builder steps that name them.

use std::future::{Future, ready};

use aws_sdk_sns::primitives::Blob;
use aws_sdk_sns::types::MessageAttributeValue as SnsAttributeValue;
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedSqsBroker, Core, CoreCell};
use crate::error::{SqsError, sdk_err};
use crate::message::{
    ENCODING_ATTRIBUTE, PARTITION_KEY_HEADER, encode_attributes, encode_body, is_service_text,
};
#[cfg(feature = "testing")]
use crate::testing::{ConnectedSqsTestBroker, SqsTestPublisher};

/// The settings one publish may differ from the next in, on both SQS and SNS.
///
/// Both fields are FIFO settings: they reach the wire on a `.fifo` queue or topic and have no
/// meaning anywhere else, so naming one for a standard destination is a publish error rather
/// than a value quietly dropped. Every field is optional - what a call leaves alone keeps what
/// the mount site's [`SqsPublish`] fixed.
///
/// A call site fills it through the steps of [`SqsPublishSteps`], never by hand; the
/// constructors below are for a test that asserts on what a publish carried
/// (`tb.out::<Marker>().with_options(..)`).
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::SqsPublishOptions;
///
/// let expected = SqsPublishOptions::default().group_id("user-42");
/// assert_eq!(expected.group_id.as_deref(), Some("user-42"));
/// assert_eq!(expected.deduplication_id, None);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
#[must_use]
pub struct SqsPublishOptions {
    /// The FIFO message group id this message is ordered within.
    pub group_id: Option<String>,
    /// The FIFO deduplication id, the idempotency key of this send. Unset means the crate
    /// supplies a process-unique one, so two identical payloads never collapse into one.
    pub deduplication_id: Option<String>,
}

impl SqsPublishOptions {
    /// Orders this message within `group`.
    pub fn group_id(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }

    /// Deduplicates this message under `id` within the queue's five-minute window.
    pub fn deduplication_id(mut self, id: impl Into<String>) -> Self {
        self.deduplication_id = Some(id.into());
        self
    }
}

/// The per-message settings of this crate's publishers, on the publish builder.
///
/// The steps are the call site's half of [`SqsPublishOptions`]: they win over the group the
/// mount site's [`SqsPublish`] fixed, for that one message. The bound is on the sink's options
/// type, so they appear on a builder over an SQS or SNS publisher and on no other broker's.
///
/// The trait is in the [prelude](crate::prelude); a handler body that names a step is the one
/// place a body imports this crate's prelude instead of the framework's, and bounds its slot
/// `Out<impl Publisher<Options = SqsPublishOptions>, Marker>`.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Outgoing, Serialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Order)]
/// struct Shipments;
///
/// #[subscriber("orders")]
/// async fn ship(
///     order: &Order,
///     Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
/// ) -> HandlerOutcome {
///     // This one order ships in its customer's group, whatever the mount site's default is.
///     if shipments
///         .message(order)
///         .to("shipments.fifo")
///         .group_id(format!("customer-{}", order.id))
///         .publish()
///         .await
///         .is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # let _ = ship;
/// ```
pub trait SqsPublishSteps: Sized {
    /// Orders this one message within `group` (a `.fifo` destination only).
    #[must_use]
    fn group_id(self, group: impl Into<String>) -> Self;

    /// Deduplicates this one message under `id` (a `.fifo` destination only).
    #[must_use]
    fn deduplication_id(self, id: impl Into<String>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> SqsPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = SqsPublishOptions>,
{
    fn group_id(mut self, group: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(SqsPublishOptions::default)
            .group_id = Some(group.into());
        self
    }

    fn deduplication_id(mut self, id: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(SqsPublishOptions::default)
            .deduplication_id = Some(id.into());
        self
    }
}

/// Publishes messages directly to SQS queues (name or URL as the destination).
///
/// On a FIFO queue (a `.fifo` destination) every send carries a message group id and a
/// deduplication id: [`SqsPublishSteps`] names them per call, the mount site's [`SqsPublish`]
/// fixes the group for the whole position, and a message carrying the `partition-key` header
/// names its own group the portable way. Buildable before `connect` and usable until
/// `shutdown`; afterwards every publish reports [`SqsError::NotConnected`] instead of silently
/// succeeding.
#[derive(Clone)]
pub struct SqsPublisher {
    cell: CoreCell,
    default_group: Option<String>,
}

impl std::fmt::Debug for SqsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsPublisher").finish_non_exhaustive()
    }
}

impl SqsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self {
            cell,
            default_group: None,
        }
    }

    /// The publisher the policy paired: the same connection, under the group the policy fixed.
    pub(crate) fn with_default_group(mut self, group: Option<String>) -> Self {
        self.default_group = group;
        self
    }

    fn core(&self) -> Result<&Core, SqsError> {
        let core = self.cell.get().ok_or(SqsError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

/// Whether a destination names a FIFO resource. Kept case-insensitive to satisfy the
/// extension-comparison lint; AWS itself only accepts the lowercase suffix.
pub(crate) fn is_fifo(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".fifo")
}

/// A process-unique deduplication id: FIFO queues without content-based deduplication require
/// one per message, and an explicit id also wins over content-based deduplication, so two
/// legitimate identical payloads never collapse.
fn dedup_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "rs-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// The FIFO fields one send carries.
#[derive(Debug)]
pub(crate) struct FifoSettings {
    pub(crate) group: String,
    pub(crate) deduplication: String,
}

/// Resolves the FIFO settings of one publish, or refuses a destination that cannot honour them.
///
/// The ladder is specific to general: the call's own step, then the `partition-key` header the
/// message carries (the spelling that travels across brokers), then the group the mount site
/// fixed, then `"default"`, because FIFO rejects a send with no group at all.
///
/// Only this crate's own settings refuse a standard destination. A `partition-key` header is a
/// portable hint a service may set for every broker it publishes to, so a standard queue
/// ignores it the way it always has.
pub(crate) fn fifo_settings(
    destination: &str,
    fifo: bool,
    options: Option<&SqsPublishOptions>,
    partition_key: Option<String>,
    default_group: Option<&str>,
) -> Result<Option<FifoSettings>, SqsError> {
    let named_group = options
        .and_then(|options| options.group_id.clone())
        .or_else(|| default_group.map(ToOwned::to_owned));
    let named_deduplication = options.and_then(|options| options.deduplication_id.clone());
    if !fifo {
        if let Some(setting) = named_group
            .is_some()
            .then_some("a message group id")
            .or_else(|| {
                named_deduplication
                    .is_some()
                    .then_some("a deduplication id")
            })
        {
            return Err(SqsError::NotFifo {
                destination: destination.to_owned(),
                setting,
            });
        }
        return Ok(None);
    }
    Ok(Some(FifoSettings {
        group: options
            .and_then(|options| options.group_id.clone())
            .or(partition_key)
            .or_else(|| default_group.map(ToOwned::to_owned))
            .unwrap_or_else(|| "default".to_owned()),
        deduplication: named_deduplication.unwrap_or_else(dedup_id),
    }))
}

impl Publisher for SqsPublisher {
    type Error = SqsError;
    type Options = SqsPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        let url = core.queue_url(msg.name()).await?;
        let (body, base64_marker) = encode_body(msg.payload());
        let (attributes, partition_key) = encode_attributes(msg.headers(), base64_marker);
        let fifo = is_fifo(msg.name()) || is_fifo(&url);
        let settings = fifo_settings(
            msg.name(),
            fifo,
            options,
            partition_key,
            self.default_group.as_deref(),
        )?;

        let mut send = core.sqs.send_message().queue_url(&url).message_body(body);
        if !attributes.is_empty() {
            send = send.set_message_attributes(Some(attributes));
        }
        if let Some(settings) = settings {
            send = send
                .message_group_id(settings.group)
                .message_deduplication_id(settings.deduplication);
        }
        send.send()
            .await
            .map(|_| ())
            .map_err(|e| SqsError::Publish {
                destination: msg.name().to_owned(),
                source: sdk_err(&e),
            })
    }
}

/// The publish policy for [`SqsPublisher`]: pure declaration, constructible anywhere, paired
/// with the connected broker by the runtime after `connect`.
///
/// It is also the broker's [`DefaultPublish`](ruststream::DefaultPublish) policy, so a replying
/// handler whose mount binds no reply position of its own replies through it, and
/// `.out(Reply, SqsPublish)` only ever restates the default. [`SnsPublish`] is the step that
/// changes the answer.
///
/// The group is the one FIFO setting a policy fixes, because it belongs to a position: every
/// message a slot publishes is ordered within the same group. A deduplication id is the
/// idempotency key of one message, and a constant one would collapse a position's whole output
/// into a single delivery, so it is named per call or left to the crate.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::SqsPublish;
///
/// // Everything this position publishes is ordered within one group.
/// let policy = SqsPublish::default().group_id("orders");
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct SqsPublish {
    group_id: Option<String>,
}

impl SqsPublish {
    /// Orders everything this position publishes within `group`, unless a call names another.
    pub fn group_id(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }
}

impl PublishPolicy<ConnectedSqsBroker> for SqsPublish {
    type Live = SqsPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher().with_default_group(self.group_id)))
    }
}

/// The same policy against the in-process stand-in, so a routes file's `.out(Reply, Publish)`
/// mounts on [`SqsTestBroker`](crate::testing::SqsTestBroker) as written. It is the stand-in's
/// [`DefaultPublish`](ruststream::DefaultPublish) policy too, so a `publish("dest")` handler
/// that binds nothing replies through it there as well.
#[cfg(feature = "testing")]
impl PublishPolicy<ConnectedSqsTestBroker> for SqsPublish {
    type Live = SqsTestPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher().with_default_group(self.group_id)))
    }
}

/// Publishes notifications to SNS topics for fan-out (the destination is a topic name or
/// ARN; names resolve through the idempotent `CreateTopic`).
///
/// SNS appears only as a publisher: its delivery targets are queues and HTTP endpoints, not a
/// consumer this crate would own. Subscribe queues to the topic with
/// [`ConnectedSqsBroker::subscribe_queue_to_topic`](crate::ConnectedSqsBroker::subscribe_queue_to_topic),
/// which enables raw message delivery so payloads and headers arrive unwrapped.
///
/// A FIFO topic takes the same [`SqsPublishOptions`] as a FIFO queue, so a slot moved from
/// [`SqsPublish`] to [`SnsPublish`] keeps the handler body that names the steps.
#[derive(Clone)]
pub struct SnsPublisher {
    cell: CoreCell,
    default_group: Option<String>,
}

impl std::fmt::Debug for SnsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnsPublisher").finish_non_exhaustive()
    }
}

impl SnsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self {
            cell,
            default_group: None,
        }
    }

    /// The publisher the policy paired: the same connection, under the group the policy fixed.
    pub(crate) fn with_default_group(mut self, group: Option<String>) -> Self {
        self.default_group = group;
        self
    }

    fn core(&self) -> Result<&Core, SqsError> {
        let core = self.cell.get().ok_or(SqsError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

impl Publisher for SnsPublisher {
    type Error = SqsError;
    type Options = SqsPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        let arn = core.topic_arn(msg.name()).await?;
        let (body, base64_marker) = encode_body(msg.payload());

        let mut publish = core.sns.publish().topic_arn(&arn).message(body);
        let mut partition_key = None;
        for (name, value) in msg.headers().iter() {
            if name == PARTITION_KEY_HEADER {
                partition_key = Some(String::from_utf8_lossy(value).into_owned());
                continue;
            }
            // The same split the SQS side makes, for the same reason: a value the service
            // refuses as text travels as binary rather than being mangled or rejected. A
            // `HeaderMap` value is bytes on both sides, so a subscriber reads back what was
            // written either way.
            let attribute = match std::str::from_utf8(value) {
                Ok(text) if is_service_text(text) => SnsAttributeValue::builder()
                    .data_type("String")
                    .string_value(text)
                    .build(),
                _ => SnsAttributeValue::builder()
                    .data_type("Binary")
                    .binary_value(Blob::new(value))
                    .build(),
            };
            if let Ok(attribute) = attribute {
                publish = publish.message_attributes(name, attribute);
            }
        }
        if base64_marker
            && let Ok(marker) = SnsAttributeValue::builder()
                .data_type("String")
                .string_value("base64")
                .build()
        {
            publish = publish.message_attributes(ENCODING_ATTRIBUTE, marker);
        }
        if let Some(settings) = fifo_settings(
            msg.name(),
            is_fifo(&arn),
            options,
            partition_key,
            self.default_group.as_deref(),
        )? {
            publish = publish
                .message_group_id(settings.group)
                .message_deduplication_id(settings.deduplication);
        }
        publish
            .send()
            .await
            .map(|_| ())
            .map_err(|e| SqsError::Publish {
                destination: msg.name().to_owned(),
                source: sdk_err(&e),
            })
    }
}

/// The publish policy for [`SnsPublisher`]: names the SNS fan-out mode as a distinct policy
/// type, so direct queue publishing and topic fan-out never mix silently.
///
/// A reply names where it goes; the mount site names who takes it there, by binding the reply
/// position to this policy instead of the broker's default [`SqsPublish`]. The destination itself
/// reads the same on both policies: a name is a queue name under [`SqsPublish`] and a topic name
/// here, whether the reply type declares it or the mount site supplies it.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// // The reply type declares no destination of its own, so it takes the one the clause names.
/// #[derive(Serialize, Outgoing)]
/// struct OrderPlaced {
///     id: u64,
/// }
///
/// #[subscriber("orders", publish("orders-events"))]
/// async fn accept(order: &Order) -> OrderPlaced {
///     OrderPlaced { id: order.id }
/// }
///
/// // Without the step the reply would ride `SqsPublish` and land on a queue named
/// // `orders-events`; with it the same reply fans out from the topic of that name.
/// fn routes() -> impl RouterDef<SqsBroker> {
///     Router::new().include(accept).out(Reply, SnsPublish::default()).build()
/// }
/// # let _ = routes;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct SnsPublish {
    group_id: Option<String>,
}

impl SnsPublish {
    /// Orders everything this position publishes within `group`, unless a call names another.
    /// See [`SqsPublish::group_id`].
    pub fn group_id(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }
}

impl PublishPolicy<ConnectedSqsBroker> for SnsPublish {
    type Live = SnsPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected
            .sns_publisher()
            .with_default_group(self.group_id)))
    }
}

/// Fan-out against the in-process stand-in, so `.out(Reply, SnsPublish::default())` mounts
/// there as written.
///
/// Both policies pair into the one [`SqsTestPublisher`], because the router has no topic to
/// fan out from: a message reaches the subscriptions on the destination it names, whichever
/// policy carried it. So a test here proves the reply took the destination the SNS policy names,
/// not that SNS delivered it onward to the queues subscribed to that topic - that is
/// `subscribe_queue_to_topic`'s job and the live suite asserts it.
#[cfg(feature = "testing")]
impl PublishPolicy<ConnectedSqsTestBroker> for SnsPublish {
    type Live = SqsTestPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher().with_default_group(self.group_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The FIFO fields one publish would send, or the refusal it would report.
    fn settings(
        destination: &str,
        options: Option<&SqsPublishOptions>,
        partition_key: Option<&str>,
        default_group: Option<&str>,
    ) -> Result<Option<FifoSettings>, SqsError> {
        fifo_settings(
            destination,
            is_fifo(destination),
            options,
            partition_key.map(ToOwned::to_owned),
            default_group,
        )
    }

    #[test]
    fn a_call_step_wins_over_the_message_key_and_the_mount_site() {
        let options = SqsPublishOptions::default().group_id("call");
        let resolved = settings("orders.fifo", Some(&options), Some("header"), Some("mount"))
            .expect("a fifo destination takes every group")
            .expect("a fifo destination carries a group");
        assert_eq!(resolved.group, "call");
    }

    #[test]
    fn the_message_key_wins_over_the_mount_site() {
        let resolved = settings("orders.fifo", None, Some("header"), Some("mount"))
            .expect("a fifo destination takes every group")
            .expect("a fifo destination carries a group");
        assert_eq!(resolved.group, "header");
    }

    #[test]
    fn the_mount_site_group_holds_when_nothing_else_names_one() {
        let resolved = settings("orders.fifo", None, None, Some("mount"))
            .expect("a fifo destination takes every group")
            .expect("a fifo destination carries a group");
        assert_eq!(resolved.group, "mount");
    }

    #[test]
    fn a_fifo_send_that_names_no_group_still_carries_one() {
        let resolved = settings("orders.fifo", None, None, None)
            .expect("a fifo destination takes every group")
            .expect("a fifo destination carries a group");
        assert_eq!(resolved.group, "default");
        assert!(resolved.deduplication.starts_with("rs-"));
    }

    #[test]
    fn an_explicit_deduplication_id_replaces_the_generated_one() {
        let options = SqsPublishOptions::default().deduplication_id("order-42");
        let resolved = settings("orders.fifo", Some(&options), None, None)
            .expect("a fifo destination takes every setting")
            .expect("a fifo destination carries a group");
        assert_eq!(resolved.deduplication, "order-42");
    }

    #[test]
    fn a_standard_queue_refuses_a_setting_it_cannot_honour() {
        let options = SqsPublishOptions::default().group_id("call");
        let refused = settings("orders", Some(&options), None, None)
            .expect_err("a standard queue cannot order a group");
        assert!(matches!(refused, SqsError::NotFifo { .. }));

        let refused = settings("orders", None, None, Some("mount"))
            .expect_err("a mount-site group is just as unhonourable there");
        assert!(matches!(refused, SqsError::NotFifo { .. }));
    }

    #[test]
    fn a_standard_queue_still_ignores_a_portable_partition_key() {
        let resolved =
            settings("orders", None, Some("header"), None).expect("a portable hint is not an ask");
        assert!(resolved.is_none());
    }
}
