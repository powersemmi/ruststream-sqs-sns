//! [`SnsPublisher`] (topic fan-out) and its policy [`SnsPublish`], behind the `sns` feature.
//!
//! A topic takes the same per-message settings as a queue and the same framing of headers and
//! payload, so everything here builds on the queue publisher's module and adds the topic.

// Without the `testing` feature the transport has one variant, so a `match` on it has a single
// arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::future::{Future, ready};

use aws_sdk_sns::primitives::Blob;
use aws_sdk_sns::types::MessageAttributeValue as SnsAttributeValue;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::{OutgoingFor, PairError, PublishPolicy, Publisher, Take};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::broker::{ConnectedSqsBroker, Core, CoreCell, Transport};
use crate::error::{SqsError, sdk_err};
#[cfg(feature = "testing")]
use crate::in_process;
use crate::message::{ENCODING_ATTRIBUTE, PARTITION_KEY_HEADER, encode_body, is_service_text};
use crate::publisher::{SqsPublishOptions, fifo_settings, is_fifo};

/// The binding version this crate writes for the `sns` protocol.
#[cfg(feature = "asyncapi")]
const SNS_BINDING_VERSION: &str = "1.0.0";

/// The `sns` channel binding a fan-out position writes: the topic the notifications leave for.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct SnsPublishChannel<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    ordering: Option<TopicOrdering>,
}

/// The specification's Ordering object, written only for a topic that has an order to report.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct TopicOrdering {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(rename = "contentBasedDeduplication")]
    content_based_deduplication: bool,
}

/// What a fan-out publish to `channel` adds to that channel in the generated `AsyncAPI`
/// document.
///
/// The topic is named the way the queue is: by the destination the mount site resolved. A
/// `.fifo` suffix is what makes a topic FIFO, so the ordering object appears exactly there; a
/// standard topic carries none, which is what the specification's default already says. The
/// deduplication flag reports what a position on this policy does rather than how the topic is
/// configured: every FIFO send carries a deduplication id of its own, and an explicit id wins
/// over one the topic would derive from the body.
#[cfg(feature = "asyncapi")]
fn sns_channel_binding(channel: &str) -> Bindings {
    let body = SnsPublishChannel {
        name: channel,
        ordering: is_fifo(channel).then_some(TopicOrdering {
            kind: "FIFO",
            content_based_deduplication: false,
        }),
    };
    Binding::new("sns", SNS_BINDING_VERSION, &body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// Publishes notifications to SNS topics for fan-out. Available with the `sns` feature.
///
/// The destination is a topic name or ARN. A name resolves through the idempotent
/// `CreateTopic`, mapped onto the alphabet SNS takes the way a queue name is, and a name ending
/// in `.fifo` opens a FIFO topic.
///
/// SNS appears only as a publisher: its delivery targets are queues and HTTP endpoints, not a
/// consumer this crate would own. Subscribe queues to the topic with
/// [`ConnectedSqsBroker::subscribe_queue_to_topic`](crate::ConnectedSqsBroker::subscribe_queue_to_topic),
/// which enables raw message delivery so payloads and headers arrive unwrapped.
///
/// A FIFO topic takes the same [`SqsPublishOptions`] as a FIFO queue, so a slot moved from
/// [`SqsPublish`] to [`SnsPublish`] keeps the handler body that names the steps.
///
/// [`SqsPublish`]: crate::SqsPublish
#[derive(Clone)]
pub struct SnsPublisher {
    cell: CoreCell,
    default_group: Option<String>,
    /// Whether a policy paired this publisher, which is when the test harness records what it
    /// publishes and asks the broker where each publish went.
    #[cfg(feature = "testing")]
    paired: bool,
    /// The queues subscribed to the topic outside the service, as the policy declared them.
    #[cfg(feature = "testing")]
    fan_out: Vec<String>,
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
            #[cfg(feature = "testing")]
            paired: false,
            #[cfg(feature = "testing")]
            fan_out: Vec::new(),
        }
    }

    /// The publisher the policy paired: the same connection, under the group the policy fixed.
    pub(crate) fn paired_by_policy(mut self, group: Option<String>) -> Self {
        self.default_group = group;
        #[cfg(feature = "testing")]
        {
            self.paired = true;
        }
        self
    }

    /// The queues the policy declared as subscribed to its topics outside the service.
    #[cfg(feature = "testing")]
    fn fanning_out_to(mut self, queues: Vec<String>) -> Self {
        self.fan_out = queues;
        self
    }

    /// Notes where a publish this paired publisher made went, for the test harness.
    #[cfg(feature = "testing")]
    fn note(&self, destination: &str) {
        if self.paired
            && let Some(core) = self.cell.get()
        {
            core.routing
                .published(destination, in_process::Surface::Topic);
        }
    }

    fn core(&self) -> Result<&Core, SqsError> {
        let core = self.cell.get().ok_or(SqsError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

impl Publisher for SnsPublisher {
    /// The SNS message is a `String` the client keeps for the request, like the SQS body.
    type Payload = Take;
    type Error = SqsError;
    type Options = SqsPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Take>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        #[cfg(feature = "testing")]
        let destination = msg.name();
        #[cfg(feature = "testing")]
        in_process::declare_fan_out(self.core()?, destination, &self.fan_out)?;
        let published = self.publish_to_topic(msg, options).await;
        #[cfg(feature = "testing")]
        if published.is_ok() {
            self.note(destination);
        }
        published
    }
}

impl SnsPublisher {
    /// `Publish` of one message to the topic it names.
    async fn publish_to_topic(
        &self,
        msg: OutgoingFor<'_, Take>,
        options: Option<&SqsPublishOptions>,
    ) -> Result<(), SqsError> {
        let core = self.core()?;
        let aws = match &core.transport {
            Transport::Aws(aws) => aws,
            #[cfg(feature = "testing")]
            Transport::InProcess(bus) => {
                return in_process::publish_topic(bus, msg, options, self.default_group.as_deref());
            }
        };
        let arn = aws.topic_arn(msg.name()).await?;
        // The destination is the caller's string and outlives the message the body is taken from.
        let (destination, payload, headers) = msg.into_parts();
        let (body, base64_marker) = encode_body(payload);

        let mut publish = aws.sns.publish().topic_arn(&arn).message(body);
        let mut partition_key = None;
        for (name, value) in headers.iter() {
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
            destination,
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
                destination: destination.to_owned(),
                source: sdk_err(&e),
            })
    }
}

/// The publish policy for [`SnsPublisher`]: names the SNS fan-out mode as a distinct policy
/// type, so direct queue publishing and topic fan-out never mix silently.
///
/// Available with the `sns` feature.
///
/// A reply names where it goes; the mount site names who takes it there, by binding the reply
/// position to this policy instead of the broker's default [`SqsPublish`]. The destination itself
/// reads the same on both policies: a name is a queue name under [`SqsPublish`] and a topic name
/// here, whether the reply type declares it or the mount site supplies it.
///
/// [`SqsPublish`]: crate::SqsPublish
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
///     Router::new().include(accept).out_reply(SnsPublish::default()).build()
/// }
/// # let _ = routes;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct SnsPublish {
    group_id: Option<String>,
    #[cfg(feature = "testing")]
    fan_out: Vec<String>,
}

impl SnsPublish {
    /// Orders everything this position publishes within `group`, unless a call names another.
    /// See [`SqsPublish::group_id`](crate::SqsPublish::group_id).
    pub fn group_id(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }

    /// Names queues subscribed to this position's topics outside the service, for the test
    /// harness. Available with the `testing` feature only; a production build has no such step.
    ///
    /// A queue the service subscribes itself, through
    /// [`subscribe_queue_to_topic`](crate::ConnectedSqsBroker::subscribe_queue_to_topic), is
    /// known already. One an operator subscribed in AWS is not, and naming it here makes a test
    /// see the whole group the topic delivers to: in process the account subscribes the queue to
    /// the topic before the publish, as the operator did, and a live test waits for that queue's
    /// subscription to handle the copy. The queues apply to every topic the position publishes
    /// to. A queue SNS would refuse (a FIFO queue on a standard topic) fails the publish.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqs_sns::prelude::*;
    ///
    /// // The topic fans out to `billing`, which the operator subscribed; production builds the
    /// // policy without the step, which exists only under `testing`.
    /// fn announcements() -> SnsPublish {
    ///     let policy = SnsPublish::default();
    ///     #[cfg(feature = "testing")]
    ///     let policy = policy.fans_out_to(["billing"]);
    ///     policy
    /// }
    /// # let _ = announcements;
    /// ```
    #[cfg(feature = "testing")]
    pub fn fans_out_to(mut self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.fan_out.extend(queues.into_iter().map(Into::into));
        self
    }
}

impl PublishPolicy<ConnectedSqsBroker> for SnsPublish {
    type Live = SnsPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        let publisher = connected.sns_publisher().paired_by_policy(self.group_id);
        #[cfg(feature = "testing")]
        let publisher = publisher.fanning_out_to(self.fan_out);
        ready(Ok(publisher))
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        sns_channel_binding(channel)
    }
}
