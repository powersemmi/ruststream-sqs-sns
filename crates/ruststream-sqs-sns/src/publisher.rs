//! [`SqsPublisher`] (direct-to-queue) and [`SnsPublisher`] (topic fan-out), with their
//! policies.

use std::future::{Future, ready};
use std::sync::Arc;

use aws_sdk_sns::types::MessageAttributeValue as SnsAttributeValue;
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedSqsBroker, Core, CoreCell};
use crate::error::{SqsError, sdk_err};
use crate::message::{ENCODING_ATTRIBUTE, PARTITION_KEY_HEADER, encode_attributes, encode_body};

/// Publishes messages directly to SQS queues (name or URL as the destination).
///
/// On a FIFO queue (a `.fifo` destination) the `partition-key` header becomes the message
/// group id (`"default"` when absent, since FIFO requires one) and a unique deduplication id
/// is supplied per send. Buildable before `connect` and usable until `shutdown`; afterwards
/// every publish reports [`SqsError::NotConnected`] instead of silently succeeding.
#[derive(Clone)]
pub struct SqsPublisher {
    cell: CoreCell,
    base: Option<HeaderMap>,
}

impl std::fmt::Debug for SqsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsPublisher").finish_non_exhaustive()
    }
}

impl SqsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell, base: None }
    }

    /// Returns a handle whose sends carry `group` as the FIFO message group id.
    ///
    /// The handle aliases the same connection; only the group differs. It carries the group as a
    /// base `partition-key` header, so a message that names `partition-key` itself wins.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Outgoing, Serialized};
    /// use ruststream_sqs_sns::SqsBroker;
    ///
    /// // The order is already encoded, so it names itself serialized and leaves byte for byte.
    /// #[derive(Outgoing, Serialized)]
    /// struct Order(Vec<u8>);
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let publisher = SqsBroker::new().publisher();
    /// publisher
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
            cell: Arc::clone(&self.cell),
            base: Some(group_headers(group)),
        }
    }

    fn core(&self) -> Result<&Core, SqsError> {
        let core = self.cell.get().ok_or(SqsError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

/// Whether a destination names a FIFO resource. Kept case-insensitive to satisfy the
/// extension-comparison lint; AWS itself only accepts the lowercase suffix.
fn is_fifo(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".fifo")
}

/// The one-entry base map a group-carrying handle publishes under.
fn group_headers(group: impl Into<String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, group.into());
    headers
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

impl Publisher for SqsPublisher {
    type Error = SqsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let url = core.queue_url(msg.name()).await?;
        let (body, base64_marker) = encode_body(msg.payload());
        let (attributes, group) = encode_attributes(msg.headers(), base64_marker);

        let mut send = core.sqs.send_message().queue_url(&url).message_body(body);
        if !attributes.is_empty() {
            send = send.set_message_attributes(Some(attributes));
        }
        if is_fifo(msg.name()) || is_fifo(&url) {
            // FIFO rejects a send with no group, so the literal closes the ladder.
            send = send
                .message_group_id(group.unwrap_or_else(|| "default".to_owned()))
                .message_deduplication_id(dedup_id());
        }
        send.send()
            .await
            .map(|_| ())
            .map_err(|e| SqsError::Publish {
                destination: msg.name().to_owned(),
                source: sdk_err(&e),
            })
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.base.as_ref()
    }
}

/// The publish policy for [`SqsPublisher`]: pure declaration, constructible anywhere, paired
/// with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::SqsPublish;
///
/// let policy = SqsPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct SqsPublish;

impl PublishPolicy<ConnectedSqsBroker> for SqsPublish {
    type Live = SqsPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

/// Publishes notifications to SNS topics for fan-out (the destination is a topic name or
/// ARN; names resolve through the idempotent `CreateTopic`).
///
/// SNS appears only as a publisher: its delivery targets are queues and HTTP endpoints, not a
/// consumer this crate would own. Subscribe queues to the topic with
/// [`ConnectedSqsBroker::subscribe_queue_to_topic`](crate::ConnectedSqsBroker::subscribe_queue_to_topic),
/// which enables raw message delivery so payloads and headers arrive unwrapped.
#[derive(Clone)]
pub struct SnsPublisher {
    cell: CoreCell,
    base: Option<HeaderMap>,
}

impl std::fmt::Debug for SnsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnsPublisher").finish_non_exhaustive()
    }
}

impl SnsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell, base: None }
    }

    /// Returns a handle whose sends carry `group` as the FIFO message group id, for a FIFO
    /// topic.
    ///
    /// The group travels as a base `partition-key` header, so a message that names
    /// `partition-key` itself wins. See [`SqsPublisher::with_group_id`].
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Outgoing, Serialized};
    ///
    /// // The notice is already a wire payload, so it names itself serialized and no codec
    /// // runs on it.
    /// #[derive(Outgoing, Serialized)]
    /// struct Notice(Vec<u8>);
    ///
    /// # async fn demo(broker: ruststream_sqs_sns::ConnectedSqsBroker)
    /// # -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// broker
    ///     .sns_publisher()
    ///     .with_group_id("user-42")
    ///     .message(&Notice(b"shipped".to_vec()))
    ///     .to("orders.fifo")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_group_id(&self, group: impl Into<String>) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
            base: Some(group_headers(group)),
        }
    }

    fn core(&self) -> Result<&Core, SqsError> {
        let core = self.cell.get().ok_or(SqsError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

impl Publisher for SnsPublisher {
    type Error = SqsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let arn = core.topic_arn(msg.name()).await?;
        let (body, base64_marker) = encode_body(msg.payload());

        let mut publish = core.sns.publish().topic_arn(&arn).message(body);
        let mut group = None;
        for (name, value) in msg.headers().iter() {
            let text = String::from_utf8_lossy(value).into_owned();
            if name == PARTITION_KEY_HEADER {
                group = Some(text);
                continue;
            }
            let attribute = SnsAttributeValue::builder()
                .data_type("String")
                .string_value(text)
                .build();
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
        if is_fifo(&arn) {
            // FIFO rejects a send with no group, so the literal closes the ladder.
            publish = publish
                .message_group_id(group.unwrap_or_else(|| "default".to_owned()))
                .message_deduplication_id(dedup_id());
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

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.base.as_ref()
    }
}

/// The publish policy for [`SnsPublisher`]: names the SNS fan-out mode as a distinct policy
/// type, so direct queue publishing and topic fan-out never mix silently.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::SnsPublish;
///
/// let policy = SnsPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct SnsPublish;

impl PublishPolicy<ConnectedSqsBroker> for SnsPublish {
    type Live = SnsPublisher;

    fn pair(
        self,
        connected: &ConnectedSqsBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.sns_publisher()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::SqsBroker;

    #[test]
    fn a_group_id_handle_publishes_under_a_base_partition_key() {
        let publisher = SqsBroker::new().publisher().with_group_id("user-42");
        let base = publisher.base_headers().expect("the handle carries a base");
        assert_eq!(base.get_str(PARTITION_KEY_HEADER), Some("user-42"));
        assert_eq!(base.len(), 1);
    }

    #[test]
    fn the_sns_handle_carries_the_same_base() {
        let publisher = SnsPublisher::new(CoreCell::default()).with_group_id("user-42");
        let base = publisher.base_headers().expect("the handle carries a base");
        assert_eq!(base.get_str(PARTITION_KEY_HEADER), Some("user-42"));
    }

    #[test]
    fn a_plain_handle_carries_no_base() {
        assert!(SqsBroker::new().publisher().base_headers().is_none());
        assert!(
            SnsPublisher::new(CoreCell::default())
                .base_headers()
                .is_none()
        );
    }
}
