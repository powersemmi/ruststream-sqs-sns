//! [`SqsPublisher`] (direct-to-queue) and [`SnsPublisher`] (topic fan-out), with their
//! policies.

use std::sync::Arc;

use aws_sdk_sns::types::MessageAttributeValue as SnsAttributeValue;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

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
    group: Option<String>,
}

impl std::fmt::Debug for SqsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsPublisher").finish_non_exhaustive()
    }
}

impl SqsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell, group: None }
    }

    /// Returns a handle whose sends carry `group` as the FIFO message group id.
    ///
    /// The handle aliases the same connection; only the group id differs. It exists because the
    /// group is a per-message argument and the publish builder fills its headers position once:
    /// a message type declaring a header contract spends that position on the contract value, so
    /// the `partition-key` header is not reachable for it. A message that does name the header
    /// still wins, the most specific level naming the group.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PublishExt;
    /// use ruststream_sqs_sns::SqsBroker;
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let publisher = SqsBroker::new().publisher();
    /// publisher
    ///     .with_group_id("user-42")
    ///     .raw(b"{\"id\":1}")
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
            group: Some(group.into()),
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

/// The message group of one send. The message's own `partition-key` is the most specific level
/// and wins; the handle's fixed group answers for the rest; FIFO requires a group, so a literal
/// closes the ladder.
fn resolve_group(from_message: Option<String>, fixed: Option<&str>) -> String {
    from_message
        .or_else(|| fixed.map(ToOwned::to_owned))
        .unwrap_or_else(|| "default".to_owned())
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
            send = send
                .message_group_id(resolve_group(group, self.group.as_deref()))
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

    async fn pair(self, connected: &ConnectedSqsBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
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
    group: Option<String>,
}

impl std::fmt::Debug for SnsPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnsPublisher").finish_non_exhaustive()
    }
}

impl SnsPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell, group: None }
    }

    /// Returns a handle whose sends carry `group` as the FIFO message group id, for a FIFO
    /// topic.
    ///
    /// The same per-message argument, and the same ladder, as
    /// [`SqsPublisher::with_group_id`]: the message's own `partition-key` header wins, and this
    /// handle answers for every message that does not name one.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::PublishExt;
    ///
    /// # async fn demo(broker: ruststream_sqs_sns::ConnectedSqsBroker)
    /// # -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// broker
    ///     .sns_publisher()
    ///     .with_group_id("user-42")
    ///     .raw(b"shipped")
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
            group: Some(group.into()),
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
            publish = publish
                .message_group_id(resolve_group(group, self.group.as_deref()))
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

    async fn pair(self, connected: &ConnectedSqsBroker) -> Result<Self::Live, PairError> {
        Ok(connected.sns_publisher())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::SqsBroker;

    #[test]
    fn a_group_id_handle_answers_for_messages_that_name_no_group() {
        let publisher = SqsBroker::new().publisher().with_group_id("user-42");
        assert_eq!(resolve_group(None, publisher.group.as_deref()), "user-42");
    }

    #[test]
    fn the_messages_own_partition_key_wins_over_the_handle() {
        let publisher = SqsBroker::new().publisher().with_group_id("user-42");
        assert_eq!(
            resolve_group(Some("user-7".to_owned()), publisher.group.as_deref()),
            "user-7"
        );
    }

    #[test]
    fn a_plain_handle_falls_back_to_the_fifo_default_group() {
        let publisher = SqsBroker::new().publisher();
        assert_eq!(resolve_group(None, publisher.group.as_deref()), "default");
    }
}
