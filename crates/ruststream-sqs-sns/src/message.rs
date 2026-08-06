//! [`SqsMessage`] and the mapping between `RustStream` headers and SQS message attributes.
//!
//! Message attributes carry headers directly (String for UTF-8 values, Binary otherwise) - no
//! envelope format is invented. The one transport constraint is the body: SQS bodies are text,
//! so a payload that is not valid UTF-8 travels base64-encoded with a marker attribute, and is
//! decoded transparently on receive.

use std::time::Duration;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::primitives::Blob;
use aws_sdk_sqs::types::{
    Message as AwsMessage, MessageAttributeValue, MessageSystemAttributeName,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned};
use tokio::task::JoinHandle;

use crate::error::sdk_err;

/// Header carrying the partition key, mapped onto the FIFO message group id.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Header exposing the approximate receive count on received messages.
pub const RECEIVE_COUNT_HEADER: &str = "sqs-receive-count";

/// Marker attribute set when the payload travels base64-encoded (SQS bodies are text; binary
/// payloads have no other faithful form).
pub(crate) const ENCODING_ATTRIBUTE: &str = "ruststream-payload-encoding";

/// A message delivered by an [`SqsSubscriber`](crate::SqsSubscriber).
///
/// `ack` deletes the message; `nack(requeue = true)` zeroes its visibility so it redelivers
/// immediately; `nack_after(delay)` sets the visibility to the delay, so deferred retry is
/// native. `nack(requeue = false)` deletes: SQS has no drop verb short of deletion - poison
/// routing belongs to the queue's redrive policy, driven by repeated requeues.
///
/// While the handle is alive, a background task keeps extending the message's visibility, so a
/// handler outliving the visibility timeout does not cause a concurrent redelivery.
pub struct SqsMessage {
    payload: Bytes,
    headers: Headers,
    client: Client,
    queue_url: String,
    receipt: String,
    extender: JoinHandle<()>,
}

impl std::fmt::Debug for SqsMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsMessage")
            .field("payload_len", &self.payload.len())
            .field("queue_url", &self.queue_url)
            .finish_non_exhaustive()
    }
}

impl Drop for SqsMessage {
    fn drop(&mut self) {
        // An unsettled drop stops the extension; the message redelivers when its current
        // visibility lapses, which is the at-least-once contract.
        self.extender.abort();
    }
}

impl SqsMessage {
    pub(crate) fn new(
        message: &AwsMessage,
        client: Client,
        queue_url: String,
        receipt: String,
        visibility: Duration,
    ) -> Self {
        let (payload, headers) = decode_message(message);
        // Why a per-message watchdog: SQS has no lease API - a handler outliving the
        // visibility timeout would get a concurrent redelivery, so the crate extends the
        // visibility for as long as the handle is held (the issue's one piece of real
        // machinery). Aborted on settle or drop.
        let extender = tokio::spawn(extend_visibility(
            client.clone(),
            queue_url.clone(),
            receipt.clone(),
            visibility,
        ));
        Self {
            payload,
            headers,
            client,
            queue_url,
            receipt,
            extender,
        }
    }

    async fn delete(&self) -> Result<(), AckError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(&self.receipt)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| AckError::Broker(sdk_err(&e)))
    }

    async fn set_visibility(&self, seconds: i32) -> Result<(), AckError> {
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(&self.receipt)
            .visibility_timeout(seconds)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| AckError::Broker(sdk_err(&e)))
    }
}

impl Partitioned for SqsMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for SqsMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.extender.abort();
        self.delete().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.extender.abort();
        if requeue {
            self.set_visibility(0).await
        } else {
            // Deleting IS the drop: SQS cannot discard without deleting, and the redrive
            // policy owns poison-message routing.
            self.delete().await
        }
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        self.extender.abort();
        // Setting the visibility to the delay is the native deferred retry (capped at the
        // protocol's 12 hours).
        let seconds = i32::try_from(delay.as_secs().min(43_200)).unwrap_or(43_200);
        self.set_visibility(seconds).await
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Keeps a message invisible while its handle is alive: re-arms the visibility to `visibility`
/// every half period. Aborted on settle/drop; a failed extension is logged and retried on the
/// next tick (the message may redeliver, which at-least-once permits).
async fn extend_visibility(
    client: Client,
    queue_url: String,
    receipt: String,
    visibility: Duration,
) {
    let period = (visibility / 2).max(Duration::from_secs(1));
    let seconds = i32::try_from(visibility.as_secs().min(43_200)).unwrap_or(43_200);
    loop {
        tokio::time::sleep(period).await;
        let outcome = client
            .change_message_visibility()
            .queue_url(&queue_url)
            .receipt_handle(&receipt)
            .visibility_timeout(seconds)
            .send()
            .await;
        if let Err(err) = outcome {
            tracing::debug!(
                queue_url = %queue_url,
                error = %aws_sdk_sqs::error::DisplayErrorContext(&err),
                "sqs visibility extension failed"
            );
        }
    }
}

fn decode_message(message: &AwsMessage) -> (Bytes, Headers) {
    let mut headers = Headers::new();
    let mut base64_payload = false;
    if let Some(attributes) = message.message_attributes() {
        for (name, value) in attributes {
            if name == ENCODING_ATTRIBUTE {
                base64_payload = value.string_value() == Some("base64");
                continue;
            }
            if let Some(text) = value.string_value() {
                headers.insert(name.clone(), text.to_owned());
            } else if let Some(blob) = value.binary_value() {
                headers.insert(name.clone(), Bytes::copy_from_slice(blob.as_ref()));
            }
        }
    }
    if let Some(system) = message.attributes() {
        if let Some(group) = system.get(&MessageSystemAttributeName::MessageGroupId) {
            headers.insert(PARTITION_KEY_HEADER, group.clone());
        }
        if let Some(count) = system.get(&MessageSystemAttributeName::ApproximateReceiveCount) {
            headers.insert(RECEIVE_COUNT_HEADER, count.clone());
        }
    }

    let body = message.body().unwrap_or_default();
    let payload = if base64_payload {
        BASE64
            .decode(body)
            .map_or_else(|_| Bytes::copy_from_slice(body.as_bytes()), Bytes::from)
    } else {
        Bytes::copy_from_slice(body.as_bytes())
    };
    (payload, headers)
}

/// Encodes a payload into an SQS body: UTF-8 passes through, anything else travels base64 with
/// the marker attribute. Returns the body and whether the marker must be set.
pub(crate) fn encode_body(payload: &[u8]) -> (String, bool) {
    std::str::from_utf8(payload).map_or_else(
        |_| (BASE64.encode(payload), true),
        |text| (text.to_owned(), false),
    )
}

/// Converts headers into SQS message attributes (String for UTF-8 values, Binary otherwise),
/// pulling the partition key out for the FIFO group id.
pub(crate) fn encode_attributes(
    headers: &Headers,
    base64_marker: bool,
) -> (
    std::collections::HashMap<String, MessageAttributeValue>,
    Option<String>,
) {
    let mut attributes = std::collections::HashMap::new();
    let mut group = None;
    for (name, value) in headers.iter() {
        if name == PARTITION_KEY_HEADER {
            group = Some(String::from_utf8_lossy(value).into_owned());
            continue;
        }
        let attribute = std::str::from_utf8(value).map_or_else(
            |_| {
                MessageAttributeValue::builder()
                    .data_type("Binary")
                    .binary_value(Blob::new(value))
                    .build()
            },
            |text| {
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(text)
                    .build()
            },
        );
        if let Ok(attribute) = attribute {
            attributes.insert(name.to_owned(), attribute);
        }
    }
    if base64_marker
        && let Ok(marker) = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("base64")
            .build()
    {
        attributes.insert(ENCODING_ATTRIBUTE.to_owned(), marker);
    }
    (attributes, group)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_payloads_pass_through() {
        let (body, marker) = encode_body(b"{\"id\":1}");
        assert_eq!(body, "{\"id\":1}");
        assert!(!marker);
    }

    #[test]
    fn binary_payloads_travel_base64_with_marker() {
        let raw = [0u8, 159, 146, 150];
        let (body, marker) = encode_body(&raw);
        assert!(marker);
        assert_eq!(BASE64.decode(body).expect("valid base64"), raw);
    }

    #[test]
    fn partition_key_header_becomes_the_group_id() {
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let (attributes, group) = encode_attributes(&headers, false);
        assert_eq!(group.as_deref(), Some("user-42"));
        assert!(attributes.contains_key("x-tenant"));
        assert!(!attributes.contains_key(PARTITION_KEY_HEADER));
    }
}
