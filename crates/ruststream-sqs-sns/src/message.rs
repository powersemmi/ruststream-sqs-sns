//! [`SqsMessage`] and the mapping between `RustStream` headers and SQS message attributes.
//!
//! Message attributes carry headers directly (String for values the service takes as text,
//! Binary otherwise) - no envelope format is invented. The one transport constraint is the
//! body: SQS bodies are text, and a payload the service will not take as text travels
//! base64-encoded with a marker attribute, decoded transparently on receive.

// Without the `testing` feature a delivery settles one way, so a `match` on how it settles has a
// single arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::num::NonZeroU32;
use std::time::Duration;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::primitives::Blob;
use aws_sdk_sqs::types::{
    Message as AwsMessage, MessageAttributeValue, MessageSystemAttributeName,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use ruststream::{AckError, BytesMut, HeaderMap, IncomingMessage, Partitioned, Str};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::error::sdk_err;
#[cfg(feature = "testing")]
use crate::in_process::BusReceipt;

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
/// routing belongs to the queue's redrive policy, driven by repeated receives. The one exception
/// is the delivery that has used up that policy's receives: there a discard returns the message
/// instead, because being received once more is how SQS carries it to the dead-letter queue, and
/// a delete would lose it.
///
/// While the handle is alive, a background task keeps extending the message's visibility, so a
/// handler outliving the visibility timeout does not cause a concurrent redelivery.
pub struct SqsMessage {
    payload: Bytes,
    headers: HeaderMap,
    /// The queue's `ApproximateReceiveCount` for this delivery: the first receive answers one.
    receives: Option<u32>,
    /// The `maxReceiveCount` the registration's declaration wrote onto the queue, where it
    /// declared one.
    redrive_max: Option<NonZeroU32>,
    receipt: Receipt,
}

/// How a delivery settles: through the SDK client against the live queue, or, under the
/// `testing` feature, against the in-process account the harness connected instead.
///
/// Without the feature there is one variant, so the type is the live receipt itself and every
/// `match` on it resolves at compile time.
enum Receipt {
    Aws(AwsReceipt),
    #[cfg(feature = "testing")]
    InProcess(BusReceipt),
}

#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Receipt>() == size_of::<AwsReceipt>());

/// A live delivery's settlement handle, and the task keeping it invisible meanwhile.
struct AwsReceipt {
    client: Client,
    queue_url: String,
    receipt: String,
    extender: JoinHandle<()>,
}

impl std::fmt::Debug for SqsMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("SqsMessage");
        debug.field("payload_len", &self.payload.len());
        match &self.receipt {
            Receipt::Aws(aws) => debug.field("queue_url", &aws.queue_url),
            #[cfg(feature = "testing")]
            Receipt::InProcess(receipt) => debug.field("in_process", receipt),
        };
        debug.finish_non_exhaustive()
    }
}

impl Drop for SqsMessage {
    fn drop(&mut self) {
        // An unsettled drop stops the extension; the message redelivers when its current
        // visibility lapses, which is the at-least-once contract. The in-process receipt does the
        // same from its own `Drop`.
        self.stop_extending();
    }
}

/// The receive count the queue reported with `message`.
fn receives_of(message: &AwsMessage) -> Option<u32> {
    message
        .attributes()
        .and_then(|system| system.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|count| count.trim().parse().ok())
}

impl SqsMessage {
    pub(crate) fn new(
        message: &AwsMessage,
        runtime: &Handle,
        client: Client,
        queue_url: String,
        receipt: String,
        visibility: Duration,
        redrive_max: Option<NonZeroU32>,
    ) -> Self {
        let (payload, headers) = decode_message(message);
        // Why a per-message watchdog: SQS has no lease API - a handler outliving the
        // visibility timeout would get a concurrent redelivery, so the crate extends the
        // visibility for as long as the handle is held (the issue's one piece of real
        // machinery). Aborted on settle or drop. It runs on the runtime the broker connected on,
        // not on the thread that holds the delivery: a handler computing on a thread of its own
        // would otherwise hold the extension back until the visibility lapsed.
        let extender = runtime.spawn(extend_visibility(
            client.clone(),
            queue_url.clone(),
            receipt.clone(),
            visibility,
        ));
        Self {
            payload,
            headers,
            receives: receives_of(message),
            redrive_max,
            receipt: Receipt::Aws(AwsReceipt {
                client,
                queue_url,
                receipt,
                extender,
            }),
        }
    }

    /// A delivery of the in-process account, decoded from the message the account handed over
    /// exactly as a live one is decoded from the service's answer.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(
        message: &AwsMessage,
        receipt: BusReceipt,
        redrive_max: Option<NonZeroU32>,
    ) -> Self {
        let (payload, headers) = decode_message(message);
        Self {
            payload,
            headers,
            receives: receives_of(message),
            redrive_max,
            receipt: Receipt::InProcess(receipt),
        }
    }

    /// Stops keeping the delivery invisible, ahead of a settlement.
    fn stop_extending(&self) {
        match &self.receipt {
            Receipt::Aws(aws) => aws.extender.abort(),
            #[cfg(feature = "testing")]
            Receipt::InProcess(_) => {}
        }
    }

    /// Whether this delivery has used up the receives the queue's redrive policy allows.
    ///
    /// The move to the dead-letter queue happens on the receive after that, so returning the
    /// message is what performs it, and deleting it is what loses it.
    fn spent(&self) -> bool {
        match (self.receives, self.redrive_max) {
            (Some(receives), Some(max)) => receives >= max.get(),
            _ => false,
        }
    }

    async fn delete(&self) -> Result<(), AckError> {
        let aws = match &self.receipt {
            Receipt::Aws(aws) => aws,
            #[cfg(feature = "testing")]
            Receipt::InProcess(receipt) => {
                receipt.delete();
                return Ok(());
            }
        };
        aws.client
            .delete_message()
            .queue_url(&aws.queue_url)
            .receipt_handle(&aws.receipt)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| AckError::Broker(sdk_err(&e)))
    }

    async fn set_visibility(&self, seconds: i32) -> Result<(), AckError> {
        let aws = match &self.receipt {
            Receipt::Aws(aws) => aws,
            #[cfg(feature = "testing")]
            Receipt::InProcess(receipt) => return receipt.change_visibility(seconds),
        };
        aws.client
            .change_message_visibility()
            .queue_url(&aws.queue_url)
            .receipt_handle(&aws.receipt)
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

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.stop_extending();
        self.delete().await
    }

    /// SQS counts every receive of a message and reports it with the delivery, so a cap counts
    /// the queue's own redeliveries rather than only what this process sent back. The receive in
    /// hand is counted, which is what the first delivery answering one means.
    fn redelivery_count(&self) -> Option<u64> {
        self.receives.map(u64::from)
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.stop_extending();
        if requeue || self.spent() {
            // Deleting IS the drop: SQS cannot discard without deleting, and the redrive policy
            // owns poison-message routing. The exception is the delivery that has run the policy
            // out: there the queue is one receive away from carrying it to the dead-letter
            // queue, so returning it is the discard and a delete would lose it.
            self.set_visibility(0).await
        } else {
            self.delete().await
        }
    }

    /// Every SQS delivery honors a delayed redelivery: the visibility timeout is the delay, so
    /// the runtime must take `nack_after` here instead of its broker-agnostic deferred
    /// re-publish, which would re-publish a copy and reset the receive count.
    fn supports_nack_after(&self) -> bool {
        true
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        self.stop_extending();
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
///
/// A queue configured with no invisibility at all has nothing to extend, so the task ends
/// instead of re-arming a zero once a second for as long as the handler runs.
async fn extend_visibility(
    client: Client,
    queue_url: String,
    receipt: String,
    visibility: Duration,
) {
    if visibility.is_zero() {
        return;
    }
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

/// The payload and the headers a delivery carries, read off the message the queue returned.
pub(crate) fn decode_message(message: &AwsMessage) -> (Bytes, HeaderMap) {
    let mut headers = HeaderMap::new();
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
            headers.insert(Str::from_static(PARTITION_KEY_HEADER), group.clone());
        }
        if let Some(count) = system.get(&MessageSystemAttributeName::ApproximateReceiveCount) {
            headers.insert(Str::from_static(RECEIVE_COUNT_HEADER), count.clone());
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

/// Whether `text` is something the service will carry as text.
///
/// SQS and SNS accept a narrower set than UTF-8 in bodies and attribute values: the C0 control
/// characters other than tab, newline and carriage return are rejected, as are the two
/// non-characters at the end of the basic plane. A Rust `char` is never a surrogate, so that
/// half of the service's rule cannot be violated here.
pub(crate) fn is_service_text(text: &str) -> bool {
    text.chars().all(|c| {
        matches!(c, '\t' | '\n' | '\r')
            || ('\u{20}'..='\u{d7ff}').contains(&c)
            || ('\u{e000}'..='\u{fffd}').contains(&c)
            || c >= '\u{10000}'
    })
}

/// Encodes a payload into an SQS body: text the service accepts passes through, anything else
/// travels base64 with the marker attribute. Returns the body and whether the marker must be
/// set.
///
/// "Anything else" is wider than "not UTF-8": a binary codec's output is often valid UTF-8 and
/// still carries control bytes the service refuses, and a rejected send is a worse answer than
/// a transparently encoded one.
pub(crate) fn encode_body(payload: BytesMut) -> (String, bool) {
    // `Vec::from` reclaims the buffer the framework wrote and `String::from_utf8` validates it in
    // place, so a body the service accepts as text reaches the request without a copy.
    match String::from_utf8(Vec::from(payload)) {
        Ok(text) if is_service_text(&text) => (text, false),
        Ok(text) => (BASE64.encode(text.as_bytes()), true),
        Err(invalid) => (BASE64.encode(invalid.as_bytes()), true),
    }
}

/// Converts headers into SQS message attributes (String for values the service carries as text,
/// Binary otherwise), pulling the partition key out for the FIFO group id.
///
/// A `HeaderMap` value is bytes on both sides of the wire, so the choice between the two
/// attribute types is invisible to a service: what it wrote is what it reads back.
pub(crate) fn encode_attributes(
    headers: &HeaderMap,
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
        let attribute = match std::str::from_utf8(value) {
            Ok(text) if is_service_text(text) => MessageAttributeValue::builder()
                .data_type("String")
                .string_value(text)
                .build(),
            _ => MessageAttributeValue::builder()
                .data_type("Binary")
                .binary_value(Blob::new(value))
                .build(),
        };
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

    /// Content equality cannot tell a hand-over from a copy, so the buffer the framework wrote is
    /// identified by its address.
    #[test]
    fn a_text_body_keeps_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written = payload.as_ptr();

        let (body, base64_marker) = encode_body(payload);

        assert!(!base64_marker, "a JSON body is text the service accepts");
        assert_eq!(
            body.as_ptr(),
            written,
            "the body is the buffer the framework wrote, validated as text rather than copied"
        );
    }

    #[test]
    fn utf8_payloads_pass_through() {
        let (body, marker) = encode_body(BytesMut::from(&b"{\"id\":1}"[..]));
        assert_eq!(body, "{\"id\":1}");
        assert!(!marker);
    }

    #[test]
    fn binary_payloads_travel_base64_with_marker() {
        let raw = [0u8, 159, 146, 150];
        let (body, marker) = encode_body(BytesMut::from(&raw[..]));
        assert!(marker);
        assert_eq!(BASE64.decode(body).expect("valid base64"), raw);
    }

    /// The bytes a binary codec emits are often valid UTF-8 and still carry control characters
    /// the service refuses in a body. Sending them raw is a rejected publish, so they take the
    /// base64 lane too.
    #[test]
    fn utf8_the_service_refuses_travels_base64_as_well() {
        let raw = 0u32.to_be_bytes();
        assert!(
            std::str::from_utf8(&raw).is_ok(),
            "the sample is valid UTF-8"
        );
        let (body, marker) = encode_body(BytesMut::from(&raw[..]));
        assert!(marker);
        assert_eq!(BASE64.decode(body).expect("valid base64"), raw);
    }

    #[test]
    fn whitespace_the_service_accepts_stays_text() {
        let (body, marker) = encode_body(BytesMut::from(&b"a\tb\r\nc"[..]));
        assert!(!marker);
        assert_eq!(body, "a\tb\r\nc");
    }

    /// A header value the service refuses as text becomes a binary attribute rather than a
    /// rejected send; the map is bytes on both sides, so nothing changes for a service.
    #[test]
    fn header_values_the_service_refuses_become_binary_attributes() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trace", Bytes::from_static(&[0u8, 1, 2, 3]));
        let (attributes, _) = encode_attributes(&headers, false);
        let attribute = attributes.get("x-trace").expect("the header is carried");
        assert_eq!(attribute.data_type(), "Binary");
        assert_eq!(
            attribute.binary_value().expect("a binary value").as_ref(),
            [0u8, 1, 2, 3]
        );
    }

    #[test]
    fn partition_key_header_becomes_the_group_id() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        let (attributes, group) = encode_attributes(&headers, false);
        assert_eq!(group.as_deref(), Some("user-42"));
        assert!(attributes.contains_key("x-tenant"));
        assert!(!attributes.contains_key(PARTITION_KEY_HEADER));
    }

    /// A client built from a bare config: no network happens until an operation is sent, and
    /// this test never sends one.
    fn offline_client() -> Client {
        let config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .build();
        Client::new(&config)
    }

    /// One delivery as the service hands it over, with `receives` as its
    /// `ApproximateReceiveCount` and `redrive_max` as the policy the registration declared.
    fn delivered(receives: Option<u32>, redrive_max: Option<u32>) -> SqsMessage {
        let mut raw = AwsMessage::builder().body("{}").receipt_handle("receipt");
        if let Some(receives) = receives {
            raw = raw.attributes(
                MessageSystemAttributeName::ApproximateReceiveCount,
                receives.to_string(),
            );
        }
        SqsMessage::new(
            &raw.build(),
            &Handle::current(),
            offline_client(),
            "http://localhost:4566/000000000000/queue".to_owned(),
            "receipt".to_owned(),
            Duration::from_secs(30),
            redrive_max.and_then(NonZeroU32::new),
        )
    }

    /// The runtime picks the native path off this flag, so a delivery that can change its own
    /// visibility has to report it; without it `retry_after` silently falls back to the
    /// deferred re-publish.
    #[tokio::test]
    async fn deliveries_advertise_native_delayed_redelivery() {
        assert!(delivered(None, None).supports_nack_after());
    }

    #[tokio::test]
    async fn a_delivery_reports_the_queues_receive_count() {
        assert_eq!(delivered(Some(1), None).redelivery_count(), Some(1));
        assert_eq!(delivered(Some(4), None).redelivery_count(), Some(4));
    }

    /// A queue that reports no count leaves the framework's own header to do the counting.
    #[tokio::test]
    async fn a_delivery_without_the_attribute_reports_no_count() {
        assert_eq!(delivered(None, None).redelivery_count(), None);
    }

    /// The one delivery a discard must not delete: the queue is a single receive away from
    /// carrying it to the dead-letter queue, and deleting it there loses it instead.
    #[tokio::test]
    async fn the_last_receive_the_redrive_policy_allows_is_spent() {
        assert!(!delivered(Some(2), Some(3)).spent());
        assert!(delivered(Some(3), Some(3)).spent());
        assert!(delivered(Some(4), Some(3)).spent());
    }

    /// Without a declared policy nothing is spent, so a discard stays the delete it always was.
    #[tokio::test]
    async fn a_queue_with_no_declared_policy_spends_nothing() {
        assert!(!delivered(Some(9), None).spent());
    }
}
