//! [`SqsSubscriber`]: a stream of deliveries backed by a long-polling pump task.
//!
//! `ReceiveMessage` is already a batching call - `MaxNumberOfMessages` asks for up to ten
//! messages per round trip - so batches are native here: the batch size a registration names
//! becomes that parameter, and one receive call is one batch. A single-message subscription
//! rides the same call at the protocol maximum and hands the messages over one at a time,
//! because SQS bills per request rather than per message.
//!
//! The pump forwards whole batches into a channel that holds one, so it never runs more than one
//! receive ahead of what the consumer drains; settlement goes straight through the SDK client
//! carried by each message (no round trip). Cancelling the in-flight long poll happens only when
//! the stream is dropped, where the cost (one closed HTTP connection) does not matter.

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};

use aws_sdk_sqs::types::MessageSystemAttributeName;
use ruststream::{BatchSubscriber, Subscriber};
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{SqsError, sdk_err};
use crate::message::SqsMessage;
use crate::queue::SqsQueue;

/// The protocol cap on `MaxNumberOfMessages`: one `ReceiveMessage` returns at most ten
/// messages, whatever a batch size asks for.
const RECEIVE_CAP: usize = 10;

/// The protocol cap on a visibility timeout, in seconds.
const MAX_VISIBILITY_SECS: u64 = 12 * 60 * 60;

/// How long a delivery of this subscription stays invisible, and where that duration came from.
///
/// The receive call and the extender are one decision rather than two settings: whatever holds
/// the first delivery is what the extender has to re-arm, or the extender moves a deadline
/// somebody else set. Keeping them in one value makes that disagreement unrepresentable.
#[derive(Debug, Clone, Copy)]
enum Visibility {
    /// The descriptor named it, so every receive asks for it and the extender re-arms it.
    Requested(Duration),
    /// The descriptor named none. The queue's own timeout governs the receive, and the extender
    /// re-arms the value read from the queue when the subscription opened.
    Queue(Duration),
}

impl Visibility {
    /// What the receive call asks for, if anything.
    ///
    /// A queue's own timeout is already in force on a receive that names none, and naming it
    /// again would pin the value read at startup over any later edit the operator makes.
    const fn requested(self) -> Option<Duration> {
        match self {
            Self::Requested(visibility) => Some(visibility),
            Self::Queue(_) => None,
        }
    }

    /// The duration a delivery is held under, which the extender re-arms.
    const fn held(self) -> Duration {
        match self {
            Self::Requested(visibility) | Self::Queue(visibility) => visibility,
        }
    }

    /// The value as the SDK spells it, saturating at the protocol cap. A descriptor is validated
    /// against that cap and a queue cannot exceed it, so the saturation is unreachable rather
    /// than a fallback anything rides on.
    fn seconds(self) -> i32 {
        i32::try_from(self.held().as_secs().min(MAX_VISIBILITY_SECS))
            .unwrap_or_else(|_| i32::try_from(MAX_VISIBILITY_SECS).unwrap_or(i32::MAX))
    }
}

/// The receive size as the SDK spells it. The clamp is what makes the conversion exact, and
/// the fallback is that same cap, so nothing rides on it.
fn receive_size(requested: usize) -> i32 {
    i32::try_from(requested.min(RECEIVE_CAP)).unwrap_or(10)
}

/// A subscription to one SQS queue; yields [`SqsMessage`]s.
///
/// Dropping the stream stops the pump task; unsettled messages redeliver when their visibility
/// lapses.
pub struct SqsSubscriber {
    client: aws_sdk_sqs::Client,
    queue_url: String,
    wait: Duration,
    visibility: Visibility,
}

impl std::fmt::Debug for SqsSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsSubscriber")
            .field("queue_url", &self.queue_url)
            .field("wait", &self.wait)
            .field("visibility", &self.visibility)
            .finish_non_exhaustive()
    }
}

impl SqsSubscriber {
    /// The resolved URL of the queue this subscription polls.
    #[must_use]
    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    /// Opens a subscription on an already resolved queue URL.
    ///
    /// When the descriptor names no visibility, the queue's own timeout is read here, once, so
    /// the extender re-arms what the operator configured. `queue` is the name as the service
    /// wrote it, for the error.
    pub(crate) async fn open(
        core: &Core,
        queue: &str,
        queue_url: String,
        descriptor: &SqsQueue,
    ) -> Result<Self, SqsError> {
        let visibility = match descriptor.visibility_value() {
            Some(requested) => Visibility::Requested(requested),
            None => Visibility::Queue(core.queue_visibility(queue, &queue_url).await?),
        };
        Ok(Self {
            client: core.sqs.clone(),
            queue_url,
            wait: descriptor.wait_value(),
            visibility,
        })
    }

    /// Starts a pump asking for `size` messages per receive (clamped to the protocol cap) and
    /// returns the batch channel.
    ///
    /// The pump lives as long as the receiver: the returned stream owns it, and dropping the
    /// stream closes the channel, which ends the pump on its next select. Because `stream` and
    /// `batches` borrow the subscriber mutably, at most one pump runs per subscription.
    fn pump(&self, size: usize) -> mpsc::Receiver<Result<Vec<SqsMessage>, SqsError>> {
        // One batch in flight, so the pump stays exactly one receive ahead of the consumer.
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(pump(
            self.client.clone(),
            self.queue_url.clone(),
            Receive {
                size: receive_size(size),
                wait: i32::try_from(self.wait.as_secs()).unwrap_or(20),
                visibility: self.visibility,
            },
            tx,
        ));
        rx
    }
}

/// The receive-call parameters one pump repeats, resolved once when the pump starts.
#[derive(Debug, Clone, Copy)]
struct Receive {
    size: i32,
    wait: i32,
    visibility: Visibility,
}

/// Turns a batch channel into the stream shape both lanes are built from.
fn batch_stream(
    mut rx: mpsc::Receiver<Result<Vec<SqsMessage>, SqsError>>,
) -> impl Stream<Item = Result<Vec<SqsMessage>, SqsError>> + Send {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx))
}

impl Subscriber for SqsSubscriber {
    type Message = SqsMessage;
    type Error = SqsError;

    fn stream(&mut self) -> impl Stream<Item = Result<SqsMessage, SqsError>> + Send + '_ {
        // A single-message subscription still receives a whole call's worth: SQS charges per
        // request, so asking for the protocol maximum and handing the messages over one at a
        // time costs a tenth of what one receive per message would.
        batch_stream(self.pump(RECEIVE_CAP)).flat_map(|batch| {
            futures::stream::iter(match batch {
                Ok(messages) => messages.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
        })
    }
}

/// Batches are the transport's own: the size a registration names becomes
/// `MaxNumberOfMessages`, and one `ReceiveMessage` call is one batch.
///
/// `ReceiveMessage` returns at most ten messages, so a larger size is clamped to ten rather than
/// refused: the framework's contract already lets a batch come back shorter than it was asked
/// for, and refusing would make this broker stricter than the contract - a handler mounted with
/// `batch(nonzero!(50))` on a broker whose batches go that high would stop compiling its way
/// onto SQS. The clamp is logged once per subscription so it is not silent.
impl BatchSubscriber for SqsSubscriber {
    type Batch = Vec<SqsMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SqsError>> + Send + '_ {
        let requested = size.get();
        if requested > RECEIVE_CAP {
            tracing::warn!(
                queue_url = %self.queue_url,
                requested,
                delivered = RECEIVE_CAP,
                "sqs receives at most 10 messages per call; batches are capped at that",
            );
        }
        batch_stream(self.pump(requested))
    }
}

async fn pump(
    client: aws_sdk_sqs::Client,
    queue_url: String,
    call: Receive,
    out: mpsc::Sender<Result<Vec<SqsMessage>, SqsError>>,
) {
    let visibility = call.visibility.held();
    loop {
        let mut receive = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(call.size)
            .wait_time_seconds(call.wait)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All);
        if call.visibility.requested().is_some() {
            receive = receive.visibility_timeout(call.visibility.seconds());
        }

        // Dropping this future when the stream is dropped is safe (hyper aborts the request); it
        // only costs the connection, and it happens once.
        let received = tokio::select! {
            biased;
            () = out.closed() => break,
            result = receive.send() => result,
        };

        match received {
            Ok(output) => {
                let batch: Vec<SqsMessage> = output
                    .messages()
                    .iter()
                    .filter_map(|message| {
                        let receipt = message.receipt_handle()?;
                        Some(SqsMessage::new(
                            message,
                            client.clone(),
                            queue_url.clone(),
                            receipt.to_owned(),
                            visibility,
                        ))
                    })
                    .collect();
                // A long poll that timed out has no batch to deliver, and an empty one would
                // break the "a batch is never empty" half of the contract.
                if batch.is_empty() {
                    continue;
                }
                if out.send(Ok(batch)).await.is_err() {
                    return;
                }
            }
            Err(err) => {
                // The SDK already retried transient failures; what reaches here is either
                // fatal (queue gone, credentials) or a repeated transport failure. Surface it
                // and back off so a persistent failure cannot spin the loop hot.
                let fatal = err
                    .as_service_error()
                    .is_some_and(aws_sdk_sqs::operation::receive_message::ReceiveMessageError::is_queue_does_not_exist);
                if out
                    .send(Err(SqsError::Receive {
                        queue: queue_url.clone(),
                        source: sdk_err(&err),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                if fatal {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, Visibility};

    #[test]
    fn a_named_visibility_is_asked_for_and_re_armed() {
        let visibility = Visibility::Requested(Duration::from_secs(45));
        assert_eq!(visibility.requested(), Some(Duration::from_secs(45)));
        assert_eq!(visibility.held(), Duration::from_secs(45));
    }

    #[test]
    fn a_queues_own_visibility_is_re_armed_without_being_asked_for() {
        // The receive call names nothing, so the queue's setting governs it and stays the
        // operator's to change; the extender still re-arms that same duration.
        let visibility = Visibility::Queue(Duration::from_secs(300));
        assert_eq!(visibility.requested(), None);
        assert_eq!(visibility.held(), Duration::from_secs(300));
    }

    #[test]
    fn the_seconds_the_sdk_gets_saturate_at_the_protocol_cap() {
        let visibility = Visibility::Requested(Duration::from_secs(u64::MAX));
        assert_eq!(visibility.seconds(), 12 * 60 * 60);
    }
}
