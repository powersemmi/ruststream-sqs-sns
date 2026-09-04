//! [`SqsSubscriber`]: a stream of deliveries backed by a long-polling pump task.
//!
//! `ReceiveMessage` is already a paging call - `MaxNumberOfMessages` asks for up to ten messages
//! per round trip - so pages are native here: the page size a registration names becomes that
//! parameter, and one receive call is one page. A single-message subscription rides the same
//! call at the protocol maximum and hands the messages over one at a time, because SQS bills per
//! request rather than per message.
//!
//! The pump forwards whole pages into a channel that holds one, so it never runs more than one
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

/// The visibility the extender re-arms when the descriptor does not name one (the SQS queue
/// default).
const DEFAULT_VISIBILITY: Duration = Duration::from_secs(30);

/// The protocol cap on `MaxNumberOfMessages`: one `ReceiveMessage` returns at most ten
/// messages, whatever a page size asks for.
const RECEIVE_CAP: usize = 10;

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
    visibility: Option<Duration>,
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

    pub(crate) fn open(core: &Core, queue_url: String, descriptor: &SqsQueue) -> Self {
        Self {
            client: core.sqs.clone(),
            queue_url,
            wait: descriptor.wait_value(),
            visibility: descriptor.visibility_value(),
        }
    }

    /// Starts a pump asking for `size` messages per receive (clamped to the protocol cap) and
    /// returns the page channel.
    ///
    /// The pump lives as long as the receiver: the returned stream owns it, and dropping the
    /// stream closes the channel, which ends the pump on its next select. Because `stream` and
    /// `batches` borrow the subscriber mutably, at most one pump runs per subscription.
    fn pump(&self, size: usize) -> mpsc::Receiver<Result<Vec<SqsMessage>, SqsError>> {
        // One page in flight, so the pump stays exactly one receive ahead of the consumer.
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
    visibility: Option<Duration>,
}

/// Turns a page channel into the stream shape both lanes are built from.
fn pages(
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
        pages(self.pump(RECEIVE_CAP)).flat_map(|page| {
            futures::stream::iter(match page {
                Ok(messages) => messages.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
        })
    }
}

/// Pages are the transport's own: the size a registration names becomes `MaxNumberOfMessages`,
/// and one `ReceiveMessage` call is one page.
///
/// `ReceiveMessage` returns at most ten messages, so a larger size is clamped to ten rather than
/// refused: the framework's contract already lets a page come back shorter than it was asked
/// for, and refusing would make this broker stricter than the contract - a handler mounted with
/// `batch(nonzero!(50))` on a broker whose pages go that high would stop compiling its way onto
/// SQS. The clamp is logged once per subscription so it is not silent.
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
                "sqs receives at most 10 messages per call; pages are capped at that",
            );
        }
        pages(self.pump(requested))
    }
}

async fn pump(
    client: aws_sdk_sqs::Client,
    queue_url: String,
    call: Receive,
    out: mpsc::Sender<Result<Vec<SqsMessage>, SqsError>>,
) {
    let visibility = call.visibility.unwrap_or(DEFAULT_VISIBILITY);
    loop {
        let mut receive = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(call.size)
            .wait_time_seconds(call.wait)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All);
        if let Some(v) = call.visibility {
            receive = receive.visibility_timeout(i32::try_from(v.as_secs()).unwrap_or(30));
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
                let page: Vec<SqsMessage> = output
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
                // A long poll that timed out has no page to deliver, and an empty one would
                // break the "a page is never empty" half of the contract.
                if page.is_empty() {
                    continue;
                }
                if out.send(Ok(page)).await.is_err() {
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
