//! [`SqsSubscriber`]: a stream of deliveries backed by a long-polling pump task.
//!
//! The pump loops `ReceiveMessage` with long polling and forwards each delivery into a bounded
//! channel; settlement goes straight through the SDK client carried by each message (no round
//! trip). Cancelling the in-flight long poll happens only at subscriber drop, where the cost
//! (one closed HTTP connection) does not matter.

use std::time::Duration;

use futures::Stream;

use aws_sdk_sqs::types::MessageSystemAttributeName;
use ruststream::Subscriber;
use tokio::sync::mpsc;

use crate::broker::Core;
use crate::error::{SqsError, sdk_err};
use crate::message::SqsMessage;
use crate::queue::SqsQueue;

/// The visibility the extender re-arms when the descriptor does not name one (the SQS queue
/// default).
const DEFAULT_VISIBILITY: Duration = Duration::from_secs(30);

/// A subscription to one SQS queue; yields [`SqsMessage`]s.
///
/// Dropping the subscriber stops the pump task; unsettled messages redeliver when their
/// visibility lapses.
pub struct SqsSubscriber {
    queue_url: String,
    rx: mpsc::Receiver<Result<SqsMessage, SqsError>>,
}

impl std::fmt::Debug for SqsSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsSubscriber")
            .field("queue_url", &self.queue_url)
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
        // The channel bounds undelivered receives at one batch, so the pump never polls ahead
        // of what the consumer drains.
        let (tx, rx) = mpsc::channel(descriptor.batch_value().unsigned_abs() as usize);
        tokio::spawn(pump(
            core.sqs.clone(),
            queue_url.clone(),
            descriptor.clone(),
            tx,
        ));
        Self { queue_url, rx }
    }
}

impl Subscriber for SqsSubscriber {
    type Message = SqsMessage;
    type Error = SqsError;

    fn stream(&mut self) -> impl Stream<Item = Result<SqsMessage, SqsError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

async fn pump(
    client: aws_sdk_sqs::Client,
    queue_url: String,
    descriptor: SqsQueue,
    out: mpsc::Sender<Result<SqsMessage, SqsError>>,
) {
    let visibility = descriptor.visibility_value().unwrap_or(DEFAULT_VISIBILITY);
    let wait = i32::try_from(descriptor.wait_value().as_secs()).unwrap_or(20);
    loop {
        let mut receive = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(descriptor.batch_value())
            .wait_time_seconds(wait)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All);
        if let Some(v) = descriptor.visibility_value() {
            receive = receive.visibility_timeout(i32::try_from(v.as_secs()).unwrap_or(30));
        }

        // Dropping this future at subscriber drop is safe (hyper aborts the request); it only
        // costs the connection, and it happens once.
        let received = tokio::select! {
            biased;
            () = out.closed() => break,
            result = receive.send() => result,
        };

        match received {
            Ok(output) => {
                for message in output.messages() {
                    let Some(receipt) = message.receipt_handle() else {
                        continue;
                    };
                    let item = SqsMessage::new(
                        message,
                        client.clone(),
                        queue_url.clone(),
                        receipt.to_owned(),
                        visibility,
                    );
                    if out.send(Ok(item)).await.is_err() {
                        return;
                    }
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
