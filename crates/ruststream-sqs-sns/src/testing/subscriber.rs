//! [`SqsTestSubscriber`] and [`SqsTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Subscriber, testing::Coordinator,
};
use tokio::time::sleep;

use crate::PARTITION_KEY_HEADER;
use crate::error::SqsError;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};

/// How long a partial batch waits for company. The in-process router hands over one delivery at
/// a time, so the batch is assembled on the client, and the window has to outlast the gap
/// between two publishes a test writes back to back - which is what makes a batch in a test
/// deterministic rather than a race with the dispatch loop.
const BATCH_WINDOW: Duration = Duration::from_millis(100);

/// Subscriber returned by [`ConnectedSqsTestBroker`](crate::testing::ConnectedSqsTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
///
/// The real subscriber batches on the wire, one `ReceiveMessage` per batch; the in-process
/// router has no such call, so batches here are assembled by the framework's own client-side
/// buffer. The mount site reads the same either way: it names a size and gets batches of at most
/// that.
pub struct SqsTestSubscriber(BufferedSubscriber<Deliveries>);

impl std::fmt::Debug for SqsTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsTestSubscriber").finish_non_exhaustive()
    }
}

impl SqsTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self(
            BufferedSubscriber::new(Deliveries {
                state,
                id,
                rx,
                requeue,
                coordinator,
            })
            .max_wait(BATCH_WINDOW),
        )
    }
}

impl Subscriber for SqsTestSubscriber {
    type Message = SqsTestMessage;
    type Error = SqsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

impl BatchSubscriber for SqsTestSubscriber {
    type Batch = Vec<SqsTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SqsError>> + Send + '_ {
        self.0.batches(size)
    }
}

/// The one-at-a-time delivery lane the buffer above batches: the subscription's own channel.
struct Deliveries {
    state: Arc<TestState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for Deliveries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deliveries").finish_non_exhaustive()
    }
}

impl Drop for Deliveries {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for Deliveries {
    type Message = SqsTestMessage;
    type Error = SqsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(SqsTestMessage::new(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                    ))
                })
            })
        })
    }
}

/// Message handed to handlers from an [`SqsTestSubscriber`].
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect. `nack_after(delay)` is the
/// same re-queue held back by the delay, which is what the queue's `ChangeMessageVisibility`
/// buys a service on SQS.
pub struct SqsTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for SqsTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for SqsTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsTestMessage").finish_non_exhaustive()
    }
}

impl SqsTestMessage {
    pub(crate) fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
        }
    }
}

impl Partitioned for SqsTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for SqsTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("SqsTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }

    /// Delayed redelivery is native here because it is native on SQS: a service that answers
    /// `retry_after` has its delay honoured under the harness the way the queue honours it, and
    /// the framework's deferred-republish fallback stays off this broker's path in a test as it
    /// is in production.
    fn supports_nack_after(&self) -> bool {
        true
    }

    fn nack_after(mut self, delay: Duration) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("SqsTestMessage ack/nack invoked twice");
        let requeue = self.requeue.clone();
        // Under the harness the redelivery is registered with the coordinator, the way the queue
        // registers a visibility timeout, so `TestApp::advance` fires it and the in-flight count
        // stays balanced against this message's `Drop`.
        if let Some(coordinator) = self.coordinator.clone() {
            let counter = coordinator.clone();
            coordinator.schedule_redelivery(delay, move || {
                if requeue.send(delivery).is_ok() {
                    counter.enqueued();
                }
            });
            return ready(Ok(()));
        }
        tokio::spawn(async move {
            sleep(delay).await;
            // The subscription may be gone by then; a dropped receiver is not an error.
            let _ = requeue.send(delivery);
        });
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
