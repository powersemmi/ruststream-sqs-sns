//! [`SqsTestSubscriber`] and [`SqsTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned, Str,
    Subscriber, testing::Coordinator,
};
use tokio::time::sleep;

use crate::error::SqsError;
use crate::queue::Redrive;
use crate::subscriber::receive_batch;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};
use crate::{PARTITION_KEY_HEADER, RECEIVE_COUNT_HEADER};

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
/// that, and never more than the ten one receive returns.
pub struct SqsTestSubscriber {
    /// The queue the subscription reads, which the batch cap's warning names.
    queue: String,
    inner: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for SqsTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsTestSubscriber").finish_non_exhaustive()
    }
}

impl SqsTestSubscriber {
    pub(crate) fn new(
        queue: String,
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        redrive: Option<Redrive>,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            queue,
            inner: BufferedSubscriber::new(Deliveries {
                state,
                id,
                rx,
                requeue,
                redrive: redrive.map(Arc::new),
                coordinator,
            })
            .max_wait(BATCH_WINDOW),
        }
    }
}

impl Subscriber for SqsTestSubscriber {
    type Message = SqsTestMessage;
    type Error = SqsError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream()
    }
}

/// Batches are capped where the queue caps them: one `ReceiveMessage` returns at most ten
/// messages, so a registration that asks for more gets batches of ten here, as it does from the
/// queue, and the same warning says so.
impl BatchSubscriber for SqsTestSubscriber {
    type Batch = Vec<SqsTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, SqsError>> + Send + '_ {
        let size = receive_batch(size, &self.queue);
        self.inner.batches(size)
    }
}

/// The one-at-a-time delivery lane the buffer above batches: the subscription's own channel.
struct Deliveries {
    state: Arc<TestState>,
    id: SubscriptionId,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// The registration's redrive policy, shared with every delivery rather than cloned per
    /// message: it holds a queue name and is read on the settle path only.
    redrive: Option<Arc<Redrive>>,
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
        let state = Arc::clone(&self.state);
        let requeue = self.requeue.clone();
        let redrive = self.redrive.clone();
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|mut delivery| {
                    // The receive is counted as the delivery leaves the queue, which is where
                    // SQS counts it: the first hand-over answers one. It reaches the handler in
                    // the same header the real subscriber puts it in, so a service reading its
                    // attempt number reads the same value in a test.
                    delivery.receives = delivery.receives.saturating_add(1);
                    delivery.headers.insert(
                        Str::from_static(RECEIVE_COUNT_HEADER),
                        delivery.receives.to_string(),
                    );
                    Ok(SqsTestMessage::new(
                        Arc::clone(&state),
                        delivery,
                        requeue.clone(),
                        redrive.clone(),
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
///
/// A registration that declared a cap and a dead-letter destination has them on the
/// subscription as a redrive policy, and a re-queue that would exceed the cap sends the delivery
/// to the dead-letter queue instead, as SQS does on the next receive. A discard of such a
/// delivery goes the same way, because that is the only route a message has to the dead-letter
/// queue on SQS.
pub struct SqsTestMessage {
    state: Arc<TestState>,
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    redrive: Option<Arc<Redrive>>,
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
        state: Arc<TestState>,
        delivery: Delivery,
        requeue: DeliverySender,
        redrive: Option<Arc<Redrive>>,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            state,
            delivery: Some(delivery),
            requeue,
            redrive,
            coordinator,
        }
    }

    /// The pieces one re-queue needs, taken out of the handle so the closures below own them.
    fn taken(&mut self) -> (Delivery, Requeue) {
        let delivery = self
            .delivery
            .take()
            .expect("SqsTestMessage ack/nack invoked twice");
        (
            delivery,
            Requeue {
                state: Arc::clone(&self.state),
                sender: self.requeue.clone(),
                redrive: self.redrive.clone(),
                coordinator: self.coordinator.clone(),
            },
        )
    }
}

/// Where a delivery goes when the handler asks for it again: back onto the subscription, or to
/// the dead-letter queue once the redrive policy's count runs out.
struct Requeue {
    state: Arc<TestState>,
    sender: DeliverySender,
    redrive: Option<Arc<Redrive>>,
    coordinator: Option<Coordinator>,
}

impl Requeue {
    /// Whether the queue's redrive policy has run out of receives for `delivery`.
    fn spent(&self, delivery: &Delivery) -> bool {
        self.redrive
            .as_ref()
            .is_some_and(|redrive| delivery.receives >= redrive.max_receive_count.get())
    }

    /// Puts `delivery` back, or carries it away.
    ///
    /// SQS moves a message on the receive that would exceed `maxReceiveCount`, so the count in
    /// hand is compared against the cap and the move happens instead of the hand-over. The
    /// dead-letter copy goes through the router's own publish, so the harness reads it in that
    /// queue's log exactly as it reads any other message.
    fn settle(self, delivery: Delivery) {
        if self.spent(&delivery) {
            self.carry_away(delivery);
            return;
        }
        // The re-queue bypasses fanout, so count the re-enqueue here to balance the message's
        // `Drop` decrement. The redelivered copy is consumed in turn.
        if self.sender.send(delivery).is_ok()
            && let Some(coordinator) = &self.coordinator
        {
            coordinator.enqueued();
        }
    }

    /// Discards `delivery`, which on a queue whose redrive policy has run out means letting the
    /// queue carry it to the dead-letter queue instead of dropping it.
    fn discard(self, delivery: Delivery) {
        if self.spent(&delivery) {
            self.carry_away(delivery);
        }
    }

    /// Publishes `delivery` to the dead-letter queue, which the redrive policy names.
    fn carry_away(self, delivery: Delivery) {
        if let Some(redrive) = &self.redrive {
            self.state
                .publish(&redrive.dead_letter, delivery.payload, delivery.headers);
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

    /// The stand-in's `ApproximateReceiveCount`, counted the way the queue counts it, so a cap
    /// driven under the harness ends where it would end in production.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivery
            .as_ref()
            .map(|delivery| u64::from(delivery.receives))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let (delivery, back) = self.taken();
        if requeue {
            back.settle(delivery);
        } else {
            back.discard(delivery);
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
        let (delivery, back) = self.taken();
        // Under the harness the redelivery is registered with the coordinator, the way the queue
        // registers a visibility timeout, so `TestApp::advance` fires it and the in-flight count
        // stays balanced against this message's `Drop`. The redrive policy is read when the
        // delay is over, which is where SQS reads it: on the receive that would follow.
        if let Some(coordinator) = self.coordinator.clone() {
            coordinator.schedule_redelivery(delay, move || back.settle(delivery));
            return ready(Ok(()));
        }
        tokio::spawn(async move {
            sleep(delay).await;
            back.settle(delivery);
        });
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
