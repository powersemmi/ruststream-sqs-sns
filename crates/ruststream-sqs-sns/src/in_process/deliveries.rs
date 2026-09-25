//! The consuming half of the in-process transport: one subscription's receives, and how a
//! delivery taken from it settles.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::AckError;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::bus::Bus;
use crate::error::SqsError;
use crate::message::SqsMessage;
use crate::subscriber::{RECEIVE_CAP, one_at_a_time};

/// How long a batch receive waits for company once the queue holds fewer messages than the batch
/// asks for. A test publishes back to back, and the window has to outlast the gap between two of
/// its publishes: that is what makes a batch in a test deterministic rather than a race with the
/// dispatch loop. A live receive long-polls instead and returns what the queue holds.
const BATCH_WINDOW: Duration = Duration::from_millis(100);

/// One subscription on a queue of the in-process account.
pub(crate) struct BusDeliveries {
    bus: Arc<Bus>,
    /// The queue, keyed the way the account keys it.
    queue: String,
    queue_url: String,
    notify: Arc<Notify>,
    /// How long a delivery stays invisible once its handle is dropped unsettled.
    visibility: Duration,
    /// The `maxReceiveCount` the registration declared, which a delivery reads to tell a discard
    /// that would lose it from one that returns it.
    redrive_max: Option<NonZeroU32>,
}

impl BusDeliveries {
    pub(crate) fn open(
        bus: &Arc<Bus>,
        queue: String,
        visibility: Duration,
        redrive_max: Option<NonZeroU32>,
    ) -> Self {
        let notify = bus.open(&queue);
        Self {
            bus: Arc::clone(bus),
            queue_url: bus.url(&queue),
            queue,
            notify,
            visibility,
            redrive_max,
        }
    }

    pub(crate) fn queue_url(&self) -> &str {
        &self.queue_url
    }

    /// One message at a time, out of receives of up to ten, as the live subscriber hands them
    /// over.
    pub(crate) fn stream(
        &mut self,
    ) -> impl Stream<Item = Result<SqsMessage, SqsError>> + Send + '_ {
        self.receives(RECEIVE_CAP, None).flat_map(one_at_a_time)
    }

    /// One receive of up to `size` messages per batch.
    pub(crate) fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<SqsMessage>, SqsError>> + Send + '_ {
        self.receives(size, Some(BATCH_WINDOW))
    }

    fn receives(
        &mut self,
        size: NonZeroUsize,
        window: Option<Duration>,
    ) -> impl Stream<Item = Result<Vec<SqsMessage>, SqsError>> + Send + '_ {
        futures::stream::unfold((self, size, window), async move |(this, size, window)| {
            let batch = this.receive(size.get(), window).await;
            Some((Ok(batch), (this, size, window)))
        })
    }

    /// Waits until the queue has something to hand over, then receives up to `max` messages.
    ///
    /// With a `window`, a queue holding fewer than `max` is given that long to fill before the
    /// receive.
    async fn receive(&self, max: usize, window: Option<Duration>) -> Vec<SqsMessage> {
        loop {
            // Interest is registered before the queue is read, so a message arriving in between
            // still wakes the wait below.
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            let ready = self.bus.receivable(&self.queue, max);
            if ready == 0 {
                notified.await;
                continue;
            }
            if let Some(window) = window
                && ready < max
            {
                tokio::time::sleep(window).await;
            }
            let received = self.bus.receive(&self.queue, max);
            // Another subscription on the queue may have taken what was there, or the redrive
            // policy carried it away: wait for the next one.
            if !received.is_empty() {
                return received
                    .into_iter()
                    .map(|received| {
                        SqsMessage::in_process(
                            &received.message,
                            BusReceipt {
                                bus: Arc::clone(&self.bus),
                                queue: self.queue.clone(),
                                receipt: received.receipt,
                                visibility: self.visibility,
                                received: Instant::now(),
                                settled: AtomicBool::new(false),
                            },
                            self.redrive_max,
                        )
                    })
                    .collect();
            }
        }
    }
}

impl Drop for BusDeliveries {
    fn drop(&mut self) {
        self.bus.close(&self.queue);
    }
}

impl fmt::Debug for BusDeliveries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusDeliveries")
            .field("queue_url", &self.queue_url)
            .finish_non_exhaustive()
    }
}

/// How an in-process delivery settles: `DeleteMessage` and `ChangeMessageVisibility` against the
/// queue it came from.
///
/// Dropped unsettled, it does what the live delivery's handle does by stopping its visibility
/// extension: the message comes back once the visibility lapses. Either way it releases the
/// delivery to the harness once.
pub(crate) struct BusReceipt {
    bus: Arc<Bus>,
    queue: String,
    receipt: u64,
    visibility: Duration,
    /// When the queue handed the delivery out: its visibility runs from here, not from the drop.
    received: Instant,
    settled: AtomicBool,
}

impl BusReceipt {
    pub(crate) fn delete(&self) {
        self.bus.delete(&self.queue, self.receipt);
        self.settled.store(true, Ordering::Release);
    }

    pub(crate) fn change_visibility(&self, seconds: i32) -> Result<(), AckError> {
        let seconds = u64::try_from(seconds).map_err(|_| {
            AckError::Broker(format!("a visibility timeout of {seconds}s is negative").into())
        })?;
        self.bus
            .change_visibility(&self.queue, self.receipt, Duration::from_secs(seconds))
            .map_err(|reason| AckError::Broker(reason.into()))?;
        self.settled.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for BusReceipt {
    fn drop(&mut self) {
        if !*self.settled.get_mut() {
            // A receipt that is no longer in flight has nothing left to return.
            let remaining = remaining_visibility(self.visibility, self.received.elapsed());
            let _ = self
                .bus
                .change_visibility(&self.queue, self.receipt, remaining);
        }
        if let Some(coordinator) = self.bus.coordinator() {
            coordinator.consumed();
        }
    }
}

/// How long a delivery dropped `held` after it was received stays invisible, as a live delivery
/// does: the visibility runs from the receive, and the handle extends it to a full timeout every
/// half timeout (at least every second) while it is held.
fn remaining_visibility(visibility: Duration, held: Duration) -> Duration {
    if visibility.is_zero() {
        return Duration::ZERO;
    }
    let period = (visibility / 2).max(Duration::from_secs(1));
    let periods = held.as_nanos() / period.as_nanos();
    let last_extension = period.saturating_mul(u32::try_from(periods).unwrap_or(u32::MAX));
    (last_extension + visibility).saturating_sub(held)
}

impl fmt::Debug for BusReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusReceipt")
            .field("queue", &self.queue)
            .field("receipt", &self.receipt)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::remaining_visibility;

    #[test]
    fn the_visibility_runs_from_the_receive_and_its_extensions() {
        let visibility = Duration::from_secs(30);
        let at = |held| remaining_visibility(visibility, Duration::from_secs(held));
        assert_eq!(at(0), Duration::from_secs(30));
        assert_eq!(at(10), Duration::from_secs(20), "no extension yet");
        assert_eq!(at(20), Duration::from_secs(25), "extended at 15s");
        assert_eq!(at(31), Duration::from_secs(29), "extended at 30s");
    }
}
