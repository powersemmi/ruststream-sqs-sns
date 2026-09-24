//! The in-process account: every queue and topic a test's broker reaches, what each queue holds,
//! and the log the harness reads.
//!
//! It answers the service calls the crate makes (`SendMessage`, `ReceiveMessage`,
//! `DeleteMessage`, `ChangeMessageVisibility`, a redrive policy, `Publish` and `Subscribe` on a
//! topic) and refuses what the service refuses, so the crate's own logic runs over it unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use aws_sdk_sqs::types::{
    Message as AwsMessage, MessageAttributeValue, MessageSystemAttributeName,
};
use ruststream::RawMessage;
use ruststream::testing::Coordinator;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::broker::resource_name;
use crate::message::{decode_message, is_service_text};
use crate::publisher::{FifoSettings, is_fifo};

/// The visibility timeout SQS gives a queue created without one.
pub(crate) const DEFAULT_VISIBILITY: Duration = Duration::from_secs(30);

/// How long a FIFO queue remembers a deduplication id.
const DEDUPLICATION_WINDOW: Duration = Duration::from_mins(5);

/// The most message attributes one message carries, on SQS and on SNS alike.
const MAX_ATTRIBUTES: usize = 10;

/// The largest SQS message, body and attributes together.
pub(crate) const SQS_MAX_MESSAGE: usize = 1_048_576;

/// The largest SNS message, body and attributes together.
pub(crate) const SNS_MAX_MESSAGE: usize = 262_144;

/// The largest `maxReceiveCount` a redrive policy takes.
const MAX_RECEIVE_COUNT: u32 = 1_000;

/// The longest visibility timeout, in seconds.
const MAX_VISIBILITY_SECS: u64 = 12 * 60 * 60;

/// The longest queue name, the `.fifo` suffix included.
const MAX_QUEUE_NAME: usize = 80;

/// The longest topic name, the `.fifo` suffix included.
const MAX_TOPIC_NAME: usize = 256;

/// The account id the queue URLs carry.
const ACCOUNT_ID: &str = "000000000000";

/// One message on its way into a queue or a topic, in the form the service takes it.
#[derive(Debug, Clone)]
pub(crate) struct Outbound {
    pub(crate) body: String,
    pub(crate) attributes: HashMap<String, MessageAttributeValue>,
    pub(crate) fifo: Option<FifoSettings>,
}

/// A message a receive handed over, and the receipt it settles under.
pub(crate) struct Received {
    pub(crate) message: AwsMessage,
    pub(crate) receipt: u64,
}

/// The in-process account one connected broker reaches.
pub(crate) struct Bus {
    account: Mutex<Account>,
    coordinator: OnceLock<Coordinator>,
    /// Scheme and host of the queue URLs: the broker's endpoint, or the region's public one.
    url_base: String,
    next_id: AtomicU64,
}

#[derive(Default)]
struct Account {
    queues: HashMap<String, Queue>,
    /// The queues subscribed to each topic, by topic.
    topics: HashMap<String, Vec<Endpoint>>,
    /// Every message that arrived at a queue or a topic, by the name it was addressed by.
    log: HashMap<String, Vec<RawMessage>>,
}

/// A queue subscribed to a topic.
#[derive(Clone)]
struct Endpoint {
    queue: String,
    /// The queue's name as the service wrote it, which the log records the copy under.
    name: String,
}

/// Where a queue's redrive policy sends a spent message.
#[derive(Clone)]
struct DeadLetter {
    max_receive_count: u32,
    queue: String,
    name: String,
}

struct Queue {
    fifo: bool,
    redrive: Option<DeadLetter>,
    /// Open subscriptions. A queue with none drops what arrives (see the module overview).
    subscriptions: usize,
    /// Woken whenever a message may have become receivable.
    notify: Arc<Notify>,
    /// Visible messages in arrival order, which is the order a FIFO group is handed out in.
    visible: BTreeMap<u64, Stored>,
    /// Received messages not deleted yet, by receipt.
    in_flight: HashMap<u64, InFlight>,
    deduplication: HashMap<String, Instant>,
}

#[derive(Clone)]
struct Stored {
    seq: u64,
    outbound: Outbound,
    receives: u32,
}

struct InFlight {
    stored: Stored,
    /// Bumped by every visibility change, so a timer armed by an earlier one finds it stale.
    generation: u64,
}

impl Queue {
    fn new(fifo: bool) -> Self {
        Self {
            fifo,
            redrive: None,
            subscriptions: 0,
            notify: Arc::new(Notify::new()),
            visible: BTreeMap::new(),
            in_flight: HashMap::new(),
            deduplication: HashMap::new(),
        }
    }

    /// The groups a receive may not hand out: those with a message in flight.
    fn blocked_groups(&self) -> HashSet<String> {
        if !self.fifo {
            return HashSet::new();
        }
        self.in_flight
            .values()
            .filter_map(|entry| group_of(&entry.stored))
            .map(ToOwned::to_owned)
            .collect()
    }

    /// The first visible message a receive may hand out.
    fn next_receivable(&self, blocked: &HashSet<String>) -> Option<u64> {
        self.visible
            .values()
            .find(|stored| group_of(stored).is_none_or(|group| !blocked.contains(group)))
            .map(|stored| stored.seq)
    }

    /// Whether `id` arrived within the deduplication window, remembering it when not.
    fn duplicate(&mut self, id: &str) -> bool {
        let now = Instant::now();
        self.deduplication
            .retain(|_, seen| now.duration_since(*seen) < DEDUPLICATION_WINDOW);
        if self.deduplication.contains_key(id) {
            return true;
        }
        self.deduplication.insert(id.to_owned(), now);
        false
    }
}

fn group_of(stored: &Stored) -> Option<&str> {
    stored
        .outbound
        .fifo
        .as_ref()
        .map(|settings| settings.group.as_str())
}

/// The queue `name` addresses, keyed the way the service keys it: the last path segment of a
/// queue URL, or the name mapped onto the alphabet SQS takes.
pub(crate) fn queue_key(name: &str) -> Result<String, String> {
    let key = if name.starts_with("http://") || name.starts_with("https://") {
        name.rsplit('/')
            .find(|segment| !segment.is_empty())
            .unwrap_or_default()
            .to_owned()
    } else {
        resource_name(name)
    };
    if key.is_empty() || key.len() > MAX_QUEUE_NAME {
        return Err(format!(
            "a queue name is 1 to {MAX_QUEUE_NAME} characters, and {key:?} is not"
        ));
    }
    Ok(key)
}

/// The topic `name` addresses: the last segment of a topic ARN, or the name mapped the way a
/// queue name is.
pub(crate) fn topic_key(name: &str) -> Result<String, String> {
    let key = if name.starts_with("arn:") {
        name.rsplit(':').next().unwrap_or_default().to_owned()
    } else {
        resource_name(name)
    };
    if key.is_empty() || key.len() > MAX_TOPIC_NAME {
        return Err(format!(
            "a topic name is 1 to {MAX_TOPIC_NAME} characters, and {key:?} is not"
        ));
    }
    Ok(key)
}

/// Refuses what the service refuses in a message: an empty body, characters it does not carry,
/// more than ten attributes, an attribute it cannot name or with no value, and a size over `limit`.
pub(crate) fn check_message(outbound: &Outbound, limit: usize) -> Result<(), String> {
    if outbound.body.is_empty() {
        return Err("the message body must not be empty".to_owned());
    }
    if !is_service_text(&outbound.body) {
        return Err("the message body carries characters the service does not accept".to_owned());
    }
    if outbound.attributes.len() > MAX_ATTRIBUTES {
        return Err(format!(
            "a message carries at most {MAX_ATTRIBUTES} message attributes, and this one carries \
             {}: a header becomes an attribute",
            outbound.attributes.len()
        ));
    }
    let mut size = outbound.body.len();
    for (name, value) in &outbound.attributes {
        check_attribute_name(name)?;
        let value_len = value.string_value().map_or(0, str::len)
            + value.binary_value().map_or(0, |blob| blob.as_ref().len());
        if value_len == 0 {
            return Err(format!("message attribute {name:?} has an empty value"));
        }
        size += name.len() + value.data_type().len() + value_len;
    }
    if size > limit {
        return Err(format!(
            "the message is {size} bytes with its attributes, over the {limit}-byte limit"
        ));
    }
    Ok(())
}

/// A message attribute name as the service takes it.
fn check_attribute_name(name: &str) -> Result<(), String> {
    let lower = name.to_ascii_lowercase();
    let valid = !name.is_empty()
        && name.len() <= 256
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && !lower.starts_with("aws.")
        && !lower.starts_with("amazon.");
    if valid {
        Ok(())
    } else {
        Err(format!(
            "{name:?} is not a message attribute name the service takes: a header becomes an \
             attribute, and its name is 1 to 256 of letters, digits, '_', '-' and '.'"
        ))
    }
}

/// What a receiver would read from `outbound`: the payload and the headers, decoded the way a
/// delivery is.
fn log_entry(name: &str, outbound: &Outbound) -> RawMessage {
    let (payload, headers) = decode_message(&wire_message(outbound, None, None));
    RawMessage::new(name.to_owned(), payload).with_headers(headers)
}

/// `outbound` as a receive returns it.
fn wire_message(outbound: &Outbound, receipt: Option<u64>, receives: Option<u32>) -> AwsMessage {
    let mut message = AwsMessage::builder()
        .body(outbound.body.clone())
        .set_message_attributes(
            (!outbound.attributes.is_empty()).then(|| outbound.attributes.clone()),
        );
    if let Some(receipt) = receipt {
        message = message.receipt_handle(receipt.to_string());
    }
    if let Some(receives) = receives {
        message = message.attributes(
            MessageSystemAttributeName::ApproximateReceiveCount,
            receives.to_string(),
        );
    }
    if let Some(settings) = &outbound.fifo {
        message = message.attributes(
            MessageSystemAttributeName::MessageGroupId,
            settings.group.clone(),
        );
    }
    message.build()
}

impl Bus {
    /// An empty account, its queue URLs built on `endpoint` or, without one, on the public
    /// service of `region`.
    pub(crate) fn new(endpoint: Option<&str>, region: &str) -> Arc<Self> {
        let url_base = endpoint.map_or_else(
            || format!("https://sqs.{region}.amazonaws.com"),
            |endpoint| endpoint.trim_end_matches('/').to_owned(),
        );
        Arc::new(Self {
            account: Mutex::default(),
            coordinator: OnceLock::new(),
            url_base,
            next_id: AtomicU64::new(1),
        })
    }

    /// Installs the harness coordinator. A second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    fn account(&self) -> MutexGuard<'_, Account> {
        self.account
            .lock()
            .expect("the in-process account mutex is poisoned")
    }

    fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// The URL of the queue keyed `key`.
    pub(crate) fn url(&self, key: &str) -> String {
        format!("{}/{ACCOUNT_ID}/{key}", self.url_base)
    }

    /// Opens a subscription on the queue keyed `key`, and hands back what wakes it.
    pub(crate) fn open(&self, key: &str) -> Arc<Notify> {
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        queue.subscriptions += 1;
        let notify = Arc::clone(&queue.notify);
        drop(account);
        notify
    }

    /// Closes one subscription on `key`. The last one to go takes what the queue holds with it.
    pub(crate) fn close(&self, key: &str) {
        let dropped = {
            let mut account = self.account();
            let queue = queue_of(&mut account, key);
            queue.subscriptions = queue.subscriptions.saturating_sub(1);
            let dropped = if queue.subscriptions == 0 {
                std::mem::take(&mut queue.visible).len()
            } else {
                0
            };
            drop(account);
            dropped
        };
        if let Some(coordinator) = self.coordinator() {
            for _ in 0..dropped {
                coordinator.consumed();
            }
        }
    }

    /// Writes a redrive policy onto `key`: after `max_receive_count` receives a message moves to
    /// `dead_letter`.
    pub(crate) fn set_redrive(
        &self,
        key: &str,
        max_receive_count: NonZeroU32,
        dead_letter: &str,
    ) -> Result<(), String> {
        let target = queue_key(dead_letter)?;
        if max_receive_count.get() > MAX_RECEIVE_COUNT {
            return Err(format!(
                "a redrive policy's maxReceiveCount is 1 to {MAX_RECEIVE_COUNT}, and this one \
                 is {max_receive_count}"
            ));
        }
        let fifo = is_fifo(key);
        if fifo != is_fifo(&target) {
            let kind = if fifo { "FIFO" } else { "standard" };
            return Err(format!(
                "the dead-letter queue of a {kind} queue must be a {kind} queue too, and \
                 {dead_letter:?} is not"
            ));
        }
        let mut account = self.account();
        let _ = queue_of(&mut account, &target);
        queue_of(&mut account, key).redrive = Some(DeadLetter {
            max_receive_count: max_receive_count.get(),
            queue: target,
            name: dead_letter.to_owned(),
        });
        drop(account);
        Ok(())
    }

    /// `SendMessage` to the queue keyed `key`, recorded under `name`.
    pub(crate) fn send(&self, name: &str, key: &str, outbound: Outbound) -> Result<(), String> {
        check_message(&outbound, SQS_MAX_MESSAGE)?;
        match (is_fifo(key), &outbound.fifo) {
            (true, None) => {
                return Err("a FIFO queue takes a message only with a MessageGroupId".to_owned());
            }
            (false, Some(_)) => {
                return Err(
                    "MessageGroupId and MessageDeduplicationId are for FIFO queues only".to_owned(),
                );
            }
            _ => {}
        }
        let entry = log_entry(name, &outbound);
        let mut account = self.account();
        account.log.entry(name.to_owned()).or_default().push(entry);
        self.deliver(&mut account, key, outbound);
        drop(account);
        Ok(())
    }

    /// `Publish` to the topic keyed `topic`, recorded under `name`: every queue subscribed to the
    /// topic receives a copy, as raw message delivery hands it over.
    pub(crate) fn publish_topic(
        &self,
        name: &str,
        topic: &str,
        outbound: &Outbound,
    ) -> Result<(), String> {
        check_message(outbound, SNS_MAX_MESSAGE)?;
        match (is_fifo(topic), &outbound.fifo) {
            (true, None) => {
                return Err("a FIFO topic takes a message only with a MessageGroupId".to_owned());
            }
            (false, Some(_)) => {
                return Err(
                    "MessageGroupId and MessageDeduplicationId are for FIFO topics only".to_owned(),
                );
            }
            _ => {}
        }
        let mut account = self.account();
        let entry = log_entry(name, outbound);
        account.log.entry(name.to_owned()).or_default().push(entry);
        let endpoints = account.topics.get(topic).cloned().unwrap_or_default();
        for endpoint in endpoints {
            let mut copy = outbound.clone();
            if !is_fifo(&endpoint.queue) {
                // A standard queue has no groups; the copy it receives carries none.
                copy.fifo = None;
            }
            let entry = log_entry(&endpoint.name, &copy);
            account
                .log
                .entry(endpoint.name.clone())
                .or_default()
                .push(entry);
            self.deliver(&mut account, &endpoint.queue, copy);
        }
        drop(account);
        Ok(())
    }

    /// `Subscribe` of the queue keyed `queue` (named `name` by the service) to `topic`.
    pub(crate) fn subscribe_topic(
        &self,
        topic: &str,
        queue: &str,
        name: &str,
    ) -> Result<(), String> {
        if !is_fifo(topic) && is_fifo(queue) {
            return Err(format!(
                "a FIFO queue cannot subscribe to a standard topic, and {name:?} is FIFO"
            ));
        }
        let mut account = self.account();
        let endpoints = account.topics.entry(topic.to_owned()).or_default();
        if !endpoints.iter().any(|endpoint| endpoint.queue == queue) {
            endpoints.push(Endpoint {
                queue: queue.to_owned(),
                name: name.to_owned(),
            });
        }
        drop(account);
        Ok(())
    }

    /// Puts `outbound` on the queue keyed `key`, unless a FIFO queue has seen its deduplication
    /// id within the window.
    fn deliver(&self, account: &mut Account, key: &str, outbound: Outbound) {
        let queue = queue_of(account, key);
        if let Some(settings) = &outbound.fifo
            && queue.duplicate(&settings.deduplication)
        {
            return;
        }
        let seq = self.next();
        let stored = Stored {
            seq,
            outbound,
            receives: 0,
        };
        self.enqueue(queue, stored);
    }

    /// Makes `stored` visible on `queue`, where a subscription can receive it.
    fn enqueue(&self, queue: &mut Queue, stored: Stored) {
        if queue.subscriptions == 0 {
            return;
        }
        if let Some(coordinator) = self.coordinator() {
            coordinator.enqueued();
        }
        queue.visible.insert(stored.seq, stored);
        queue.notify.notify_waiters();
    }

    /// How many messages a receive of up to `max` on `key` would hand over now.
    pub(crate) fn receivable(&self, key: &str, max: usize) -> usize {
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        let blocked = queue.blocked_groups();
        let receivable = queue
            .visible
            .values()
            .filter(|stored| group_of(stored).is_none_or(|group| !blocked.contains(group)))
            .take(max)
            .count();
        drop(account);
        receivable
    }

    /// `ReceiveMessage` of up to `max` messages from `key`.
    ///
    /// A message whose receives have run the queue's redrive policy out moves to the dead-letter
    /// queue here instead of being handed over, which is where SQS moves it. On a FIFO queue a
    /// group with a message in flight hands out nothing, and one receive hands out a group's
    /// messages in order.
    pub(crate) fn receive(&self, key: &str, max: usize) -> Vec<Received> {
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        let blocked = queue.blocked_groups();
        let mut received = Vec::new();
        let mut spent = Vec::new();
        while received.len() < max {
            let Some(seq) = queue.next_receivable(&blocked) else {
                break;
            };
            let Some(mut stored) = queue.visible.remove(&seq) else {
                break;
            };
            if let Some(redrive) = &queue.redrive
                && stored.receives >= redrive.max_receive_count
            {
                spent.push((redrive.clone(), stored));
                continue;
            }
            stored.receives = stored.receives.saturating_add(1);
            let receipt = self.next();
            received.push(Received {
                message: wire_message(&stored.outbound, Some(receipt), Some(stored.receives)),
                receipt,
            });
            queue.in_flight.insert(
                receipt,
                InFlight {
                    stored,
                    generation: 0,
                },
            );
        }
        for (redrive, stored) in spent {
            if let Some(coordinator) = self.coordinator() {
                coordinator.consumed();
            }
            let entry = log_entry(&redrive.name, &stored.outbound);
            account
                .log
                .entry(redrive.name.clone())
                .or_default()
                .push(entry);
            let target = queue_of(&mut account, &redrive.queue);
            self.enqueue(target, stored);
        }
        drop(account);
        received
    }

    /// `DeleteMessage`. A receipt that is no longer in flight deletes nothing and succeeds, as
    /// a stale receipt handle does on a standard queue.
    pub(crate) fn delete(&self, key: &str, receipt: u64) {
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        if queue.in_flight.remove(&receipt).is_some() {
            // A FIFO group waiting on this message is receivable now.
            queue.notify.notify_waiters();
        }
        drop(account);
    }

    /// `ChangeMessageVisibility`: the message becomes visible again after `visibility`.
    pub(crate) fn change_visibility(
        self: &Arc<Self>,
        key: &str,
        receipt: u64,
        visibility: Duration,
    ) -> Result<(), String> {
        if visibility.as_secs() > MAX_VISIBILITY_SECS {
            return Err(format!(
                "a visibility timeout is at most {MAX_VISIBILITY_SECS} seconds"
            ));
        }
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        let Some(entry) = queue.in_flight.get_mut(&receipt) else {
            return Err(
                "the message is not in flight: its receipt handle is no longer valid".to_owned(),
            );
        };
        if visibility.is_zero() {
            if let Some(entry) = queue.in_flight.remove(&receipt) {
                self.enqueue(queue, entry.stored);
            }
            return Ok(());
        }
        entry.generation += 1;
        let generation = entry.generation;
        drop(account);
        self.lapse_after(visibility, key.to_owned(), receipt, generation);
        Ok(())
    }

    /// Arms the timer that makes an in-flight message visible again.
    fn lapse_after(self: &Arc<Self>, delay: Duration, key: String, receipt: u64, generation: u64) {
        // Why a runtime check: a delivery can be dropped after the test's runtime is gone, and
        // then there is no clock left to arm and no receiver left to hand the message to.
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let bus = Arc::clone(self);
        let lapse = move || bus.lapse(&key, receipt, generation);
        match self.coordinator() {
            Some(coordinator) => coordinator.schedule_redelivery(delay, lapse),
            None => drop(runtime.spawn(async move {
                tokio::time::sleep(delay).await;
                lapse();
            })),
        }
    }

    /// The visibility timeout armed at `generation` ran out.
    fn lapse(&self, key: &str, receipt: u64, generation: u64) {
        let mut account = self.account();
        let queue = queue_of(&mut account, key);
        if queue
            .in_flight
            .get(&receipt)
            .is_some_and(|entry| entry.generation == generation)
            && let Some(entry) = queue.in_flight.remove(&receipt)
        {
            self.enqueue(queue, entry.stored);
        }
        drop(account);
    }

    /// Every message that arrived at `name`, in arrival order.
    pub(crate) fn published(&self, name: &str) -> Vec<RawMessage> {
        self.account().log.get(name).cloned().unwrap_or_default()
    }
}

/// The queue keyed `key`, created on first use: every queue a service names exists here.
fn queue_of<'a>(account: &'a mut Account, key: &str) -> &'a mut Queue {
    account
        .queues
        .entry(key.to_owned())
        .or_insert_with(|| Queue::new(is_fifo(key)))
}

impl fmt::Debug for Bus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let account = self.account();
        f.debug_struct("Bus")
            .field("queues", &account.queues.len())
            .field("topics", &account.topics.len())
            .finish_non_exhaustive()
    }
}
