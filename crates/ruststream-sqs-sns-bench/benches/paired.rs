// The benchmark is a binary of its own, not library surface: a measured loop panics on a broker
// fault rather than threading a `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate's consumer and publisher cost over the `aws-sdk-sqs` client they wrap, and what
//! the runtime costs on top of them.
//!
//! One scenario, run three times over, as three loops that differ in one thing each: what carries
//! the messages.
//!
//! - **raw** - the `aws-sdk-sqs` client, driven directly.
//! - **adapter** - this crate's own types and nothing above them: the broker, the queue
//!   descriptor, the stream the subscription yields, the delivery's `payload` and its `ack`, and
//!   [`SqsPublisher`] on the way in. No handler, no app, no router, no dispatch.
//! - **framework** - the whole service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! Two differences come out of that. `adapter` against `raw` is what this crate's consumer and
//! publisher cost over the client they wrap, which is the question this repository answers.
//! `framework` against `adapter` is what the runtime costs on top, over this broker in
//! particular - worth publishing here because a runtime share that differs from broker to broker
//! is a fact about how the two meet, not about the core.
//!
//! The procedure is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! A fresh queue is created, the consumer is attached to it, publishers then feed it, and the
//! window runs from the first delivery to the end of the last one's handling. Creating the queue,
//! building the clients and opening the subscription are startup cost and sit outside it. The
//! queue goes away when the run ends, so a run never sees what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw half's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three loops are interleaved in that order, round after round - raw, adapter, framework,
//! raw, adapter, framework - and each reports its best, median and worst round. The best is the
//! headline: noise only ever slows a run down, so the fastest round is the closest to the
//! undisturbed cost. The distance between the best and the worst is the noise a difference has to
//! clear. Running one loop to the end and then the next would charge every drift of the machine to
//! whichever ran last.
//!
//! # Long polling is part of the pair
//!
//! [`WAIT`] is the receive wait all three loops ask for, and it is the crate's own default and
//! the protocol maximum. On a queue that always has a backlog it never elapses - a receive
//! answers as soon as there is anything to answer with - but it decides what an empty queue costs,
//! so a round whose loops disagreed about it would be measuring polling policy instead of code.
//!
//! # What the numbers do not say
//!
//! Every delivery is settled with its own `DeleteMessage`, in all three loops alike, because that
//! is where this crate acks. One HTTP round trip per message is the floor of this scenario, so the
//! honest outcome here is a verdict rather than a percentage.
//!
//! # How the broker-bound flag is decided
//!
//! It is measured, never inferred. [`round_trip`] times the smallest request the stand answers, on
//! a client of its own and outside every pair, and [`round_trips_per_delivery`] counts what a
//! delivery charges the consumer: its own delete, plus its tenth of the receive that carried its
//! batch over. A row is flagged when those round trips account for at least
//! [`BROKER_BOUND_SHARE`] of the time a delivery took, because then the code's work happened
//! inside a wait the raw client was already paying, and the differences between the loops are a
//! lower bound on what that code costs rather than a measurement of it.
//!
//! A publisher that never had to wait for its consumer is not the signal, whatever it looks like:
//! a publisher running several sends at once outruns a consumer deleting one at a time by
//! construction, so it would report the consumer as the limit in exactly the run where the
//! consumer sat on the socket throughout.
//!
//! The raw loop keeps one receive in flight while it settles the batch it already holds, the way
//! this crate's own pump does. A baseline that waited for each receive in turn would report the
//! absence of that pipelining as this crate's advantage, which is a statement about prefetching
//! rather than about this crate.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName, QueueAttributeName};
use futures::StreamExt;
use futures::future::join_all;
use ruststream::runtime::RunningApp;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::{SqsPublishOptions, SqsPublisher};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, mpsc};
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. With the framework's harness feature compiled in, this crate
// swaps its transport for an in-process stand-in, so a number taken with it on is not the
// production path. The benchmark lives in a package of its own for the same reason:
// `ruststream-sqs-sns`'s dev-dependencies enable that feature through the conformance harness, and
// a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// The region both halves address. A local stack ignores it and the SDK insists on one.
const REGION: &str = "us-east-1";
/// The per-attempt timeout the crate configures, repeated here so the raw client is the same
/// client: a long poll waits up to [`WAIT`], and an attempt timeout below that kills every
/// receive.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(25);
/// The receive wait both halves ask for: the crate's default and the protocol maximum.
const WAIT: Duration = Duration::from_secs(20);
/// Messages one `ReceiveMessage` asks for. Ten is the protocol maximum, and it is what this
/// crate's subscription asks for, so the raw half asks for it too.
const BATCH: i32 = 10;
/// Sends in flight while a run is being filled.
///
/// Both halves publish one message per request, because that is the only shape this crate's
/// publisher has; running several at once is what keeps the consumer, and not the publisher, the
/// slower half of every run.
const PUBLISHERS: usize = 8;
/// Message groups a FIFO run spreads its bodies over.
///
/// A FIFO queue holds back a group with a delivery in flight, so a run over a handful of groups
/// would measure that hold rather than the adapter: wide enough that a receive always has
/// untouched groups to answer from.
const GROUPS: usize = 64;

/// Deliveries the probe run takes to measure the raw half's rate.
const PROBE_MESSAGES: usize = 1_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a stand an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 100_000;
/// Rounds run. Each loop reports its best, median and worst round.
const PAIRS: usize = 3;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// How far the publishers may run ahead of the consumer, in messages. The cap is what keeps the
/// queue from growing without bound on a long run.
const IN_FLIGHT: usize = 4_096;
/// How long a run may go without a delivery before it is called stuck. Above [`WAIT`], so a long
/// poll that runs its full course is not mistaken for a stall.
const STALL: Duration = Duration::from_secs(60);
/// Requests the round-trip probe makes on one client before the pairs start.
///
/// Every request here is an HTTP round trip rather than a protocol ping, so thousands settle the
/// mean that tens of thousands would settle on a framed protocol, and the probe stays under a
/// handful of seconds.
const ROUND_TRIP_CALLS: usize = 2_000;
/// How much of the time a delivery takes has to be round trips for the row to be reported as
/// paced by the transport rather than by the code.
const BROKER_BOUND_SHARE: f64 = 0.5;

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> String {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of handling the last.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What the measured half of one run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// How many deliveries the consumer still had ahead of it when the publishers sent their last
    /// body. Zero would mean publishing was the slower of the two, and the run a measurement of
    /// the publisher.
    outstanding: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }
}

/// The AWS configuration both halves use, built once and shared.
///
/// The adapter half takes it through `SqsBroker::from_config`, so "the same client configuration"
/// is not a promise this file makes and then has to keep - it is one value.
async fn sdk_config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new(REGION))
        .test_credentials()
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(ATTEMPT_TIMEOUT)
                .build(),
        )
        .load()
        .await
}

/// A queue of this run's own, created and addressed at the endpoint both halves talk to.
async fn create_queue(client: &Client, endpoint: &str, scenario: Scenario) -> String {
    let mut create = client.create_queue().queue_name(fresh_queue(scenario));
    if scenario.fifo() {
        create = create.attributes(QueueAttributeName::FifoQueue, "true");
    }
    let url = create
        .send()
        .await
        .expect("the queue is created")
        .queue_url()
        .expect("CreateQueue answers with the URL")
        .to_owned();
    rebase(endpoint, &url)
}

async fn delete_queue(client: &Client, queue_url: &str) {
    client
        .delete_queue()
        .queue_url(queue_url)
        .send()
        .await
        .expect("the queue is deleted");
}

/// The URL the service answered with, rebased onto the endpoint this run talks to.
///
/// The crate does the same to whatever `GetQueueUrl` returns, because local stands hand back a
/// host of their own choosing. Doing it here too means both halves address the queue with one
/// string.
fn rebase(endpoint: &str, url: &str) -> String {
    url.split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map_or_else(
            || url.to_owned(),
            |(_, path)| format!("{}/{path}", endpoint.trim_end_matches('/')),
        )
}

/// A queue name no other run owns.
fn fresh_queue(scenario: Scenario) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    format!("rs-bench-{stamp}{}", scenario.suffix())
}

// ---------------------------------------------------------------------------------------------
// Filling a run
// ---------------------------------------------------------------------------------------------

/// Which publisher a half fills its queue through: the client, or this crate's.
#[derive(Clone, Copy, Debug)]
enum Producer<'a> {
    Client(&'a Client),
    Adapter(&'a SqsPublisher),
}

impl Producer<'_> {
    /// One body, one request, on either path. The FIFO settings are spelled out on both, so the
    /// service sees the same send whichever made it.
    async fn send(self, queue_url: &str, body: &str, fifo: Option<(String, String)>) {
        match self {
            Self::Client(client) => {
                let mut call = client
                    .send_message()
                    .queue_url(queue_url)
                    .message_body(body);
                if let Some((group, deduplication)) = fifo {
                    call = call
                        .message_group_id(group)
                        .message_deduplication_id(deduplication);
                }
                call.send().await.expect("the queue accepts the send");
            }
            Self::Adapter(publisher) => {
                let options = fifo.map(|(group, deduplication)| {
                    SqsPublishOptions::default()
                        .group_id(group)
                        .deduplication_id(deduplication)
                });
                publisher
                    .publish(
                        OutgoingMessage::new(queue_url, body.as_bytes()),
                        options.as_ref(),
                    )
                    .await
                    .expect("the queue accepts the send");
            }
        }
    }
}

/// Fills the queue from [`PUBLISHERS`] sends at once, never letting more than [`IN_FLIGHT`]
/// messages the consumer has not reached yet pile up.
///
/// Answers with what the consumer still had ahead of it when the last body went out, which is how
/// a run proves the consumer was the slower half and therefore the one it measured.
async fn publish_all(
    producer: Producer<'_>,
    queue_url: &str,
    scenario: Scenario,
    messages: usize,
    run: &Run,
) -> usize {
    let body = json_body(BODY_BYTES);
    let lanes = (0..PUBLISHERS).map(|lane| {
        let body = &body;
        async move {
            let mut index = lane;
            while index < messages {
                while index.saturating_sub(run.handled()) > IN_FLIGHT {
                    sleep(Duration::from_micros(500)).await;
                }
                // The bodies are identical by design, so deduplication has to come from an id of
                // the message's own or the queue would keep one body out of every run.
                let fifo = scenario
                    .fifo()
                    .then(|| (format!("g{}", index % GROUPS), format!("d{index}")));
                producer.send(queue_url, body, fifo).await;
                index += PUBLISHERS;
            }
        }
    });
    join_all(lanes).await;
    messages - run.handled()
}

// ---------------------------------------------------------------------------------------------
// The adapter half: this crate's own consumer and publisher, and nothing above them
// ---------------------------------------------------------------------------------------------

async fn adapter(
    config: &SdkConfig,
    queue_url: &str,
    scenario: Scenario,
    messages: usize,
) -> Sample {
    let connected = SqsBroker::from_config(config.clone())
        .connect()
        .await
        .expect("the broker connects");
    let subscriber = connected
        .subscribe_queue(SqsQueue::new(queue_url).wait(WAIT))
        .await
        .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut subscriber = subscriber;
            let mut stream = pin!(subscriber.stream());
            while let Some(delivery) = stream.next().await {
                let delivery = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(delivery.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The window closes before the acknowledgement, as it does on the raw half.
                let done = run.arrived();
                delivery.ack().await.expect("the ack reaches the queue");
                if done {
                    break;
                }
            }
        }
    });

    let publisher = connected.publisher();
    let outstanding = publish_all(
        Producer::Adapter(&publisher),
        queue_url,
        scenario,
        messages,
        &run,
    )
    .await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let sample = Sample {
        window: run.window(),
        outstanding,
    };
    connected.shutdown().await.expect("the broker shuts down");
    sample
}

// ---------------------------------------------------------------------------------------------
// The framework loop: the service a user writes, started through the real runtime
// ---------------------------------------------------------------------------------------------

/// The queue URL the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its queue here first, so the
/// subscription the runtime opens is the one this run publishes to.
static QUEUE: Mutex<Option<String>> = Mutex::new(None);

fn install(queue_url: &str) {
    *QUEUE
        .lock()
        .expect("the queue cell is never held across a panic") = Some(queue_url.to_owned());
}

fn installed() -> String {
    QUEUE
        .lock()
        .expect("the queue cell is never held across a panic")
        .clone()
        .expect("a run installs its queue before it builds the service")
}

// The mount is the same descriptor the adapter loop opens by hand, so what separates the two runs
// is the runtime between the subscription and this body.
#[subscriber(SqsQueue::new(installed()).wait(WAIT))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(broker: SqsBroker, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("sqs-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(broker, |b| {
            b.include(consume);
        })
        .start()
        .await
        .expect("the service starts")
}

async fn framework(
    config: &SdkConfig,
    queue_url: &str,
    scenario: Scenario,
    messages: usize,
) -> Sample {
    let broker = SqsBroker::from_config(config.clone());
    // Taken before the app consumes the broker: the handle shares its connection cell and resolves
    // when the runtime connects, which is the documented way a service publishes from outside a
    // handler.
    let publisher = broker.publisher();

    let run = Run::new(messages);
    install(queue_url);
    let app = start(broker, run.clone()).await;
    let outstanding = publish_all(
        Producer::Adapter(&publisher),
        queue_url,
        scenario,
        messages,
        &run,
    )
    .await;
    drain(&run, "framework").await;
    app.shutdown().await.expect("the service stops");
    Sample {
        window: run.window(),
        outstanding,
    }
}

// ---------------------------------------------------------------------------------------------
// The raw loop: the same work on the client this crate wraps
// ---------------------------------------------------------------------------------------------

/// The receive this crate's own pump repeats, spelled out so both halves ask the queue for the
/// same thing: ten messages, the same wait, every attribute, and no visibility timeout of its own
/// (the queue's governs, on both halves).
fn receive(
    client: &Client,
    queue_url: &str,
) -> aws_sdk_sqs::operation::receive_message::builders::ReceiveMessageFluentBuilder {
    client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(BATCH)
        .wait_time_seconds(WAIT.as_secs() as i32)
        .message_attribute_names("All")
        .message_system_attribute_names(MessageSystemAttributeName::All)
}

/// Keeps one receive in flight while the consumer settles the batch it already holds, which is
/// what this crate's subscription does. The channel holds one batch, so the pump never runs more
/// than one receive ahead.
async fn receive_pump(client: Client, queue_url: String, out: mpsc::Sender<Vec<Message>>) {
    loop {
        let answered = tokio::select! {
            biased;
            () = out.closed() => break,
            answer = receive(&client, &queue_url).send() => answer,
        };
        let batch = answered
            .expect("the queue answers the receive")
            .messages
            .unwrap_or_default();
        if batch.is_empty() {
            continue;
        }
        if out.send(batch).await.is_err() {
            break;
        }
    }
}

async fn raw(config: &SdkConfig, queue_url: &str, scenario: Scenario, messages: usize) -> Sample {
    let client = Client::new(config);
    let (tx, mut rx) = mpsc::channel(1);
    let pump = tokio::spawn(receive_pump(client.clone(), queue_url.to_owned(), tx));

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let client = client.clone();
        let queue_url = queue_url.to_owned();
        let run = run.clone();
        async move {
            'batches: while let Some(batch) = rx.recv().await {
                for message in batch {
                    let body = message.body().unwrap_or_default();
                    let order: Order =
                        serde_json::from_slice(body.as_bytes()).expect("the body decodes");
                    black_box((order.id, order.quantity));
                    let done = run.arrived();
                    client
                        .delete_message()
                        .queue_url(&queue_url)
                        .receipt_handle(message.receipt_handle().expect("a delivery has a receipt"))
                        .send()
                        .await
                        .expect("the queue accepts the delete");
                    if done {
                        break 'batches;
                    }
                }
            }
        }
    });

    let outstanding = publish_all(
        Producer::Client(&client),
        queue_url,
        scenario,
        messages,
        &run,
    )
    .await;
    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");
    pump.abort();
    Sample {
        window: run.window(),
        outstanding,
    }
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Standard,
    Fifo,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Standard => "Standard queue, 512 B JSON, delete per message",
            Self::Fifo => "FIFO queue, 64 message groups, 512 B JSON, delete per message",
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Standard => "",
            Self::Fifo => ".fifo",
        }
    }

    const fn fifo(self) -> bool {
        matches!(self, Self::Fifo)
    }
}

/// What carries the messages in one run.
#[derive(Clone, Copy, Debug)]
enum Loop {
    Raw,
    Adapter,
    Framework,
}

impl Loop {
    const fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Adapter => "adapter",
            Self::Framework => "framework",
        }
    }
}

/// What every run of this process shares: the endpoint, the configuration both halves are built
/// from, and the client that creates the queues.
#[derive(Debug)]
struct Stand {
    endpoint: String,
    config: SdkConfig,
    admin: Client,
}

impl Stand {
    async fn open(endpoint: String) -> Self {
        let config = sdk_config(&endpoint).await;
        let admin = Client::new(&config);
        Self {
            endpoint,
            config,
            admin,
        }
    }
}

/// One loop of one round, on a queue of its own.
async fn run_loop(stand: &Stand, scenario: Scenario, messages: usize, which: Loop) -> Sample {
    let queue_url = create_queue(&stand.admin, &stand.endpoint, scenario).await;
    let sample = match which {
        Loop::Raw => raw(&stand.config, &queue_url, scenario, messages).await,
        Loop::Adapter => adapter(&stand.config, &queue_url, scenario, messages).await,
        Loop::Framework => framework(&stand.config, &queue_url, scenario, messages).await,
    };
    delete_queue(&stand.admin, &queue_url).await;
    sample
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    pairs: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    /// The whole service against the raw client, which is what the core's schema calls the
    /// overhead.
    overhead_percent: f64,
    /// This crate's own consumer and publisher against the raw client.
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    verdict: &'static str,
    broker_bound: bool,
}

/// How long one request to the stand takes, answer included.
///
/// This is what decides the broker-bound flag, and it is measured rather than inferred. A
/// `GetQueueAttributes` naming no attribute is the smallest thing SQS answers, so what the loop
/// times is the round trip itself rather than the work behind it. It runs outside both halves, on
/// a client and a queue of its own, before any pair starts.
async fn round_trip(config: &SdkConfig, endpoint: &str) -> Duration {
    let client = Client::new(config);
    let queue_url = create_queue(&client, endpoint, Scenario::Standard).await;
    let probe = async || {
        client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .send()
            .await
            .expect("the stand answers the probe");
    };
    // One warm call, so the connection and the first allocations behind it are not charged to the
    // sample.
    probe().await;
    let started = Instant::now();
    for _ in 0..ROUND_TRIP_CALLS {
        probe().await;
    }
    let elapsed = started.elapsed();
    delete_queue(&client, &queue_url).await;
    elapsed / ROUND_TRIP_CALLS as u32
}

/// Round trips the consumer pays per delivery: its own `DeleteMessage`, and its share of the
/// `ReceiveMessage` that brought a batch of [`BATCH`] over.
fn round_trips_per_delivery() -> f64 {
    1.0 + 1.0 / f64::from(BATCH)
}

async fn measure(
    stand: &Stand,
    scenario: Scenario,
    pairs: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its rate is thrown away, and what it measured sets a
    // count that makes every run below last at least `seconds`.
    let probe = run_loop(stand, scenario, PROBE_MESSAGES, Loop::Raw).await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed)",
        scenario.name(),
        probe.rate(PROBE_MESSAGES),
    );

    let mut rates = [
        Vec::with_capacity(pairs),
        Vec::with_capacity(pairs),
        Vec::with_capacity(pairs),
    ];
    for round in 1..=pairs {
        let mut taken = [0.0; 3];
        for (slot, which) in [Loop::Raw, Loop::Adapter, Loop::Framework]
            .into_iter()
            .enumerate()
        {
            let sample = run_loop(stand, scenario, messages, which).await;
            assert!(
                sample.outstanding > 0,
                "the publishers were still sending when the {} consumer ran out of work, so the \
                 run measured publishing rather than consuming",
                which.name(),
            );
            taken[slot] = sample.rate(messages);
        }
        println!(
            "  round {round:>2}: raw {:>8.0} msg/s, adapter {:>8.0} msg/s, framework {:>8.0} msg/s",
            taken[0], taken[1], taken[2],
        );
        for (slot, rate) in taken.into_iter().enumerate() {
            rates[slot].push(rate);
        }
    }

    let [raws, adapters, frameworks] = rates;
    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    let difference = (raw.best - framework.best).abs();
    let adapter_difference = (raw.best - adapter.best).abs();
    // What a delivery cost the raw client, and how much of that was round trips it could not have
    // avoided. A row where the round trips account for most of the time is a row the transport
    // paced: the adapter's work happened inside a wait that was being paid anyway, so the
    // difference is a lower bound on what the adapter costs rather than a measurement of it.
    let per_message = 1.0 / raw.best;
    let in_round_trips = round_trips_per_delivery() * round_trip.as_secs_f64();
    println!(
        "  {:.0} us per delivery, {:.0} us of it round trips",
        per_message * 1e6,
        in_round_trips * 1e6,
    );
    Measured {
        scenario,
        messages,
        pairs,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.best - framework.best) / raw.best * 100.0,
        adapter_overhead_percent: (raw.best - adapter.best) / raw.best * 100.0,
        adapter_verdict: if adapter_difference < raw.spread().max(adapter.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        verdict: if difference < raw.spread().max(framework.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: in_round_trips >= BROKER_BOUND_SHARE * per_message,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    let mut out = format!(
        "{{\n  \"round_trip_us\": {:.1},\n  \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1e6,
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {pairs},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            pairs = row.pairs,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            overhead = row.overhead_percent,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let endpoint = env::var("SQS_TEST_ENDPOINT")
        .expect("SQS_TEST_ENDPOINT names the stand to measure against; `just bench` sets it");
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let stand = runtime.block_on(Stand::open(endpoint));
    let trip = runtime.block_on(round_trip(&stand.config, &stand.endpoint));
    println!(
        "round trip to the stand: {:.0} us over {ROUND_TRIP_CALLS} calls, {:.1} of them per \
         delivery",
        trip.as_secs_f64() * 1e6,
        round_trips_per_delivery(),
    );
    let measured: Vec<Measured> = [Scenario::Standard, Scenario::Fifo]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(&stand, scenario, pairs, seconds, trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%, {}{})",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            -row.adapter_overhead_percent,
            row.framework.best,
            -row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
