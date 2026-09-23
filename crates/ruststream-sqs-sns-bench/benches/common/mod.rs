//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the queue fill, the latch a handler counts deliveries down on, and the
//! measurement configuration. The method is the core's, described in its `benches/common` and on
//! the [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app on [`SqsBroker`], built with the
//! constructor a service uses and pointed at the stand `just bench-code` starts, then started
//! through [`RustStream::start`] on a single-threaded runtime. Every delivery is a real
//! `ReceiveMessage` share, a real `DeleteMessage` for its `ack` and, in the reply scenario, a real
//! `SendMessage`, over HTTP to the emulator on the loopback.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: loading the AWS configuration, resolving the queue, reading
//! its visibility timeout and opening the subscription.
//!
//! What a body measures is the start and the drain, in two regions. The queue is filled between
//! them from a thread of its own, with its own runtime and the raw `aws-sdk-sqs` client, and the
//! fill returns once the queue has accepted every message. The service's runtime runs only inside
//! a region, so nothing is consumed while the queue fills and nothing of the fill is counted.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything the service's thread runs inside the region is counted: the framework,
//! this crate, and the work of `aws-sdk-sqs` and its HTTP stack on that thread (building and
//! signing a request, writing it, parsing the answer). Threads the client runs on its own are not
//! counted, and neither is the emulator's side of a round trip. [`measure`] is the only frame
//! that carries its name, because a toggle on a name that also appears inside closure types
//! switches collection off again one frame deeper. DHAT is pointed at the same frame; the number
//! read is `Total blocks`, allocations per run.
//!
//! # Real I/O in the count
//!
//! How the emulator answers and how the bytes of an answer arrive move the count a little from
//! one run to the next: over four runs the longest run's instructions moved by up to 0.4 percent
//! and its allocations by up to three blocks in 1.7 million. Each allocation floor is therefore
//! the highest total observed plus a tenth of a percent, which one more allocation per message
//! still exceeds; each scenario states its range next to its floor. The instruction limit stays at
//! two percent, five times the movement seen.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use futures::{StreamExt, stream};
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_sqs_sns::SqsBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw, so a number taken with it on is not the production path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The queue the reply scenario answers on. A reply type names it in its own
/// `#[outgoing(name = ..)]` attribute, which takes a literal.
pub const REPLIES: &str = "confirmations";

/// The region the stand is addressed in. A local stack ignores it and the SDK insists on one.
const REGION: &str = "us-east-1";

/// The per-attempt timeout the crate configures, repeated for the client that fills the queue so
/// both talk to the stand with the same settings.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(25);

/// Messages one `SendMessageBatch` carries, the protocol maximum.
const FILL_BATCH: usize = 10;

/// `SendMessageBatch` calls the fill keeps in flight at once.
const FILL_LANES: usize = 8;

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate, the client and the framework rather than about `serde_json`'s
/// string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays under a minute of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what the rest of the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) allocates once; together they are
/// the hard limit that run is held to, so the run fails when the path allocates more than it does
/// today. Both are floors the code is held to, so a number that goes down is lowered here in the
/// same change. The instruction limit is relative: `just bench-code --save-baseline=main` records
/// a baseline and `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    // The runner clears the environment of the measured process, so the stand's address is
    // handed over by name.
    if let Ok(endpoint) = env::var(ENDPOINT) {
        config.env(ENDPOINT, endpoint);
    }
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION_FRAME}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION_FRAME.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION_FRAME: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and nothing of the
/// service runs outside the regions that drive it.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
fn json_body() -> String {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}")
}

/// The variable naming the stand a run talks to: `just bench-code` starts the stand and sets it.
const ENDPOINT: &str = "SQS_TEST_ENDPOINT";

/// The stand a run talks to.
fn endpoint() -> String {
    env::var(ENDPOINT)
        .expect("SQS_TEST_ENDPOINT names the stand to measure against; `just bench-code` sets it")
}

/// The queue this process's run consumes, installed before the service is built.
///
/// A `#[subscriber(..)]` attribute is evaluated when the handler is mounted, inside the builder
/// of the run that is starting, so the run installs its queue here first and the subscription
/// the runtime opens is the one the fill publishes to.
static INPUT: Mutex<Option<String>> = Mutex::new(None);

/// The URL of this run's input queue. `SqsQueue` passes a URL through as it is.
pub fn input() -> String {
    INPUT
        .lock()
        .expect("the queue cell is never held across a panic")
        .clone()
        .expect("a run installs its queue before it builds the service")
}

/// Runs `work` on a thread of its own, with a runtime of its own, and waits for it.
///
/// Setting the stand up, filling the queue and taking the queue down again all go through here,
/// so none of it runs on the service's thread or on its runtime.
fn aside<Output, Work>(work: Work) -> Output
where
    Output: Send,
    Work: FnOnce(&Runtime) -> Output + Send,
{
    thread::scope(|scope| {
        scope
            .spawn(|| work(&runtime()))
            .join()
            .expect("the side thread finishes")
    })
}

/// The AWS configuration the side thread's raw client uses.
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

/// The URL the stand answered with, rebased onto the endpoint this run talks to, as the crate
/// rebases whatever `GetQueueUrl` returns.
fn rebase(endpoint: &str, url: &str) -> String {
    url.split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map_or_else(
            || url.to_owned(),
            |(_, path)| format!("{}/{path}", endpoint.trim_end_matches('/')),
        )
}

/// The queues of one run on the stand, and the client that fills and removes them.
struct Stand {
    endpoint: String,
    config: SdkConfig,
    queue_url: String,
}

impl Stand {
    /// Creates a queue no other run owns, and the reply queue the reply scenario answers on.
    fn open() -> Self {
        let endpoint = endpoint();
        aside(|runtime| {
            runtime.block_on(async {
                let config = sdk_config(&endpoint).await;
                let client = Client::new(&config);
                let stamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|since| since.as_nanos())
                    .unwrap_or_default();
                let mut urls = Vec::with_capacity(2);
                for name in [format!("rs-bench-orders-{stamp}"), REPLIES.to_owned()] {
                    let created = client
                        .create_queue()
                        .queue_name(name)
                        .send()
                        .await
                        .expect("the stand creates the queue");
                    urls.push(
                        created
                            .queue_url()
                            .expect("CreateQueue answers with the URL")
                            .to_owned(),
                    );
                }
                let queue_url = rebase(&endpoint, &urls[0]);
                Self {
                    endpoint: endpoint.clone(),
                    config,
                    queue_url,
                }
            })
        })
    }

    /// Publishes `count` bodies to the run's queue and returns once the queue has accepted each.
    ///
    /// Part of every setup, never of a measured region: the service's runtime is not driven while
    /// this runs, so the deliveries are in the queue before the drain starts and what the drain
    /// pays for is delivery, not production.
    fn fill(&self, count: usize) {
        aside(|runtime| {
            runtime.block_on(async {
                let client = Client::new(&self.config);
                let body = json_body();
                let batches = (0..count).step_by(FILL_BATCH).map(|first| {
                    let entries = (first..count.min(first + FILL_BATCH))
                        .map(|index| {
                            SendMessageBatchRequestEntry::builder()
                                .id(index.to_string())
                                .message_body(body.clone())
                                .build()
                                .expect("an entry names its id and its body")
                        })
                        .collect::<Vec<_>>();
                    let client = &client;
                    async move {
                        let sent = client
                            .send_message_batch()
                            .queue_url(&self.queue_url)
                            .set_entries(Some(entries))
                            .send()
                            .await
                            .expect("the stand answers the send");
                        assert!(
                            sent.failed().is_empty(),
                            "the queue refused part of a batch: {:?}",
                            sent.failed()
                        );
                    }
                });
                stream::iter(batches)
                    .buffer_unordered(FILL_LANES)
                    .collect::<()>()
                    .await;
            });
        });
    }

    /// Deletes the run's queue, so a later run starts from an empty one.
    fn remove(self) {
        aside(|runtime| {
            runtime.block_on(async {
                Client::new(&self.config)
                    .delete_queue()
                    .queue_url(&self.queue_url)
                    .send()
                    .await
                    .expect("the stand deletes the queue");
            });
        });
    }
}

/// A service that is built but not started, and the queue it will drain.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    stand: Stand,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<SqsBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, pointed at a fresh queue on the stand,
/// ready to be started by the body.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let stand = Stand::open();
    *INPUT
        .lock()
        .expect("the queue cell is never held across a panic") = Some(stand.queue_url.clone());
    let latch = Latch::default();
    let broker = SqsBroker::new()
        .endpoint(stand.endpoint.clone())
        .test_credentials()
        .region(REGION);
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker, mount);
    Pending {
        runtime: runtime(),
        latch,
        stand,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        stand,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    stand.fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    black_box(&latch);
    // The service stops before its queue goes, so no receive is left asking for a deleted queue.
    drop(running);
    drop(runtime);
    stand.remove();
}
