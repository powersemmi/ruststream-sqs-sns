//! The broker ladder: [`SqsBroker`] -> [`ConnectedSqsBroker`].
//!
//! Construction is synchronous and I/O-free; credential resolution (env, profile, IMDS, SSO)
//! happens in the consuming [`Broker::connect`], and the connected form holds the live SDK
//! clients directly. One shared cell remains so publishers can be handed out while the
//! application is still being assembled, before `connect` runs.

// Without the `testing` feature the transport has one variant, so a `match` on it has a single
// arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::types::QueueAttributeName;
#[cfg(feature = "testing")]
use ruststream::testing::InProcess;
use ruststream::{
    Broker, BrokerMoves, ConnectedBroker, DeclareRetryError, DefaultPublish, DescribeServer,
    RetryDeclaration, ServerSpec, Subscribe,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OnceCell};

use crate::error::{SqsError, sdk_err};
#[cfg(feature = "testing")]
use crate::in_process::{self, Bus};
#[cfg(feature = "sns")]
use crate::publisher::is_fifo;
use crate::publisher::{SqsPublish, SqsPublisher};
use crate::queue::{Redrive, SqsQueue};
#[cfg(feature = "sns")]
use crate::sns::SnsPublisher;
use crate::subscriber::SqsSubscriber;

/// The client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the SDK clients have no shutdown and keep working
/// forever, and publishers may be handed out before `connect` and outlive `shutdown`
/// (aliasing) - so the dead-connection path must be an explicit runtime flag, or a stale
/// handle would silently succeed against a "closed" broker.
pub(crate) struct Core {
    /// What every handle derived from this state speaks over.
    pub(crate) transport: Transport,
    pub(crate) closed: AtomicBool,
    /// What registrations mounted by a bare queue name declared, by queue name.
    ///
    /// A bare name carries no descriptor to hold the declaration, and the call that takes it
    /// has no queue URL yet, so the policy waits here for the `subscribe` that writes it.
    declared_redrives: StdMutex<HashMap<String, Redrive>>,
    /// Which topics this broker subscribed queues to and which surface each publish went
    /// through, which is how the test harness learns which subscriptions a publish reaches.
    /// Recorded on both transports, because a live test waits on the same answer.
    #[cfg(all(feature = "testing", feature = "sns"))]
    pub(crate) routing: in_process::Routing,
}

/// What a connected broker and every handle paired off it speak over: the AWS clients, or, under
/// the `testing` feature, the in-process account the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the clients themselves and every
/// `match` on it is irrefutable: a production build carries no second transport and no branch
/// to it.
// The live clients are the larger variant on purpose: they are what a production build has, and
// boxing them would put an indirection on the service's own path to shrink a test build.
#[cfg_attr(feature = "testing", allow(clippy::large_enum_variant))]
pub(crate) enum Transport {
    Aws(Aws),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// transport exactly the size of the clients it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Transport>() == size_of::<Aws>());

/// The live SDK clients and the name caches every resolution goes through.
pub(crate) struct Aws {
    pub(crate) sqs: aws_sdk_sqs::Client,
    /// The runtime `connect` ran on, which every task the broker starts on its own behalf (a
    /// subscription's pump, a delivery's visibility extender) is spawned through, whichever
    /// thread subscribes: a task left on a caller's runtime would wait behind that thread's work
    /// and stop with it, while the queue it serves lives on.
    pub(crate) runtime: Handle,
    #[cfg(feature = "sns")]
    pub(crate) sns: aws_sdk_sns::Client,
    pub(crate) endpoint: Option<String>,
    /// Queue-name -> URL cache shared by publishers and subscriptions.
    pub(crate) queue_urls: Mutex<HashMap<String, String>>,
    /// Topic-name -> ARN cache for the SNS publisher.
    #[cfg(feature = "sns")]
    pub(crate) topic_arns: Mutex<HashMap<String, String>>,
}

impl Core {
    fn new(transport: Transport) -> Self {
        Self {
            transport,
            closed: AtomicBool::new(false),
            declared_redrives: StdMutex::new(HashMap::new()),
            #[cfg(all(feature = "testing", feature = "sns"))]
            routing: in_process::Routing::default(),
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<(), SqsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqsError::NotConnected);
        }
        Ok(())
    }

    /// Holds what a bare-name registration declared for `queue` until `subscribe` writes it.
    ///
    /// A registration that declared nothing clears the entry rather than leaving one behind, so
    /// a queue two registrations open does not inherit the first one's policy.
    fn record_redrive(&self, queue: &str, redrive: Option<Redrive>) {
        let mut declared = self
            .declared_redrives
            .lock()
            .expect("sqs declaration mutex poisoned");
        match redrive {
            Some(redrive) => drop(declared.insert(queue.to_owned(), redrive)),
            None => drop(declared.remove(queue)),
        }
    }

    /// The redrive policy a bare-name registration declared for `queue`.
    fn declared_redrive(&self, queue: &str) -> Option<Redrive> {
        self.declared_redrives
            .lock()
            .expect("sqs declaration mutex poisoned")
            .get(queue)
            .cloned()
    }
}

impl Aws {
    /// Resolves a queue name to its URL through the shared cache; URLs pass through. When a
    /// custom endpoint is configured, the returned URL's authority is rewritten onto it, so
    /// the adapter is immune to the host-rewriting strategies local stacks use.
    // The cache guard intentionally spans the resolve so two callers cannot race a double
    // lookup for the same name.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn queue_url(&self, queue: &str) -> Result<String, SqsError> {
        if queue.starts_with("http://") || queue.starts_with("https://") {
            return Ok(queue.to_owned());
        }
        let mut cache = self.queue_urls.lock().await;
        if let Some(url) = cache.get(queue) {
            return Ok(url.clone());
        }
        let resolved = self
            .sqs
            .get_queue_url()
            .queue_name(resource_name(queue))
            .send()
            .await
            .map_err(|e| SqsError::Queue {
                name: queue.to_owned(),
                source: sdk_err(&e),
            })?
            .queue_url()
            .ok_or_else(|| SqsError::Queue {
                name: queue.to_owned(),
                source: Box::from("GetQueueUrl returned no URL"),
            })?
            .to_owned();
        let url = self.rebase_url(resolved);
        cache.insert(queue.to_owned(), url.clone());
        Ok(url)
    }

    /// Resolves a topic name to its ARN through the shared cache; ARNs pass through.
    /// `CreateTopic` is documented idempotent: an existing topic's ARN is returned as-is.
    // The cache guard intentionally spans the resolve so two callers cannot race a double
    // create for the same name.
    #[cfg(feature = "sns")]
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn topic_arn(&self, topic: &str) -> Result<String, SqsError> {
        if topic.starts_with("arn:") {
            return Ok(topic.to_owned());
        }
        let mut cache = self.topic_arns.lock().await;
        if let Some(arn) = cache.get(topic) {
            return Ok(arn.clone());
        }
        let name = resource_name(topic);
        let mut create = self.sns.create_topic().name(&name);
        if is_fifo(&name) {
            // SNS refuses a `.fifo` name outright unless the topic is declared FIFO, so the
            // suffix that makes a queue FIFO makes a topic FIFO here too. Deduplication stays
            // per message: every send this crate makes carries an id of its own.
            create = create.attributes("FifoTopic", "true");
        }
        let created = create.send().await.map_err(|e| SqsError::Admin {
            name: topic.to_owned(),
            source: sdk_err(&e),
        })?;
        let arn = created
            .topic_arn()
            .ok_or_else(|| SqsError::Admin {
                name: topic.to_owned(),
                source: Box::from("CreateTopic returned no ARN"),
            })?
            .to_owned();
        cache.insert(topic.to_owned(), arn.clone());
        Ok(arn)
    }

    /// The queue's ARN, which is how every SQS resource names another one: an SNS subscription
    /// endpoint, a redrive policy's dead-letter target.
    pub(crate) async fn queue_arn(&self, queue: &str, queue_url: &str) -> Result<String, SqsError> {
        self.sqs
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| SqsError::Queue {
                name: queue.to_owned(),
                source: sdk_err(&e),
            })?
            .attributes()
            .and_then(|map| map.get(&QueueAttributeName::QueueArn))
            .cloned()
            .ok_or_else(|| SqsError::Queue {
                name: queue.to_owned(),
                source: Box::from("GetQueueAttributes returned no QueueArn"),
            })
    }

    /// The queue's own visibility timeout, read once when a subscription opens.
    ///
    /// A subscription that names no visibility of its own holds every delivery under this
    /// value, so it is read from the queue rather than assumed: the timeout is the operator's
    /// setting, and a service that substituted its own would shorten it without saying so.
    pub(crate) async fn queue_visibility(
        &self,
        queue: &str,
        queue_url: &str,
    ) -> Result<Duration, SqsError> {
        let attributes = self
            .sqs
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::VisibilityTimeout)
            .send()
            .await
            .map_err(|e| SqsError::Queue {
                name: queue.to_owned(),
                source: sdk_err(&e),
            })?;
        parse_visibility(
            attributes
                .attributes()
                .and_then(|map| map.get(&QueueAttributeName::VisibilityTimeout))
                .map(String::as_str),
        )
        .map_err(|reason| SqsError::Queue {
            name: queue.to_owned(),
            source: reason.into(),
        })
    }

    pub(crate) fn rebase_url(&self, url: String) -> String {
        let Some(endpoint) = &self.endpoint else {
            return url;
        };
        // Keep the path (account/queue), replace scheme+authority with the configured
        // endpoint.
        url.find("//")
            .and_then(|scheme_end| url[scheme_end + 2..].find('/').map(|p| scheme_end + 2 + p))
            .map_or_else(
                || url.clone(),
                |path_start| format!("{}{}", endpoint.trim_end_matches('/'), &url[path_start..]),
            )
    }
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Core");
        match &self.transport {
            Transport::Aws(aws) => debug.field("endpoint", &aws.endpoint),
            #[cfg(feature = "testing")]
            Transport::InProcess(bus) => debug.field("in_process", bus),
        };
        debug
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

/// Reads the `VisibilityTimeout` attribute, which SQS reports as a whole number of seconds.
///
/// SQS sets the attribute on every queue, so an answer without it, or with something that is
/// not a number of seconds, means the timeout is unknown. The subscription then refuses to open
/// instead of holding deliveries under a duration this crate invented.
fn parse_visibility(raw: Option<&str>) -> Result<Duration, String> {
    let Some(raw) = raw else {
        return Err("GetQueueAttributes returned no VisibilityTimeout".to_owned());
    };
    raw.trim()
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| {
            format!(
                "GetQueueAttributes returned VisibilityTimeout {raw:?}, \
                 which is not a whole number of seconds"
            )
        })
}

/// Maps a logical destination name onto a name the services accept: characters outside
/// `[A-Za-z0-9_-]` become `-` (neither SQS nor SNS takes them), and a `.fifo` suffix survives,
/// because that suffix is what makes a queue or a topic FIFO. Subscribers and publishers share
/// this mapping on both services, so a dotted framework name stays routable wherever it is sent.
pub(crate) fn resource_name(logical: &str) -> String {
    let (stem, fifo) = logical
        .strip_suffix(".fifo")
        .map_or((logical, ""), |stem| (stem, ".fifo"));
    let mapped: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("{mapped}{fifo}")
}

/// The redrive policy SQS takes, as the service spells it.
///
/// Built by hand rather than through a serializer: the two values are an ARN the service
/// returned and a number, and neither alphabet reaches a JSON metacharacter.
fn redrive_policy(dead_letter_arn: &str, max_receive_count: u32) -> String {
    format!(
        r#"{{"deadLetterTargetArn":"{dead_letter_arn}","maxReceiveCount":"{max_receive_count}"}}"#
    )
}

/// An Amazon SQS broker (with SNS fan-out publishing on the `sns` feature) for the `RustStream`
/// messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime resolves credentials and
/// builds the clients once at startup via the consuming [`Broker::connect`]. That is what lets
/// a service compose with the synchronous `#[ruststream::app]` builder.
///
/// # Examples
///
/// ```
/// use ruststream_sqs_sns::SqsBroker;
///
/// let broker = SqsBroker::new(); // region and credentials from the environment
/// let local = SqsBroker::new()
///     .endpoint("http://localhost:4566")
///     .test_credentials()
///     .region("us-east-1");
/// # let _ = (broker, local);
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct SqsBroker {
    endpoint: Option<String>,
    region: Option<String>,
    test_credentials: bool,
    sdk_config: Option<SdkConfig>,
    cell: CoreCell,
}

impl SqsBroker {
    /// Records configuration only; region and credentials resolve from the environment on
    /// `connect`. No I/O.
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses an already built AWS config instead of resolving one from the environment.
    pub fn from_config(config: SdkConfig) -> Self {
        Self {
            sdk_config: Some(config),
            ..Self::default()
        }
    }

    /// Overrides the service endpoint (a local stack for development). Queue URLs returned by
    /// the service are rebased onto this endpoint.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Overrides the region.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Uses dummy static credentials, for local stacks that require credentials to be present
    /// but ignore their values.
    pub fn test_credentials(mut self) -> Self {
        self.test_credentials = true;
        self
    }

    /// A publisher sending directly to SQS queues, sharing this broker's connection cell;
    /// buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> SqsPublisher {
        SqsPublisher::new(Arc::clone(&self.cell))
    }
}

impl Broker for SqsBroker {
    type Error = SqsError;
    type Connected = ConnectedSqsBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let core = self
            .cell
            .get_or_try_init(async || {
                let config = if let Some(config) = self.sdk_config.clone() {
                    config
                } else {
                    // BehaviorVersion::latest(): every pinned version eventually deprecates
                    // (which -D warnings turns into a build failure); the consumer who needs a
                    // frozen behaviour passes a prebuilt config via from_config.
                    let mut loader = aws_config::defaults(BehaviorVersion::latest());
                    if let Some(endpoint) = &self.endpoint {
                        loader = loader.endpoint_url(endpoint.clone());
                    }
                    if let Some(region) = &self.region {
                        loader = loader.region(Region::new(region.clone()));
                    }
                    if self.test_credentials {
                        loader = loader.test_credentials();
                    }
                    // A long poll waits up to 20s; the per-attempt timeout must exceed it or
                    // every receive dies (the SDK maintainers' recommended pairing).
                    loader = loader.timeout_config(
                        aws_config::timeout::TimeoutConfig::builder()
                            .operation_attempt_timeout(Duration::from_secs(25))
                            .build(),
                    );
                    loader.load().await
                };
                let sqs = aws_sdk_sqs::Client::new(&config);
                Ok::<_, SqsError>(Arc::new(Core::new(Transport::Aws(Aws {
                    sqs,
                    runtime: Handle::current(),
                    #[cfg(feature = "sns")]
                    sns: aws_sdk_sns::Client::new(&config),
                    endpoint: self
                        .endpoint
                        .clone()
                        .or_else(|| config.endpoint_url().map(str::to_owned)),
                    queue_urls: Mutex::new(HashMap::new()),
                    #[cfg(feature = "sns")]
                    topic_arns: Mutex::new(HashMap::new()),
                }))))
            })
            .await?
            .clone();
        Ok(ConnectedSqsBroker {
            core,
            cell: self.cell,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process account in place of the AWS clients.
///
/// The account reads what it needs from this broker: the endpoint and the region the queue URLs
/// it hands out are built on. Publishers taken from the broker before the transition resolve
/// against it, as they resolve against the clients after `connect`.
///
/// # Errors
///
/// Returns [`SqsError::Config`] when a clone of this broker already connected to AWS: the handles
/// it gave out share one connection, and it cannot be two transports at once.
#[cfg(feature = "testing")]
impl InProcess for SqsBroker {
    // The body awaits nothing, but it has to run where the future is polled: the runtime it
    // captures is the one the harness connects on, and a caller outside any runtime may build
    // the future before handing it to one.
    #[allow(clippy::unused_async_trait_impl)]
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        let region = self
            .region
            .clone()
            .or_else(|| {
                self.sdk_config
                    .as_ref()
                    .and_then(SdkConfig::region)
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "us-east-1".to_owned());
        let endpoint = self.endpoint.clone().or_else(|| {
            self.sdk_config
                .as_ref()
                .and_then(SdkConfig::endpoint_url)
                .map(str::to_owned)
        });
        let fresh = Arc::new(Core::new(Transport::InProcess(Bus::new(
            endpoint.as_deref(),
            &region,
            Handle::current(),
        ))));
        let core = match self.cell.set(Arc::clone(&fresh)) {
            Ok(()) => Ok(fresh),
            Err(_) => self
                .cell
                .get()
                .filter(|core| matches!(core.transport, Transport::InProcess(_)))
                .cloned()
                .ok_or_else(|| {
                    SqsError::Config(
                        "this broker's handles already connected to AWS, so it cannot connect \
                         in process as well"
                            .to_owned(),
                    )
                }),
        };
        core.map(|core| ConnectedSqsBroker {
            core,
            cell: self.cell,
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(SqsBroker);

impl DescribeServer for SqsBroker {
    fn describe_server(&self) -> ServerSpec {
        // An endpoint that carries no host at all (an empty override) says nothing about where
        // clients connect, so the public service is the honest answer for the document.
        let host = self
            .endpoint
            .as_deref()
            .map(ServerSpec::host_from_url)
            .filter(|host| !host.is_empty())
            .unwrap_or_else(|| "sqs.amazonaws.com".to_owned());
        ServerSpec::new(host, "sqs")
    }
}

/// The typed witness that `connect` succeeded: holds the live SDK clients directly.
#[derive(Debug)]
pub struct ConnectedSqsBroker {
    pub(crate) core: Arc<Core>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedSqsBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as
    /// the early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> SqsPublisher {
        SqsPublisher::new(Arc::clone(&self.cell))
    }

    /// An SNS fan-out publisher from the connected form. Available with the `sns` feature.
    #[cfg(feature = "sns")]
    #[must_use]
    pub fn sns_publisher(&self) -> SnsPublisher {
        SnsPublisher::new(Arc::clone(&self.cell))
    }

    /// Subscribes `queue` to `topic` (both names or ARN/URL) with raw message delivery, so
    /// payloads and headers arrive unwrapped as plain SQS messages. Available with the `sns`
    /// feature.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the topic or queue cannot be resolved or the subscription
    /// call fails.
    #[cfg(feature = "sns")]
    pub async fn subscribe_queue_to_topic(&self, topic: &str, queue: &str) -> Result<(), SqsError> {
        self.core.ensure_open()?;
        let aws = match &self.core.transport {
            Transport::Aws(aws) => aws,
            #[cfg(feature = "testing")]
            Transport::InProcess(bus) => {
                return in_process::subscribe_topic(bus, topic, queue)
                    .inspect(|()| self.record_topic_queue(topic, queue));
            }
        };
        let topic_arn = aws.topic_arn(topic).await?;
        let queue_url = aws.queue_url(queue).await?;
        let queue_arn = aws.queue_arn(queue, &queue_url).await?;
        let subscribed = aws
            .sns
            .subscribe()
            .topic_arn(&topic_arn)
            .protocol("sqs")
            .endpoint(queue_arn)
            .attributes("RawMessageDelivery", "true")
            .send()
            .await
            .map(|_| ())
            .map_err(|e| SqsError::Admin {
                name: topic.to_owned(),
                source: sdk_err(&e),
            });
        #[cfg(feature = "testing")]
        if subscribed.is_ok() {
            self.record_topic_queue(topic, queue);
        }
        subscribed
    }

    /// Opens the subscription described by `queue`.
    ///
    /// A descriptor that names no visibility takes the queue's own timeout, read here with one
    /// `GetQueueAttributes` call, so every delivery is held under the value the operator
    /// configured. A registration that declared a cap and a dead-letter destination has them
    /// written onto the queue as its redrive policy first, which is what makes SQS carry a spent
    /// delivery away by itself.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the descriptor is invalid, the registration declared half a
    /// redrive policy, the queue cannot be resolved (or created, when opted in), the redrive
    /// policy cannot be written, the queue's visibility timeout cannot be read, or the broker is
    /// shut down.
    pub async fn subscribe_queue(&self, queue: SqsQueue) -> Result<SqsSubscriber, SqsError> {
        queue.validate()?;
        self.core.ensure_open()?;
        let aws = match &self.core.transport {
            Transport::Aws(aws) => aws,
            #[cfg(feature = "testing")]
            Transport::InProcess(bus) => return in_process::subscribe(bus, &queue),
        };

        let url = if queue.create_value() {
            aws.ensure_queue(queue.queue()).await?
        } else {
            aws.queue_url(queue.queue()).await?
        };
        if let Some(redrive) = queue.redrive()? {
            aws.set_redrive_policy(queue.queue(), &url, &redrive, queue.create_value())
                .await?;
        }
        SqsSubscriber::open(aws, queue.queue(), url, &queue).await
    }
}

impl Aws {
    /// Writes the registration's declaration onto the queue as its redrive policy.
    ///
    /// The dead-letter destination is a queue name like any other, resolved to the ARN the
    /// policy addresses it by. A FIFO queue takes a FIFO dead-letter queue and a standard one a
    /// standard queue; SQS rejects the mismatch and the error carries its words.
    async fn set_redrive_policy(
        &self,
        queue: &str,
        queue_url: &str,
        redrive: &Redrive,
        create_if_missing: bool,
    ) -> Result<(), SqsError> {
        let dead_letter_url = if create_if_missing {
            self.ensure_queue(&redrive.dead_letter).await?
        } else {
            self.queue_url(&redrive.dead_letter).await?
        };
        let dead_letter_arn = self
            .queue_arn(&redrive.dead_letter, &dead_letter_url)
            .await?;
        self.sqs
            .set_queue_attributes()
            .queue_url(queue_url)
            .attributes(
                QueueAttributeName::RedrivePolicy,
                redrive_policy(&dead_letter_arn, redrive.max_receive_count.get()),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| SqsError::Queue {
                name: queue.to_owned(),
                source: sdk_err(&e),
            })
    }

    /// Resolves the queue, creating it when missing. A `.fifo` name creates a FIFO queue with
    /// content-based deduplication.
    async fn ensure_queue(&self, queue: &str) -> Result<String, SqsError> {
        if let Ok(url) = self.queue_url(queue).await {
            return Ok(url);
        }
        let mut create = self.sqs.create_queue().queue_name(resource_name(queue));
        if queue.to_ascii_lowercase().ends_with(".fifo") {
            create = create
                .attributes(QueueAttributeName::FifoQueue, "true")
                .attributes(QueueAttributeName::ContentBasedDeduplication, "true");
        }
        let created = create.send().await.map_err(|e| SqsError::Queue {
            name: queue.to_owned(),
            source: sdk_err(&e),
        })?;
        let url = created.queue_url().ok_or_else(|| SqsError::Queue {
            name: queue.to_owned(),
            source: Box::from("CreateQueue returned no URL"),
        })?;
        let url = self.rebase_url(url.to_owned());
        self.queue_urls
            .lock()
            .await
            .insert(queue.to_owned(), url.clone());
        Ok(url)
    }
}

impl ConnectedBroker for ConnectedSqsBroker {
    type Error = SqsError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        // The SDK clients have no close/flush; requests are synchronous per call, so marking
        // the shared state closed is the whole teardown. Aliased handles error afterwards.
        self.core.closed.store(true, Ordering::Release);
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedSqsBroker {
    type Subscriber = SqsSubscriber;
    // A bare name is a queue, and a queue moves a spent delivery itself, so the by-name form
    // takes the same copy path the descriptor does.
    type Copies = BrokerMoves;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_queue(SqsQueue::new(name).with_redrive(self.core.declared_redrive(name)))
            .await
    }

    /// Takes the declaration a registration made over a bare queue name, which on SQS is the
    /// queue's redrive policy: after `max_attempts` receives the queue moves the delivery to the
    /// dead-letter queue by itself. The mapping is the descriptor's own, so both spellings reach
    /// the same policy, and the write happens in `subscribe`, the call that has the queue's URL.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareRetryError::Broker`] carrying [`SqsError::IncompleteRedrive`] when only
    /// one half was declared: a redrive policy needs the cap and the destination together, and
    /// a service running under a cap the queue never received would lose the messages it counts.
    fn declare_retry(
        &self,
        name: &str,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        let declared = SqsQueue::new(name)
            .with_declaration(declaration)
            .redrive()
            .map_err(|incomplete| DeclareRetryError::Broker(Box::new(incomplete)))?;
        self.core.record_redrive(name, declared);
        Ok(())
    }
}

impl DefaultPublish for ConnectedSqsBroker {
    type Policy = SqsPublish;
}

#[cfg(test)]
mod tests {
    use ruststream::DescribeServer;

    use super::{Duration, SqsBroker, parse_visibility, resource_name};

    /// The host a description carries for `endpoint`.
    fn described(endpoint: &str) -> String {
        SqsBroker::new()
            .endpoint(endpoint)
            .describe_server()
            .host
            .expect("a configured endpoint describes a host")
    }

    #[test]
    fn a_description_carries_the_host_and_port_of_every_endpoint_form() {
        assert_eq!(described("http://localstack:4566"), "localstack:4566");
        assert_eq!(described("localstack:4566"), "localstack:4566");
        assert_eq!(
            described("https://sqs.eu-west-1.amazonaws.com"),
            "sqs.eu-west-1.amazonaws.com"
        );
        assert_eq!(
            described("http://localstack:4566/queues/"),
            "localstack:4566"
        );
        assert_eq!(
            described("http://user:pass@localstack:4566"),
            "localstack:4566"
        );
    }

    #[test]
    fn a_description_carries_neither_a_scheme_nor_credentials() {
        for endpoint in [
            "http://localstack:4566",
            "localstack:4566",
            "https://sqs.eu-west-1.amazonaws.com",
            "http://localstack:4566/queues/",
            "http://user:pass@localstack:4566",
        ] {
            let host = described(endpoint);
            assert!(
                !host.contains("://") && !host.contains('@'),
                "the description of {endpoint:?} published {host:?}",
            );
        }
    }

    #[test]
    fn a_path_that_carries_an_at_sign_is_not_mistaken_for_credentials() {
        // The cuts are ordered for this case: taking the credentials out first would leave the
        // tail of the path as the host.
        assert_eq!(described("https://localstack:4566/a@b"), "localstack:4566");
    }

    #[test]
    fn the_public_service_is_described_when_no_endpoint_is_configured() {
        let described = SqsBroker::new().describe_server();
        assert_eq!(described.host.as_deref(), Some("sqs.amazonaws.com"));
        assert_eq!(described.protocol, "sqs");
    }

    /// A framework destination carries dots freely and neither service takes them in a name, so
    /// the same mapping answers for a queue and for a topic.
    #[test]
    fn a_dotted_destination_maps_onto_a_name_the_services_accept() {
        assert_eq!(resource_name("order.events"), "order-events");
        assert_eq!(resource_name("orders"), "orders");
    }

    /// The one dot that survives: it is what makes a queue or a topic FIFO.
    #[test]
    fn a_fifo_suffix_survives_the_mapping() {
        assert_eq!(resource_name("order.events.fifo"), "order-events.fifo");
    }

    #[test]
    fn a_queues_visibility_timeout_is_read_in_seconds() {
        assert_eq!(parse_visibility(Some("300")), Ok(Duration::from_secs(300)));
    }

    #[test]
    fn an_absent_visibility_timeout_is_refused_rather_than_replaced() {
        let reason = parse_visibility(None).expect_err("an absent attribute has no answer");
        assert!(
            reason.contains("no VisibilityTimeout"),
            "the reason names the missing attribute, got {reason:?}",
        );
    }

    #[test]
    fn a_visibility_timeout_that_is_not_seconds_is_refused() {
        let reason =
            parse_visibility(Some("PT5M")).expect_err("a value that is not seconds has no answer");
        assert!(
            reason.contains("PT5M"),
            "the reason carries the value it could not read, got {reason:?}",
        );
    }
}
