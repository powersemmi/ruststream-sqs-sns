//! The broker ladder: [`SqsBroker`] -> [`ConnectedSqsBroker`].
//!
//! Construction is synchronous and I/O-free; credential resolution (env, profile, IMDS, SSO)
//! happens in the consuming [`Broker::connect`], and the connected form holds the live SDK
//! clients directly. One shared cell remains so publishers can be handed out while the
//! application is still being assembled, before `connect` runs.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sqs::types::QueueAttributeName;
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::{Mutex, OnceCell};

use crate::error::{SqsError, sdk_err};
use crate::publisher::{SnsPublisher, SqsPublish, SqsPublisher};
use crate::queue::SqsQueue;
use crate::subscriber::SqsSubscriber;

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the SDK clients have no shutdown and keep working
/// forever, and publishers may be handed out before `connect` and outlive `shutdown`
/// (aliasing) - so the dead-connection path must be an explicit runtime flag, or a stale
/// handle would silently succeed against a "closed" broker.
pub(crate) struct Core {
    pub(crate) sqs: aws_sdk_sqs::Client,
    pub(crate) sns: aws_sdk_sns::Client,
    pub(crate) endpoint: Option<String>,
    pub(crate) closed: AtomicBool,
    /// Queue-name -> URL cache shared by publishers and subscriptions.
    pub(crate) queue_urls: Mutex<HashMap<String, String>>,
    /// Topic-name -> ARN cache for the SNS publisher.
    pub(crate) topic_arns: Mutex<HashMap<String, String>>,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), SqsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqsError::NotConnected);
        }
        Ok(())
    }

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
            .queue_name(queue_name(queue))
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
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn topic_arn(&self, topic: &str) -> Result<String, SqsError> {
        if topic.starts_with("arn:") {
            return Ok(topic.to_owned());
        }
        let mut cache = self.topic_arns.lock().await;
        if let Some(arn) = cache.get(topic) {
            return Ok(arn.clone());
        }
        let created = self
            .sns
            .create_topic()
            .name(topic)
            .send()
            .await
            .map_err(|e| SqsError::Admin {
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
        f.debug_struct("Core")
            .field("endpoint", &self.endpoint)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

/// Maps a logical destination name onto a valid SQS queue name: characters outside
/// `[A-Za-z0-9_-]` become `-` (SQS forbids them), and a `.fifo` suffix survives. Subscribers
/// and publishers share this mapping, so dotted framework names stay routable.
pub(crate) fn queue_name(logical: &str) -> String {
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

/// An Amazon SQS broker (with SNS fan-out publishing) for the `RustStream` messaging
/// framework.
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
                let sns = aws_sdk_sns::Client::new(&config);
                Ok::<_, SqsError>(Arc::new(Core {
                    sqs,
                    sns,
                    endpoint: self
                        .endpoint
                        .clone()
                        .or_else(|| config.endpoint_url().map(str::to_owned)),
                    closed: AtomicBool::new(false),
                    queue_urls: Mutex::new(HashMap::new()),
                    topic_arns: Mutex::new(HashMap::new()),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedSqsBroker {
            core,
            cell: self.cell,
        })
    }
}

impl DescribeServer for SqsBroker {
    fn describe_server(&self) -> ServerSpec {
        let host = self
            .endpoint
            .clone()
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

    /// An SNS fan-out publisher from the connected form.
    #[must_use]
    pub fn sns_publisher(&self) -> SnsPublisher {
        SnsPublisher::new(Arc::clone(&self.cell))
    }

    /// Subscribes `queue` to `topic` (both names or ARN/URL) with raw message delivery, so
    /// payloads and headers arrive unwrapped as plain SQS messages.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the topic or queue cannot be resolved or the subscription
    /// call fails.
    pub async fn subscribe_queue_to_topic(&self, topic: &str, queue: &str) -> Result<(), SqsError> {
        self.core.ensure_open()?;
        let topic_arn = self.core.topic_arn(topic).await?;
        let queue_url = self.core.queue_url(queue).await?;
        let attributes = self
            .core
            .sqs
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| SqsError::Queue {
                name: queue.to_owned(),
                source: sdk_err(&e),
            })?;
        let queue_arn = attributes
            .attributes()
            .and_then(|map| map.get(&QueueAttributeName::QueueArn))
            .ok_or_else(|| SqsError::Queue {
                name: queue.to_owned(),
                source: Box::from("GetQueueAttributes returned no QueueArn"),
            })?
            .clone();
        self.core
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
            })
    }

    /// Opens the subscription described by `queue`.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the descriptor is invalid, the queue cannot be resolved (or
    /// created, when opted in), or the broker is shut down.
    pub async fn subscribe_queue(&self, queue: SqsQueue) -> Result<SqsSubscriber, SqsError> {
        queue.validate()?;
        self.core.ensure_open()?;

        let url = if queue.create_value() {
            self.ensure_queue(queue.queue()).await?
        } else {
            self.core.queue_url(queue.queue()).await?
        };
        Ok(SqsSubscriber::open(&self.core, url, &queue))
    }

    /// Resolves the queue, creating it when missing. A `.fifo` name creates a FIFO queue with
    /// content-based deduplication.
    async fn ensure_queue(&self, queue: &str) -> Result<String, SqsError> {
        if let Ok(url) = self.core.queue_url(queue).await {
            return Ok(url);
        }
        let mut create = self.core.sqs.create_queue().queue_name(queue_name(queue));
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
        let url = self.core.rebase_url(url.to_owned());
        self.core
            .queue_urls
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

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_queue(SqsQueue::new(name)).await
    }
}

impl DefaultPublish for ConnectedSqsBroker {
    type Policy = SqsPublish;
}
