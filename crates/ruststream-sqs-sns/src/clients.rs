//! The SDK clients a request goes out through: one set per runtime that sends requests.
//!
//! The SDK's HTTP client opens a connection on the runtime of the request that needs it, and keeps
//! it in a pool every clone of the client shares. Such a connection makes progress only while its
//! runtime is polled, so a connection a dedicated thread opened would stall a later request from
//! the broker's own runtime for as long as that thread computes. Each runtime therefore sends
//! through clients of its own. The broker's runtime, every worker thread of it included, uses the
//! set `connect` built. Any other runtime uses a set built for its thread on the first request,
//! from the same `SdkConfig`: the credentials provider and its cache are shared, and only the HTTP
//! client and its pool are the thread's own.
//!
//! A thread decides once, on its first request through a broker, whether it runs that broker's
//! runtime, and keeps the answer: a thread that later runs a different runtime keeps sending
//! through the set it chose first.

use std::cell::RefCell;
use std::ptr;
use std::sync::{Arc, Weak};

use aws_config::SdkConfig;
#[cfg(feature = "sns")]
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sqs::Client as SqsClient;
use tokio::runtime::{Handle, Id};

/// The SDK clients of one broker, and the recipe for a thread's own set.
pub(crate) struct Clients {
    /// The runtime `connect` ran on: a request from it sends through [`Self::home`].
    runtime: Id,
    /// What a thread's own set is built from, so it shares the credentials and their cache.
    config: SdkConfig,
    home: ClientSet,
}

/// One SDK client per service this build talks to.
pub(crate) struct ClientSet {
    pub(crate) sqs: SqsClient,
    #[cfg(feature = "sns")]
    pub(crate) sns: SnsClient,
}

impl ClientSet {
    /// Clients with an HTTP client and a connection pool of their own: the config leaves the HTTP
    /// client to the SDK, which builds a fresh one for every client.
    fn new(config: &SdkConfig) -> Self {
        Self {
            sqs: SqsClient::new(config),
            #[cfg(feature = "sns")]
            sns: SnsClient::new(config),
        }
    }
}

/// What one thread sends through for one broker.
enum Placement {
    /// The thread runs the broker's runtime.
    Home,
    /// The thread runs another runtime, and this set is its own.
    Own(ClientSet),
}

/// One broker's placement on the current thread.
struct ThreadEntry {
    /// The broker the entry belongs to. It is the key as well: the entry keeps the allocation
    /// alive, so no other broker can take its address while the entry exists.
    owner: Weak<Clients>,
    placement: Placement,
}

thread_local! {
    /// The placements of every broker this thread sent a request through. One entry per broker,
    /// so a service with one broker scans a single entry.
    static THREAD_CLIENTS: RefCell<Vec<ThreadEntry>> = const { RefCell::new(Vec::new()) };
}

impl Clients {
    /// The clients `connect` builds on `runtime`, from `config`.
    pub(crate) fn new(config: SdkConfig, runtime: &Handle) -> Arc<Self> {
        Arc::new(Self {
            runtime: runtime.id(),
            home: ClientSet::new(&config),
            config,
        })
    }

    /// The set of the broker's own runtime, for the tasks the broker spawns there.
    pub(crate) const fn home(&self) -> &ClientSet {
        &self.home
    }

    /// Starts an SQS request through the client of the runtime this call runs on.
    ///
    /// `request` receives the client and returns the request builder, which owns what it needs
    /// from the client, so the choice costs one thread-local lookup and nothing per request
    /// beyond it.
    pub(crate) fn sqs<Request>(
        self: &Arc<Self>,
        request: impl Fn(&SqsClient) -> Request,
    ) -> Request {
        self.with(|set| request(&set.sqs))
    }

    /// Starts an SNS request through the client of the runtime this call runs on; see
    /// [`Self::sqs`].
    #[cfg(feature = "sns")]
    pub(crate) fn sns<Request>(
        self: &Arc<Self>,
        request: impl Fn(&SnsClient) -> Request,
    ) -> Request {
        self.with(|set| request(&set.sns))
    }

    fn with<Request>(self: &Arc<Self>, request: impl Fn(&ClientSet) -> Request) -> Request {
        // The thread-local is gone only while the thread itself is being torn down; a request
        // sent from a destructor then goes out through the broker's own clients.
        THREAD_CLIENTS
            .try_with(|entries| self.with_placed(entries, &request))
            .unwrap_or_else(|_| request(&self.home))
    }

    fn with_placed<Request>(
        self: &Arc<Self>,
        entries: &RefCell<Vec<ThreadEntry>>,
        request: &impl Fn(&ClientSet) -> Request,
    ) -> Request {
        let key = Arc::as_ptr(self);
        {
            let entries = entries.borrow();
            if let Some(entry) = entries
                .iter()
                .find(|entry| ptr::eq(entry.owner.as_ptr(), key))
            {
                return request(self.set_for(&entry.placement));
            }
        }
        // A thread outside any runtime has nothing to decide by; the SDK needs a runtime to send,
        // so the request fails there as it would through any client, and nothing is recorded.
        let Ok(current) = Handle::try_current() else {
            return request(&self.home);
        };
        let placement = if current.id() == self.runtime {
            Placement::Home
        } else {
            Placement::Own(ClientSet::new(&self.config))
        };
        let mut entries = entries.borrow_mut();
        // A broker that is gone leaves its entry behind; the thread drops such entries when it
        // meets a new broker, so their connection pools do not outlive the service's brokers by
        // more than one broker.
        entries.retain(|entry| entry.owner.strong_count() > 0);
        entries.push(ThreadEntry {
            owner: Arc::downgrade(self),
            placement,
        });
        let placement = &entries[entries.len() - 1].placement;
        request(self.set_for(placement))
    }

    const fn set_for<'set>(&'set self, placement: &'set Placement) -> &'set ClientSet {
        match placement {
            Placement::Home => &self.home,
            Placement::Own(set) => set,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::sync::Arc;
    use std::thread;

    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use tokio::runtime::{Builder, Handle};
    use tokio::task::JoinError;

    use super::{ClientSet, Clients};

    fn clients() -> Arc<Clients> {
        let config = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .build();
        Clients::new(config, &Handle::current())
    }

    /// The address of the set a request from here goes through.
    fn chosen(clients: &Arc<Clients>) -> usize {
        clients.with(|set| ptr::from_ref::<ClientSet>(set).addr())
    }

    fn home(clients: &Clients) -> usize {
        ptr::from_ref(clients.home()).addr()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_thread_of_the_broker_runtime_sends_through_the_connect_clients()
    -> Result<(), JoinError> {
        let clients = clients();
        assert_eq!(chosen(&clients), home(&clients));
        // A task lands on a worker thread of the same runtime, which decides for itself.
        let spawned = Arc::clone(&clients);
        let on_worker = tokio::spawn(async move { chosen(&spawned) }).await?;
        assert_eq!(on_worker, home(&clients));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_thread_of_another_runtime_sends_through_a_set_of_its_own() {
        let clients = clients();
        let (first, second) = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a current-thread runtime builds");
                    runtime.block_on(async { (chosen(&clients), chosen(&clients)) })
                })
                .join()
                .expect("the other thread finished")
        });
        assert_ne!(
            first,
            home(&clients),
            "the other runtime got the connect clients"
        );
        assert_eq!(first, second, "the thread built its set more than once");
        // The broker's own runtime keeps its clients after the other thread built a set.
        assert_eq!(chosen(&clients), home(&clients));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_brokers_on_one_thread_do_not_share_a_set() {
        let (one, other) = (clients(), clients());
        let (for_one, for_other) = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a current-thread runtime builds");
                    runtime.block_on(async { (chosen(&one), chosen(&other)) })
                })
                .join()
                .expect("the other thread finished")
        });
        assert_ne!(for_one, home(&one));
        assert_ne!(for_other, home(&other));
        assert_ne!(for_one, for_other, "two brokers took one slot");
    }
}
