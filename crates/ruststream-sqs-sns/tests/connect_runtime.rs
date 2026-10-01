//! The broker's own tasks run on the runtime it connected on, whichever thread starts them.
//!
//! A handler on a dedicated thread settles from that thread's current-thread runtime, and a
//! subscription can be opened from any runtime. Each case here does its part from such a runtime,
//! stops it, and expects the work to complete on the broker's runtime. The conformance
//! `lifecycle` covers a publish and a delayed nack that the queue answers at once; these cover
//! the delay the in-process account runs itself and the live subscription's pump. The last cases
//! cover the connections a request opens: one opened from a dedicated thread belongs to that
//! thread, so the broker's own requests never wait on a thread that is busy computing.

#![cfg(feature = "testing")]

use std::error::Error;
use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_sqs_sns::{SqsBroker, SqsError, SqsQueue};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::time::timeout;

mod live;

use live::{RECV_TIMEOUT, admin, connect, unique};

/// How long a publish from the broker's own runtime may take while a dedicated thread computes:
/// a round trip to the local stack, with room to spare, and far below the dedicated thread's
/// computation.
const PROMPT: Duration = Duration::from_millis(500);

/// The longest the dedicated thread computes, so it ends even when the test fails before it
/// tells the thread to stop.
const BUSY: Duration = Duration::from_secs(10);

/// A current-thread runtime of its own, the kind a dedicated handler thread runs.
fn foreign_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
}

/// A delivery settled with a delay from a runtime that stops at once comes back when the delay
/// runs out: the account's timer runs on the runtime the broker connected on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_nack_from_another_runtime_comes_back_in_process() -> Result<(), Box<dyn Error>> {
    let connected = SqsBroker::new()
        .region("us-east-1")
        .connect_in_process()
        .await?;
    let mut subscriber = connected.subscribe_queue(SqsQueue::new("claims")).await?;
    connected
        .publisher()
        .publish(OutgoingMessage::new("claims", b"claim".as_slice()), None)
        .await?;
    let mut stream = pin!(subscriber.stream());
    let first = stream.next().await.ok_or("the stream ended")??;

    let delay = Duration::from_secs(1);
    let (settled, done) = oneshot::channel();
    thread::spawn(move || {
        let runtime = foreign_runtime();
        let outcome = runtime.block_on(first.nack_after(delay));
        drop(runtime);
        let _ = settled.send(outcome);
    });
    done.await??;

    let again = timeout(delay + Duration::from_secs(10), stream.next())
        .await
        .map_err(|_| "the delayed nack never came back")?
        .ok_or("the stream ended")??;
    assert_eq!(again.payload(), b"claim");
    again.ack().await?;
    Ok(())
}

/// A subscription whose stream was opened on a runtime that stopped keeps receiving: its pump,
/// and every delivery's visibility extender with it, runs on the runtime the broker connected on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_opened_on_another_runtime_outlives_it() -> Result<(), Box<dyn Error>> {
    let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
        return Ok(());
    };
    let queue = unique("connect-runtime-pump");
    let connected = connect(&endpoint).await;
    let mut subscriber = connected
        .subscribe_queue(SqsQueue::new(queue.clone()).create_if_missing())
        .await?;

    // The stream is opened, which starts the pump, inside a runtime that is gone before anything
    // is published.
    let stream = thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = foreign_runtime();
                let stream = runtime.block_on(async { subscriber.stream() });
                drop(runtime);
                stream
            })
            .join()
            .expect("the foreign thread finished")
    });
    let mut stream = pin!(stream);

    connected
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"order".as_slice()), None)
        .await?;
    let delivered = timeout(RECV_TIMEOUT, stream.next())
        .await
        .map_err(|_| "nothing arrived")?
        .ok_or("the stream ended with the runtime it was opened on")??;
    assert_eq!(delivered.payload(), b"order");
    delivered.ack().await?;
    Ok(())
}

/// Publishes once through `publish` from a current-thread runtime on a thread of its own, then
/// keeps that thread computing, with the runtime alive and unpolled, until `stop` fires or
/// [`BUSY`] runs out: the shape of a handler on a dedicated thread that publishes and computes.
fn publish_then_compute(
    publish: impl Future<Output = Result<(), SqsError>> + Send + 'static,
    published: oneshot::Sender<Result<(), SqsError>>,
    stop: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let runtime = foreign_runtime();
        let _ = published.send(runtime.block_on(publish));
        let started = Instant::now();
        let mut state = 0_u64;
        while started.elapsed() < BUSY && stop.try_recv().is_err() {
            for step in 0..10_000 {
                state = black_box(state.wrapping_mul(31).wrapping_add(step));
            }
        }
        drop(runtime);
    })
}

/// Runs the scenario: the broker's first request goes out from a dedicated thread through
/// `from_thread`, and while that thread computes, `from_broker` publishes from the broker's
/// runtime within [`PROMPT`].
async fn a_publish_from_the_broker_runtime_is_prompt(
    from_thread: impl Future<Output = Result<(), SqsError>> + Send + 'static,
    from_broker: impl Future<Output = Result<(), SqsError>>,
) -> Result<(), Box<dyn Error>> {
    let (published, first) = oneshot::channel();
    let (stop, stopped) = mpsc::channel();
    let worker = publish_then_compute(from_thread, published, stopped);
    first.await??;

    let started = Instant::now();
    let prompt = timeout(PROMPT, from_broker).await;
    let took = started.elapsed();
    let _ = stop.send(());
    spawn_blocking(move || worker.join())
        .await?
        .map_err(|_| "the dedicated thread panicked")?;
    prompt.map_err(|_| {
        format!(
            "a publish from the broker's runtime did not finish within {PROMPT:?} \
             while a dedicated thread computed"
        )
    })??;
    eprintln!("the publish from the broker's runtime took {took:?}");
    Ok(())
}

/// A queue publish from the broker's runtime does not wait on a connection a dedicated thread
/// opened and then stopped driving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_publish_does_not_wait_on_a_busy_dedicated_thread() -> Result<(), Box<dyn Error>> {
    let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
        return Ok(());
    };
    let queue = unique("connect-runtime-busy-queue");
    // The queue is made through a client of the test's own, so the broker's first request is
    // the one the dedicated thread sends.
    admin(&endpoint)
        .await
        .create_queue()
        .queue_name(&queue)
        .send()
        .await?;
    let connected = connect(&endpoint).await;

    let publisher = connected.publisher();
    let destination = queue.clone();
    let from_thread = async move {
        publisher
            .publish(
                OutgoingMessage::new(&destination, b"from a thread".as_slice()),
                None,
            )
            .await
    };
    let publisher = connected.publisher();
    let from_broker = async move {
        publisher
            .publish(
                OutgoingMessage::new(&queue, b"from the broker".as_slice()),
                None,
            )
            .await
    };
    Box::pin(a_publish_from_the_broker_runtime_is_prompt(
        from_thread,
        from_broker,
    ))
    .await
}

/// A topic publish from the broker's runtime does not wait on a connection a dedicated thread
/// opened and then stopped driving.
#[cfg(feature = "sns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_publish_does_not_wait_on_a_busy_dedicated_thread() -> Result<(), Box<dyn Error>> {
    let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
        return Ok(());
    };
    let topic = unique("connect-runtime-busy-topic");
    let connected = connect(&endpoint).await;

    // The topic is resolved, which creates it, from the dedicated thread: that is the broker's
    // first request to SNS.
    let publisher = connected.sns_publisher();
    let destination = topic.clone();
    let from_thread = async move {
        publisher
            .publish(
                OutgoingMessage::new(&destination, b"from a thread".as_slice()),
                None,
            )
            .await
    };
    let publisher = connected.sns_publisher();
    let from_broker = async move {
        publisher
            .publish(
                OutgoingMessage::new(&topic, b"from the broker".as_slice()),
                None,
            )
            .await
    };
    Box::pin(a_publish_from_the_broker_runtime_is_prompt(
        from_thread,
        from_broker,
    ))
    .await
}
