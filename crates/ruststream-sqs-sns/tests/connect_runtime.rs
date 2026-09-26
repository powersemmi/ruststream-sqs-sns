//! The broker's own tasks run on the runtime it connected on, whichever thread starts them.
//!
//! A handler on a dedicated thread settles from that thread's current-thread runtime, and a
//! subscription can be opened from any runtime. Each case here does its part from such a runtime,
//! stops it, and expects the work to complete on the broker's runtime. The conformance
//! `lifecycle` covers a publish and a delayed nack that the queue answers at once; these cover
//! the delay the in-process account runs itself and the live subscription's pump.

#![cfg(feature = "testing")]

use std::error::Error;
use std::pin::pin;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_sqs_sns::{SqsBroker, SqsQueue};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::time::timeout;

mod live;

use live::{RECV_TIMEOUT, connect, unique};

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
