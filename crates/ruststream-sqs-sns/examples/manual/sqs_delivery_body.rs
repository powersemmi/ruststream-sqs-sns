//! The service of `examples/sqs_delivery_body.rs` without the macros: the byte view and the
//! handler are written by hand, and so is `main`.
//!
//! ```text
//! cargo test --example manual_sqs_delivery_body --features testing
//! just brokers-up
//! SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --example manual_sqs_delivery_body --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::Unnamed;
use ruststream::runtime::cli::run_main;
use ruststream_sqs_sns::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:handler]
/// A thumbnail as the producer sent it: raw image bytes, read without a codec.
struct Thumbnail<'a>(&'a [u8]);

impl Deserialized for Thumbnail<'_> {
    type Output<'a> = Thumbnail<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Thumbnail<'_>, Self::Error> {
        Ok(Thumbnail(payload))
    }
}

// Routes the type onto the lane of payloads that read themselves.
impl Input for Thumbnail<'_> {
    type Axis = SoloDeserialized<Thumbnail<'static>>;
}

struct Store;

// For every lifetime of the payload it borrows.
impl<'p> Handle<Thumbnail<'p>> for Store {
    fn handle(
        &self,
        thumbnail: &Thumbnail<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(bytes = thumbnail.0.len(), "thumbnail stored");
        ready(Ok(()))
    }
}
// --8<-- [end:handler]

/// The service reading `queue`, which it creates on a fresh stack.
fn service(broker: SqsBroker, queue: String) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(Unnamed::<SqsQueue>::new(), Store)
                .build()
                .name(queue)
                .create_if_missing(),
        );
    })
}

/// The broker for a local stack; production builds `SqsBroker::new()` and lets the SDK resolve
/// the region and credentials.
fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials()
}

fn app() -> impl App {
    service(broker("http://localhost:4566"), "thumbnails".to_owned())
}

// What `#[app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqs_sns::prelude::*;

    use super::{broker, live, service};

    /// What a producer sends: bytes it already holds, published as they are.
    #[derive(Outgoing, Serialized)]
    struct Image(Vec<u8>);

    /// The start of a PNG file: a byte above 0x7f and control characters, which SQS does not
    /// take as text.
    const PNG: [u8; 8] = [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];

    /// The body both modes run: the handler gets the bytes the producer sent.
    async fn the_bytes_arrive_unchanged(
        tb: TestApp<()>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<SqsBroker>()
            .message(&Image(PNG.to_vec()))
            .to(queue.to_owned())
            .publish()
            .await?;

        tb.broker::<SqsBroker>()
            .subscriber(queue)
            .assert_called_once()
            .with_raw(&PNG)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let app = service(broker("http://localhost:4566"), "thumbnails".to_owned());
        the_bytes_arrive_unchanged(TestApp::start(app).await?, "thumbnails").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stack() -> Result<(), Box<dyn Error>> {
        let Some(endpoint) = live::endpoint("SQS_TEST_ENDPOINT") else {
            return Ok(());
        };
        let queue = live::unique("body-thumbnails");
        let app = service(broker(&endpoint), queue.clone());
        the_bytes_arrive_unchanged(TestApp::start_live(app).await?, &queue).await
    }

    /// The harness refuses to inject what SQS refuses to send.
    #[tokio::test]
    #[should_panic(expected = "must not be empty")]
    async fn an_empty_body_is_refused() {
        let app = service(broker("http://localhost:4566"), "thumbnails".to_owned());
        let tb = TestApp::start(app).await.expect("the service starts");

        let _ = tb
            .broker::<SqsBroker>()
            .message(&Image(Vec::new()))
            .to("thumbnails")
            .publish()
            .await;
    }
}
