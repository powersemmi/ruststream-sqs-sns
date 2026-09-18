//! The gate the live suites share, and the handful of helpers every one of them repeats.
//!
//! A live test skips when `SQS_TEST_ENDPOINT` is unset, which is what keeps the suites usable
//! during development: `cargo test` on a laptop with no stack passes. The same skip in CI is a
//! lie, because the job stood a stack up first, and a suite that returns before its first
//! assertion reports `ok` exactly like one that ran. The live job therefore sets
//! `RUSTSTREAM_REQUIRE_LIVE`, and under that flag every skip becomes a failure naming what it
//! wanted.

// Each live suite is its own test binary and uses the part of this module its topic needs, so
// what one of them leaves alone is not dead code.
#![allow(dead_code)]

use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::types::QueueAttributeName;
use ruststream::Broker;
use ruststream_sqs_sns::{ConnectedSqsBroker, SqsBroker};

/// The variable a job sets to say it stood a stack up, so skipping past it is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// Whether this run is required to reach a live stack.
fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The endpoint from `name`, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `name` is not: a job that started a stack and then
/// lost its address is a broken job, and the tests behind it would have passed without running.
pub(crate) fn endpoint(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(endpoint) if !endpoint.is_empty() => Some(endpoint),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {name} is unset or empty",
            );
            eprintln!("{name} is not set; skipping the live suite");
            None
        }
    }
}

/// How long a live test waits for a delivery it expects to arrive.
pub(crate) const RECV_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a live test waits to conclude that nothing is coming.
///
/// Long enough for a queue that would redeliver to have done so, short enough that a suite of
/// these still runs in a minute.
pub(crate) const QUIET: Duration = Duration::from_secs(5);

/// A broker connected to the local stack, with the credentials a stack accepts and ignores.
pub(crate) async fn connect(endpoint: &str) -> ConnectedSqsBroker {
    broker(endpoint).connect().await.expect("broker connects")
}

/// The broker these suites build, before it connects.
pub(crate) fn broker(endpoint: &str) -> SqsBroker {
    SqsBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

/// Per-test unique queue, so runs do not observe each other's leftovers.
pub(crate) fn unique(name: &str) -> String {
    format!("it-{name}-{}", std::process::id())
}

/// An SDK client of the test's own, for reading back what the service holds. Deliberately not
/// the broker's: what proves a setting arrived is the service's answer, not this crate's view
/// of it.
pub(crate) async fn admin(endpoint: &str) -> aws_sdk_sqs::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    aws_sdk_sqs::Client::new(&config)
}

/// Reads one attribute of a queue as the service reports it.
pub(crate) async fn queue_attribute(
    endpoint: &str,
    queue: &str,
    attribute: QueueAttributeName,
) -> String {
    let client = admin(endpoint).await;
    let url = client
        .get_queue_url()
        .queue_name(queue)
        .send()
        .await
        .expect("the queue is there")
        .queue_url()
        .expect("GetQueueUrl returns the URL")
        .to_owned();
    client
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(attribute.clone())
        .send()
        .await
        .expect("the attributes are readable")
        .attributes()
        .and_then(|map| map.get(&attribute))
        .cloned()
        .unwrap_or_default()
}
