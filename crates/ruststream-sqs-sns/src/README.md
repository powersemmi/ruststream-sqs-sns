Amazon SQS broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) client, with
publishing to SNS topics over [`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) behind the `sns`
feature.

A subscription is one queue, long-polled, and every outcome of a handler is one SQS call on the
message it received. A retry waits in the queue through the visibility timeout, and an attempt cap
is the queue's redrive policy. SNS is a publish target: a topic fans a message out to the queues
subscribed to it, and each queue is read the ordinary way.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

```
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(handle);
    })
}
```

`cargo run -- run` starts it. [`SqsBroker::new`] only records the configuration. The runtime
connects the broker at startup: the connect loads the AWS configuration and builds the SDK
clients, and the SDK fetches the credentials on the first request, the subscription's
`GetQueueUrl`. The runtime shuts the broker down when the service stops.

# Where things are

The [guides][guides] take an SQS service from installation to production, in this order:
[getting started][g-start], [consuming queues][g-consuming],
[acknowledgement and delivery guarantees][g-ack], [retries and dead-letter queues][g-retries],
[FIFO queues][g-fifo], [fan-out with SNS][g-fan-out], [testing][g-testing] and
[running in production][g-production]. The [architecture][architecture] pages show how the crate
sits between the AWS SDK and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [SqsBroker][c-broker]: [`SqsBroker`], connected into [`ConnectedSqsBroker`].
- [Credentials][c-credentials]: the AWS SDK's default chain, or a prebuilt configuration through
  [`SqsBroker::from_config`].
- [Endpoint][c-endpoint]: [`SqsBroker::endpoint`], [`SqsBroker::region`] and
  [`SqsBroker::test_credentials`] point the broker at a local stack.
- [Startup][c-startup]: each subscription resolves its queue, writes its redrive policy and reads
  the queue's visibility timeout.
- [Shutdown][c-shutdown]: [`SqsBroker::publisher`], and [`SqsError::NotConnected`] from a
  publisher used after shutdown.
- [Prelude][c-prelude]: [`prelude`], the one glob an SQS service imports.
- [Errors][c-errors]: [`SqsError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare queue name in `#[subscriber]`.
- [SqsQueue][c-sqs-queue]: [`SqsQueue`], one queue by name or URL.
- [wait][c-wait]: [`SqsQueue::wait`], the long poll of each receive.
- [visibility][c-visibility]: [`SqsQueue::visibility`], how long a received message stays
  hidden.
- [create_if_missing][c-create-if-missing]: [`SqsQueue::create_if_missing`], a queue the
  service creates at startup.
- [SqsSubscription][c-sqs-subscription]: [`SqsSubscription`], the queue steps on the mount
  chain.
- [Batches][c-batches]: a slice handler over the batches of [`SqsSubscriber`].
- [FIFO][c-fifo]: a `.fifo` queue and its message groups.

## Settlement

- [Settlement][c-settlement]: what `ack`, `drop` and `retry` do to a message.
- [retry_after][c-retry-after]: a retry that waits in the queue through the visibility timeout.
- [Dead letters][c-dead-letters]: `max_attempts` and `dead_letter`, the queue's redrive policy.

## Delivery

- [Delivery][c-delivery]: [`SqsMessage`], with each header as one SQS message attribute.
- [RECEIVE_COUNT_HEADER][c-receive-count]: [`RECEIVE_COUNT_HEADER`], the queue's receive count.
- [PARTITION_KEY_HEADER][c-partition-key]: [`PARTITION_KEY_HEADER`], the FIFO message group.
- [Body][c-body]: a payload SQS does not take as text travels base64-encoded.

## Publishing

- [SqsPublish][c-publishing]: [`SqsPublish`], the queue publish policy, and its live
  [`SqsPublisher`].
- [SqsPublishOptions][c-options]: [`SqsPublishOptions`] and the steps of [`SqsPublishSteps`].
- [group_id][c-group-id]: [`SqsPublish::group_id`], the message group of a position.
- [deduplication_id][c-deduplication-id]: [`SqsPublishSteps::deduplication_id`], the
  idempotency key of one message.

## SNS

- [SnsPublish][c-sns]: `SnsPublish`, the topic publish policy, and its live `SnsPublisher`
  (feature `sns`).
- [subscribe_queue_to_topic][c-subscribe-queue]: `ConnectedSqsBroker::subscribe_queue_to_topic`,
  a queue subscribed to a topic with raw message delivery.
- [fans_out_to][c-fans-out-to]: `SnsPublish::fans_out_to`, the queues an operator subscribed,
  named for a test (features `sns` and `testing`).

## AsyncAPI and testing

- [SQS bindings][c-asyncapi]: the `sqs` and `sns` bindings of the generated document (feature
  `asyncapi`): the [server][c-server], the [channels][c-channels] and the [topics][c-topics].
- [Testing][c-testing]: the in-process account
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running stack.

# Cargo features

The default build reads and writes queues only. `sns` adds publishing to SNS topics and links the
SNS client. `asyncapi` contributes the `sqs` bindings to the generated document, and the `sns`
ones with `sns` on. `testing` adds the in-process mode `TestApp::start` connects the broker
through.

[guides]: https://powersemmi.github.io/ruststream-sqs-sns/latest/
[g-start]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/getting-started/
[g-consuming]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/consuming/
[g-ack]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/acknowledgement/
[g-retries]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/retries/
[g-fifo]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/fifo/
[g-fan-out]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/fan-out/
[g-testing]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-sqs-sns/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-sqs-sns/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-sqs-sns/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/
[c-credentials]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/credentials/
[c-endpoint]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/endpoint/
[c-startup]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/
[c-sqs-queue]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/sqs-queue/
[c-wait]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/wait/
[c-visibility]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/visibility/
[c-create-if-missing]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/create-if-missing/
[c-sqs-subscription]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/sqs-subscription/
[c-batches]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/batches/
[c-fifo]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/subscriptions/fifo/
[c-settlement]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/settlement/
[c-retry-after]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/settlement/retry-after/
[c-dead-letters]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/settlement/dead-letters/
[c-delivery]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/delivery/
[c-receive-count]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/delivery/receive-count/
[c-partition-key]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/delivery/partition-key/
[c-body]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/delivery/body/
[c-publishing]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/publishing/
[c-options]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/publishing/options/
[c-group-id]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/publishing/group-id/
[c-deduplication-id]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/publishing/deduplication-id/
[c-sns]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/sns/
[c-subscribe-queue]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/sns/subscribe-queue-to-topic/
[c-fans-out-to]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/sns/fans-out-to/
[c-asyncapi]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/asyncapi/
[c-server]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/asyncapi/server/
[c-channels]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/asyncapi/channels/
[c-topics]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/asyncapi/topics/
[c-testing]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-sqs-sns/latest/components/testing/live/
