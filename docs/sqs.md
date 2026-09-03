# Amazon SQS

`ruststream-sqs-sns` is the Amazon SQS broker, with SNS fan-out publishing. It covers long
polling, visibility-based retries, FIFO message groups, and ships an in-process test broker under
its `testing` feature. For framework concepts (writing subscribers, routing, codecs, middleware),
see the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

A service file imports `ruststream_sqs_sns::prelude::*` and nothing else from either crate: the
glob carries the framework's own prelude along with this crate's broker, queue descriptor and
publish types. Code working below the handler surface - a raw codec, an `OutgoingMessage`, the
`testing` broker - imports what it needs by name.

Everything the glob carries is the framework's own item rather than an alias of it, so a service
that runs on two brokers can glob both preludes and what they share resolves to a single item.

The crate's MSRV is 1.94, tracking the AWS SDK; the framework core stays at 1.85, and a dependent
may exceed its dependency's floor.

## Capabilities

Which of the framework's optional capability traits this broker implements natively. A capability
that is not implemented does not compile at the mount site, rather than failing at runtime.

| Capability | Native | Why |
| --- | --- | --- |
| `Subscribe` | yes | the connected broker resolves a queue by name, so `#[subscriber("orders")]` binds without a descriptor |
| `BatchSubscriber` | no | `ReceiveMessage` returns up to ten messages per call, but each delivery is streamed and settled on its own; a page handler mounts over the framework's own buffer instead (`.buffered(nonzero!(n), window)`), which closes a page by size or by a deadline |
| `TransactionalPublisher` | no | SQS has no transactional send |
| `OwnedTransactions` | no | SQS has no transactional send |
| `RequestReply` | no | SQS has no reply inbox; a reply is an ordinary send to another queue |
| `Partitioned` | yes | the `partition-key` header is the FIFO message group id, in both directions (see [FIFO message groups](#fifo-message-groups)) |
| `Seekable` / `Positioned` | no | a queue is not a replayable log: a delivery is either deleted or returned to the queue, and there is no cursor to move; messages that outlive their attempts are recovered from the redrive policy's dead-letter queue, not by repositioning |
| `DescribeServer` | yes | `SqsBroker` reports its endpoint and the `sqs` protocol, which the framework's AsyncAPI generation consumes |

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
SqsBroker::new()          configuration only, synchronous, no I/O
  .connect()   ->  ConnectedSqsBroker    the live SDK clients; subscriptions and publishers
  .shutdown()  ->  ()                    the terminal transition
```

`new` performs no I/O, so an SQS service is assembled with the same `#[ruststream::app]` macro as
any other broker: region and credentials resolve from the environment (profile, IMDS, SSO) inside
`connect`, which the runtime calls once at startup before opening subscriptions. Because
`shutdown` consumes the connected broker, publishing or subscribing after it does not compile. The
AWS SDK clients have no close or flush, so shutdown carries no diagnostics and its witness type is
`()`; what it does is mark the shared client state closed, and a publisher handed out earlier then
reports `SqsError::NotConnected` rather than succeeding against a broker the application considers
gone.

Configuration sits on the synchronous builder:

- `SqsBroker::new()` resolves everything from the environment.
- `SqsBroker::from_config(config)` takes a prebuilt `aws_config::SdkConfig`, which is also how a
  service pins a fixed SDK behaviour version instead of the latest.
- `endpoint(url)`, `region(name)`, and `test_credentials()` target a local stack. Queue URLs
  returned by the service are rebased onto the configured endpoint, so the adapter is unaffected
  by the host-rewriting strategies local stacks use.

`connect` also sets a per-attempt timeout of 25 seconds on the SDK config it builds, because a
long poll waits up to 20 seconds and a shorter attempt timeout would kill every receive.

## Queue descriptors

`SqsQueue` is the subscription descriptor. The parameters that decide cost and latency are
explicit on it:

| Method | Meaning | Default |
| --- | --- | --- |
| `wait(Duration)` | long-polling wait per receive call, capped at the protocol's 20 seconds | 20 seconds |
| `batch(i32)` | messages per receive call, within the protocol range `1..=10` | 10 |
| `visibility(Duration)` | visibility timeout requested per receive, within `1s..=12h` | the queue's configured timeout |
| `create_if_missing()` | create the queue on subscribe when it does not exist | off |

A descriptor is validated before any I/O: an empty name, a wait above 20 seconds, a batch outside
`1..=10`, or a visibility outside `1s..=12h` fails with `SqsError::InvalidQueue` at subscribe time,
without a call to AWS.

`SqsQueue` implements `SubscriptionSource`, so it sits inline in the `#[subscriber(..)]`
decorator, and the queue is named either by URL or by name (resolved through `GetQueueUrl` and
cached):

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

The plain string form `#[subscriber("orders")]` also works and takes the descriptor defaults.
Logical destination names map onto valid SQS queue names by replacing every character outside
`[A-Za-z0-9_-]` with `-`; a `.fifo` suffix survives the mapping. Subscribers and publishers share
that mapping, so a dotted framework name stays routable.

`create_if_missing` is meant for local development and tests, where the queue is not provisioned
ahead of the process; a name ending in `.fifo` creates a FIFO queue with content-based
deduplication. Production queues are usually managed as infrastructure.

A subscription is a stream fed by a background pump that long-polls `ReceiveMessage`. Its channel
holds one batch, so the pump never polls ahead of what the handler drains. Dropping the subscriber
stops the pump; messages it had already delivered and that were never settled redeliver once their
visibility lapses. Receive failures surface as items on the stream: a queue that does not exist
ends the stream, and any other failure backs off for a second so a persistent error cannot spin
the loop.

## Settlement and deferred retry

Every settlement verb is a native SQS operation:

| Handler outcome | SQS operation |
| --- | --- |
| `HandlerOutcome::ack()` | `DeleteMessage` |
| `HandlerOutcome::retry()` | `ChangeMessageVisibility` to 0, so the message redelivers immediately |
| `HandlerOutcome::retry_after(delay)` | `ChangeMessageVisibility` to the delay |
| `HandlerOutcome::drop()` | `DeleteMessage` |

Delayed redelivery is native as well: a negative acknowledgement carrying a delay
(`IncomingMessage::nack_after`) sets the message's visibility to that delay, capped at the
protocol's 12 hours. The service holds the message for the delay and redelivers it on the same
queue with its receive count intact, since nothing is re-published and no copy is made.

Dropping is deletion because SQS has no discard operation short of deleting. Poison-message
routing belongs to the queue's redrive policy: after `maxReceiveCount` deliveries the service
moves the message to the dead-letter queue on its own, driven by the repeated requeues above. The
count that policy uses is visible to handlers as the `sqs-receive-count` header
(`RECEIVE_COUNT_HEADER`), carrying the service's approximate receive count, so a handler can treat
the last attempt differently from the first.

## The visibility extender

SQS has no lease renewal API, and a handler that outlives the message's visibility timeout would
otherwise see the same message delivered again while it is still working on it. For as long as a
delivered message handle is alive, the crate keeps a background task re-arming that message's
visibility, every half of the visibility period, back to the full value. The task is aborted the
moment the message is settled or dropped, so it never holds a message invisible after the handler
is done, and a failed extension is logged at debug level and retried on the next tick. Handler
duration is thus bounded by the process, not by the queue's timeout.

An unsettled drop stops the extension without any further call, and the message redelivers when
its current visibility lapses, which is the at-least-once contract.

## FIFO message groups

On a `.fifo` destination the `partition-key` header is the message group id. Publishing sends it
as the group id (`"default"` when the header is absent, since FIFO queues require one), and a
received message carries its group id back in the same header, so a service reads and writes one
header regardless of which side of the queue it is on. `SqsMessage` also implements the
framework's `Partitioned` capability over that header. The convention matches the in-memory
broker's, so switching brokers does not change a service's headers. A publisher can also carry
that header for the messages that do not name it, with
[`with_group_id`](#per-message-arguments).

Every FIFO send also carries a process-unique deduplication id. An explicit id wins over
content-based deduplication, so two legitimate identical payloads are never collapsed into one by
the deduplication window.

## Publishing

A publisher is a policy plus the live connection. The policy holds no connection, so it is
constructed anywhere - in a router, in configuration, at a mount site - and the runtime pairs it
with the broker at startup. Naming a policy picks the destination kind:

- `SqsPublish` pairs into `SqsPublisher`: sends directly to a queue, named by URL or by name. It
  is also the broker's default publish policy, so a `#[subscriber(.., publish("dest"))]` handler
  mounted without an explicit publisher sends through it.
- `SnsPublish` pairs into `SnsPublisher`: publishes a notification to an SNS topic, named by ARN
  or by name (a name resolves through the idempotent `CreateTopic`).

The prelude also exports `SqsPublish` as `Publish`, the name every broker crate gives the policy a
mount site hands to `include` and the lifecycle hooks; the examples use it. `SnsPublish` keeps its
own name, because fan-out is the departure rather than the default. Both stay available under their
prefixed names for a file that mixes them.

That name is free because the two vocabularies live in different files. A handler file globs the
framework's prelude alone and bounds an injected publisher with the framework's `Publisher`; a
routes file globs this crate's prelude, names the broker it mounts on, and writes `Publish` for the
policy. Nothing needs both globs at once.

A publisher can also be taken directly from the broker before the application starts, with
`SqsBroker::publisher()`, or from the connected form with `ConnectedSqsBroker::publisher()` and
`ConnectedSqsBroker::sns_publisher()`. Either way it aliases the connection, and every publish
after `shutdown` reports `SqsError::NotConnected`.

### Per-message arguments

A publish builder fills its headers position once, so a message type declaring a header contract
spends that position on the contract value. `with_group_id`, on `SqsPublisher` and `SnsPublisher`,
carries the FIFO message group beside it as a **base header**: a map the handle holds, which the
builder writes the call site's own headers over, key by key.

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_fifo_group.rs:publish"
```

The group is the `partition-key` header, so a message that names `partition-key` itself wins and a
handle with no group adds nothing. The send path pulls `partition-key` out into the native
`MessageGroupId` rather than sending it as a message attribute.

## SNS fan-out

SNS is a publisher only: its delivery targets are queues and HTTP endpoints, not a consumer this
crate would own. One publish to a topic reaches every subscribed queue, and each queue is consumed
by an ordinary `SqsQueue` subscription.

Queues are attached to the topic with `subscribe_queue_to_topic`, which subscribes with raw
message delivery enabled, so payloads and headers arrive unwrapped as plain SQS messages instead
of inside an SNS envelope:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:wiring"
```

Topology administration runs on the broker's own lifecycle ladder rather than through the
application builder: in production the topic and its subscriptions are provisioned as
infrastructure. The example wires them from an `after_startup` hook, where the queues already
exist because the subscriptions opened them, and publishes one notification through the
`SnsPublish` policy:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:app"
```

## Payloads and headers

Headers travel as SQS message attributes, one attribute per header: `String` for values that are
valid UTF-8 and `Binary` for the rest. No envelope format is invented, so any other SQS producer
or consumer reads the same message.

The body is the one transport constraint. SQS bodies are text: a UTF-8 payload passes through
untouched, and a payload that is not valid UTF-8 travels base64-encoded with a marker attribute
and is decoded transparently on receive.

A handler that parses the body itself - a queue fed by a producer outside this framework, a wire
format with no `serde` model - takes the framework's byte lane instead of a decoded payload: a
`#[derive(Deserialized)]` newtype over `&[u8]`, with no codec anywhere on the path. The base64
hop above is already undone by then, so the handler sees the bytes the producer sent.

## Local development with LocalStack

The repository ships a LocalStack compose file with the `sqs` and `sns` services, and the just
recipes around it:

```bash
just brokers-up                 # start LocalStack on 127.0.0.1:4566
cargo run --example sqs_service
cargo run --example sns_fanout
just brokers-down
```

Point the broker at the stack with `endpoint`, `test_credentials`, and an explicit `region`, as
both examples do. The live test suite is gated on `SQS_TEST_ENDPOINT`, and skips when it is
unset:

```bash
just test-brokers               # LocalStack up, integration + conformance, LocalStack down
```

or, against an already running stack:

```bash
SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --workspace --all-features -- --test-threads=1
```

The same suite runs in CI: the integration tests plus the framework's conformance lifecycle check,
which walks `new` -> `connect` -> subscribe -> publish -> receive -> ack -> `shutdown` and asserts
that a publisher created before shutdown errors afterwards.

## Testing

The `testing` feature ships `SqsTestBroker`: an in-process broker that reproduces the crate's core
routing with no server and no network. It follows the same ladder as the real broker, and its
connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite in process; inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

It routes by exact queue name and does not simulate SQS product behaviour: visibility timing,
redelivery, redrive dead-lettering, FIFO ordering, and SNS fan-out are covered by the live suite
against LocalStack instead.
