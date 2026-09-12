# Amazon SQS

`ruststream-sqs-sns` runs a RustStream service on Amazon SQS, with SNS fan-out publishing. SQS is
a queue: a delivery is either deleted or returned to the queue, and the visibility timeout decides
when it comes back. The crate covers long polling, visibility-based retries, FIFO message groups,
and ships an in-process test broker under its `testing` feature. For framework concepts (writing
subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

A service file imports `ruststream_sqs_sns::prelude::*` and nothing else from either crate: the
glob re-exports the framework's own prelude together with this crate's broker, queue descriptor,
publish policies and publishers. The framework's items come through unchanged, so a service that
runs on two brokers globs both preludes and what they share resolves to a single item.

The crate's MSRV is 1.94, tracking the AWS SDK; the framework core stays at 1.88, and a dependent
crate may sit above its dependency's floor.

## Capabilities

The framework's optional capabilities, and which of them this broker implements natively. A
capability the broker does not implement is a compile error at the mount site.

| Capability | Native | Why |
| --- | --- | --- |
| `Subscribe` | yes | a queue name is enough: `#[subscriber("orders")]` binds without a descriptor |
| `BatchSubscriber` | yes | `ReceiveMessage` is already a batching call: one receive is one batch (see [Batches](#batches)) |
| `TransactionalPublisher` | no | SQS has no transactional send |
| `OwnedTransactions` | no | SQS has no transactional send |
| `RequestReply` | no | SQS has no reply inbox; a reply is an ordinary send to another queue |
| `Partitioned` | yes | a delivery reports its FIFO message group id as the `partition-key` header, and a publish reads its own group from that header (see [FIFO message groups](#fifo-message-groups)) |
| `Seekable` / `Positioned` | no | a queue keeps no cursor to move; messages that outlive their attempts are recovered from the redrive policy's dead-letter queue |
| `DescribeServer` | yes | `SqsBroker` reports the host and port it connects to, and the `sqs` protocol, into the AsyncAPI document the framework generates |

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
SqsBroker::new()          configuration only, synchronous, no I/O
  .connect()   ->  ConnectedSqsBroker    the live SDK clients; subscriptions and publishers
  .shutdown()  ->  ()                    the terminal transition
```

The runtime calls `connect` once at startup, before the first subscription opens, and region and
credentials resolve from the environment there (profile, IMDS, SSO). Keeping the I/O out of `new`
is what lets an SQS service be assembled with the same `#[ruststream::app]` macro as any other
broker. Because `shutdown` consumes the connected broker, publishing or subscribing after it does
not compile. Shutdown marks the shared client state closed, so a publisher handed out earlier
returns `SqsError::NotConnected` instead of succeeding against a connection the application has
given up.

Configuration sits on the synchronous builder:

- `SqsBroker::new()` resolves everything from the environment.
- `SqsBroker::from_config(config)` takes a prebuilt `aws_config::SdkConfig`, which is also how a
  service pins a fixed SDK behaviour version instead of the latest.
- `endpoint(url)`, `region(name)` and `test_credentials()` point the broker at a local stack.
  Queue URLs returned by SQS are rebased onto the configured endpoint, so the host-rewriting
  strategies local stacks use change nothing for the adapter.

`connect` also sets a per-attempt timeout of 25 seconds on the SDK config it builds, above the 20
seconds a long poll can wait.

## Queue descriptors

`SqsQueue` describes one queue subscription. The parameters that decide cost and latency are
explicit on it:

| Method | Meaning | Default |
| --- | --- | --- |
| `wait(Duration)` | long-polling wait per receive call, capped at the protocol's 20 seconds | 20 seconds |
| `visibility(Duration)` | the visibility timeout asked for on each receive, within `1s..=12h` | the queue's configured timeout |
| `create_if_missing()` | creates the queue on subscribe when it does not exist | off |

The batch size is not among them. It belongs to the registration: a batch handler names it at the
mount site with `batch(n)` (see [Batches](#batches)).

A descriptor is checked before any I/O. An empty name, a wait above the cap or a visibility
outside the range returns `SqsError::InvalidQueue` at subscribe time, with no call to AWS.

A descriptor sits inline in the `#[subscriber(..)]` macro and names the queue by URL or by name; a
name resolves through `GetQueueUrl` and is cached. The examples add `create_if_missing()` because
they run against a local stack with nothing provisioned; a production service leaves it off:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:handler"
```

Mount it on the broker:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

The plain string form `#[subscriber("orders")]` takes the descriptor's defaults. `#[subscriber(SqsQueue)]`
fixes the kind and leaves the name to the mount site, `b.include(handler.name("orders"))`, which
is how one handler definition serves two queues.

A queue name the service uses need not be a legal SQS name: on the way to SQS every character outside
`[A-Za-z0-9_-]` becomes `-`, and a `.fifo` suffix survives. Subscriptions and queue publishes
share that mapping, so a dotted framework name stays routable on a queue.

The same options are reachable at the mount site too, in this crate's own vocabulary, through the
`SqsSubscription` trait in the prelude. A registration that names a batch size first continues
with them there, because the framework's own steps come first on a chain:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_batches.rs:mount"
```

A queue that `create_if_missing` has to create is a plain queue, or a FIFO queue with
content-based deduplication when the name ends in `.fifo`. Production queues are usually managed
as infrastructure.

A subscription is a stream that a background pump fills by long-polling `ReceiveMessage`. Its
channel holds one batch, so the pump never runs more than one receive ahead of what the handler
drains. Dropping the stream stops the pump; messages it had already delivered and that were never
settled redeliver once their visibility lapses. Receive errors reach the stream as items: a queue
that does not exist ends the stream, and any other error backs off for a second, so a persistent
error cannot spin the loop.

## Batches

Batches here are the transport's own. A batch handler names one size at the mount site, that size
becomes `MaxNumberOfMessages`, and one `ReceiveMessage` call is one batch. Nothing buffers on the
client.

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_batches.rs:handler"
```

The size goes on the mount chain first and the queue options chain after it, as in the example
above. A batch comes back shorter than the size when that is all the queue had. It is never empty:
a long poll that times out yields no batch at all.

`ReceiveMessage` returns at most ten messages per call, so a larger size is clamped to ten, and
the clamp is logged once for the subscription.

A single-message handler has no batch. Its subscription still asks for the protocol maximum per
receive, since SQS bills per request rather than per message, and hands the messages to the
handler one at a time. How many of them a handler processes at once is `workers(n)`, a framework
setting.

## Settlement and deferred retry

Every settlement verb is a native SQS operation:

| Handler outcome | SQS operation |
| --- | --- |
| `HandlerOutcome::ack()` | `DeleteMessage` |
| `HandlerOutcome::retry()` | `ChangeMessageVisibility` to 0, so the message redelivers immediately |
| `HandlerOutcome::retry_after(delay)` | `ChangeMessageVisibility` to the delay |
| `HandlerOutcome::drop()` | `DeleteMessage` |

Deferred retry is native as well: `retry_after(delay)` sets the message's visibility to the delay,
capped at the protocol's 12 hours. The message waits in place and redelivers on the same queue
with its receive count intact, since nothing is republished and no copy is made.

The framework's own fallback, which republishes a delayed copy, therefore never runs here. It is
still wired: a queue answers where a deferred copy would go with its own name, so a scope built
with `BrokerScope::retry_via` starts on this broker instead of refusing to. A queue fed by an SNS
topic answers the same way - the copy reaches the queue directly and skips the fan-out, which is
what a redelivery of that one subscription means.

SQS has no discard short of deletion, so poison-message routing belongs to the queue's redrive
policy: after `maxReceiveCount` deliveries SQS moves the message to the dead-letter queue itself.
A handler reads that count in the `sqs-receive-count` header (`RECEIVE_COUNT_HEADER`), the
approximate receive count SQS reports, and can treat the last attempt differently from the first.

## The visibility extender

A handler may run longer than the message's visibility timeout, and SQS has no lease renewal call.
While a delivery is alive, the crate re-arms its visibility every half period, so the queue does
not hand the same message to a second worker while the first is still on it. It re-arms the value
the delivery is held under: what the descriptor named, or the queue's own visibility timeout when
the descriptor named none. That timeout is read from the queue once, when the subscription opens,
so a subscription that names none needs `sqs:GetQueueAttributes` on its queue and refuses to open
without it. The extension stops the moment the message is settled or dropped. A failed extension
is logged at debug level and retried on the next tick. A handler's duration is bounded by the
process, not by the queue's timeout.

Dropping a delivery unsettled makes no further call: the message redelivers when its current
visibility lapses, which is the at-least-once contract.

## FIFO message groups

On a `.fifo` destination every send carries a message group id, because a FIFO queue requires one.
Name it per message with the [`group_id`](#per-message-settings) step, or fix it for a whole
publish position on the policy: `Publish::default().group_id("orders")`. A delivery arrives with
its group in the `partition-key` header, which is also the header a message may name its own group
in - the spelling that travels unchanged to every other broker. A send that names none at all goes
under `"default"`.

Three answers can be in play at once, and they resolve from the most specific: the call's own
step, then the message's `partition-key` header, then the group the mount site fixed.

Every FIFO send also supplies a deduplication id, unique within the process, unless the call names
one with the `deduplication_id` step. An explicit id takes precedence over content-based
deduplication, so two identical payloads sent on purpose are never collapsed into one.

Both settings are FIFO settings. Naming either for a standard queue or topic is a publish error
(`SqsError::NotFifo`): the ordering the caller asked for would not happen there, and a value
dropped in silence is the worse answer. A `partition-key` header is not an ask of this broker, so
a standard queue keeps ignoring it.

## Publishing

A publish policy constructs the live publisher, and the runtime instantiates it on the connected
broker at startup. Naming a policy picks the destination kind:

- `SqsPublish` is the policy that constructs `SqsPublisher`: it sends directly to a queue, named
  by URL or by name. It is also the broker's default publish policy, so a replying handler
  mounted with no policy of its own sends through it.
- `SnsPublish` constructs `SnsPublisher`: it publishes a notification to an SNS topic, named by
  ARN or by name. A topic name reaches the idempotent `CreateTopic` exactly as written, so a
  topic is addressed by the name SNS itself accepts.

The reply type declares where the reply goes: `#[outgoing(name = "..")]` on it is the destination.
A reply type that declares none takes the destination from the handler's `publish("..")` clause.

The mount site names who takes it there. `.out(Reply, policy)` binds it: `Reply` is the marker for
the value a replying handler returns, and the steps after the call (`.codec(..)`, `.transform(..)`)
apply to the position it named. A mount that names no policy keeps `SqsPublish`, so a
queue-to-queue service writes `b.include(handler)` and nothing more. Sending the same reply to a
topic instead is one step on the chain:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:reply"
```

The mount that binds it is in [SNS fan-out](#sns-fan-out). The same policy value goes to the
lifecycle hooks, `b.after_startup(Publish::default(), ..)`, which is where a service publishes
outside a handler.

The prelude also exports `SqsPublish` as `Publish`, the name every broker crate gives the policy
that a mount site and the lifecycle hooks take; the examples write it. `SnsPublish` keeps its own
name, fan-out being the departure rather than the default. Both stay available under their
prefixed names for a file that mixes them.

A publisher can also be taken from the broker itself: `SqsBroker::publisher()` before the
application starts, `ConnectedSqsBroker::publisher()` and `ConnectedSqsBroker::sns_publisher()`
from the connected form. Each shares the broker's connection, and every publish after `shutdown`
returns `SqsError::NotConnected`.

### Per-message settings

What one message may differ from the next in is `SqsPublishOptions`: a message group id and a
deduplication id, both optional. The publish builder takes them as steps, from the
`SqsPublishSteps` trait the prelude exports:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_fifo_group.rs:publish"
```

A step is a position on the builder, not a wrapper around the publisher, so the publish it
finishes still encodes with the codec the mount site named and runs the transforms that mount
named. The same steps are on every publish surface: an injected `Out` slot, a publisher a
lifecycle hook is handed, a publisher taken from the broker.

A handler body that names a step is the one place a handler file imports this crate's prelude
instead of the framework's, and it bounds the slot it publishes through on the options type:

```rust
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome
```

What the call leaves alone is what the mount site fixed:
`.out(Shipments, Publish::default().group_id("orders"))`. A reply adjusts nothing, because it has
no call site - the policy bound to the `Reply` position is its whole answer.

On the way out the group becomes the native `MessageGroupId` rather than a message attribute, and
the delivery carries it back in the `partition-key` header.

## SNS fan-out

SNS appears as a publisher only: its delivery targets are queues and HTTP endpoints, not a
consumer this crate would own. One publish to a topic reaches every subscribed queue, and each
queue is consumed by an ordinary `SqsQueue` subscription.

`subscribe_queue_to_topic` attaches a queue to the topic with raw message delivery, so payloads
and headers arrive unwrapped as plain SQS messages instead of inside an SNS envelope:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:wiring"
```

Topology administration runs on the broker's own lifecycle ladder rather than through the
application builder; in production the topic and its subscriptions are provisioned as
infrastructure. The example wires them from an `after_startup` hook, where the queues already
exist because the subscriptions opened them, then places one order on a queue and lets the
handler's reply fan out. `.out(Reply, SnsPublish::default())` is the whole of the fan-out wiring:

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:app"
```

## Payloads and headers

Each header becomes one SQS message attribute: `String` for a value SQS takes as text, `Binary`
for the rest. No envelope format is invented, so any other SQS producer or consumer reads the same
message. A header value is bytes on both sides of the framework's `HeaderMap`, so which of the two
attribute types carried it is invisible to a service.

The body is the one transport constraint. SQS bodies are text, and what counts as text there is
narrower than UTF-8: the C0 control characters other than tab, newline and carriage return are
refused, and so are the two non-characters at the end of the basic plane. A payload SQS accepts
passes through untouched. Anything else is base64-encoded with a marker attribute and decoded
again on receive: a binary payload, and equally a valid-UTF-8 one carrying those characters, which
is what a binary codec often emits. The same rule picks the attribute type for a header value, so
a publish is never rejected over bytes SQS refuses.

A handler can parse the body itself, for a queue fed by a producer outside this framework or a
wire format with no `serde` model: a `#[derive(Deserialized)]` newtype over `&[u8]` takes the
framework's byte lane, with no codec anywhere on the path. The base64 step is already undone by
then, so the handler sees the bytes the producer sent.

## Local development with LocalStack

The repository ships a LocalStack compose file with the `sqs` and `sns` services, and the `just`
recipes around it:

```bash
just brokers-up                 # start LocalStack on 127.0.0.1:4566
cargo run --example sqs_service
cargo run --example sqs_batches
cargo run --example sns_fanout
just brokers-down
```

Point the broker at the stack with `endpoint`, `test_credentials` and an explicit `region`, as the
examples do. The live test suite runs when `SQS_TEST_ENDPOINT` is set and skips when it is not:

```bash
just test-brokers               # LocalStack up, integration + conformance, LocalStack down
```

or, against an already running stack:

```bash
SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --workspace --all-features -- --test-threads=1
```

CI runs the same suite against LocalStack on every change.

Every conformance suite this crate's capabilities justify runs twice: once against the in-process
stand-in and once against the stack. The lifecycle ladder and `capabilities::batches` have both
legs; the routing suite is in process only. `request_reply`, `transactions`, `owned_transactions`
and `seeking` have no leg at all
here - SQS offers no request-reply channel, no transactions and no cursor to seek, so the crate
implements none of those capabilities and their suites do not apply.

## Testing

The `testing` feature ships `SqsTestBroker`: an in-process broker that reproduces the crate's core
routing with no server and no network, on the same ladder as the real one - teardown included,
where a publisher that aliased the connection reports `NotConnected` instead of routing into a
cleared router. It lives in the crate's `testing` module, which a test file imports by name:
`use ruststream_sqs_sns::testing::SqsTestBroker;`.
Build the application on it and the `TestApp` harness drives your real handlers, codecs and
middleware: publish an input, then assert on what a handler received and on what it published
downstream. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

The declaration a service ships mounts there unchanged. `SqsQueue` is a subscription source for
the stand-in as well, so `#[subscriber(SqsQueue::new("orders").wait(..))]` and the mount-site
settings chained onto it run the wiring the service actually runs. The options stop at the
descriptor: in process there is no long poll for `wait` to bound, no redelivery clock for
`visibility` to arm, and no queue for `create_if_missing` to create.

The publish half matches. `SqsPublish` and `SnsPublish` pair against the stand-in, which names
`SqsPublish` as its default policy, so `.out(Reply, Publish::default())` and the default reply of
a `publish(..)` handler mount as written. The per-message settings arrive there too: a step a body
names reaches the stand-in's publisher as it reaches the real one, the harness records it
(`tb.out::<Shipments>().assert_called_once().with_options(..)`), and the resolved group is
delivered in the `partition-key` header exactly as SQS delivers it. Both policies pair into the
one `SqsTestPublisher`,
because the router has no topic to fan out from: a test proves the reply took the destination the
policy names, not that SNS delivered it onward to the queues subscribed to that topic.

It routes by exact queue name. What belongs to SQS itself (visibility timing, redelivery,
dead-lettering through the redrive policy, FIFO ordering and SNS fan-out) is answered by the live
suite against LocalStack.

Batches are the one place the two transports differ inside: in process the framework's client-side
buffer assembles them, while the real subscriber takes them from `ReceiveMessage`. A mount names a
size and gets batches of at most that size either way, which is what makes a batch handler
testable in process at all.
