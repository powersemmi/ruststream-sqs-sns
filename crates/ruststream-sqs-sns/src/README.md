Amazon SQS transport for the [`RustStream`](https://docs.rs/ruststream) messaging framework,
with SNS fan-out publishing.

Handlers, routers, codecs and middleware come from the framework; this crate supplies the
transport over the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) and
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) clients. One subscription is one queue, long-polled.
SNS is a publisher only: its delivery targets are queues and HTTP endpoints rather than a
consumer this crate would own, so a topic fans out to queues and each queue is consumed the
ordinary way.

A queue is not a log, and the framework's optional capabilities fall out of that.
[`Subscribe`](ruststream::Subscribe), [`BatchSubscriber`](ruststream::BatchSubscriber),
[`Partitioned`](ruststream::Partitioned) and [`DescribeServer`](ruststream::DescribeServer) are
native. [`TransactionalPublisher`](ruststream::TransactionalPublisher),
[`OwnedTransactions`](ruststream::OwnedTransactions), [`RequestReply`](ruststream::RequestReply)
and [`Seekable`](ruststream::Seekable) are not implemented, because SQS has no transactional
send, no reply inbox and no cursor to move; a mount that asks for one does not compile.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

Two additive features sit on top of the default build: `testing` ships the in-process transport
of the [`testing`](crate::testing) module, and `asyncapi` adds the `sqs` bindings described under
[the generated document](#the-generated-document). The runnable services this page is drawn
from are in `examples/`:
<https://github.com/powersemmi/ruststream-sqs-sns/tree/main/crates/ruststream-sqs-sns/examples>.

# A service

A handler takes the decoded payload, [`SqsQueue`] names the queue it comes from, and
[`SqsBroker`] is what the application mounts it on. Construction is synchronous and does no I/O,
so the whole service is one `#[app]` function.

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
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(handle);
    })
}
```

`cargo run -- run` starts it. The broker is a ladder of consuming transitions:
`SqsBroker::new()` records configuration, the runtime's one `connect` resolves credentials and
builds the SDK clients, and `shutdown` consumes the connected form, so subscribing or publishing
after it does not compile. What stays dynamic is aliasing: a publisher handed out before
shutdown reports [`SqsError::NotConnected`] afterwards instead of succeeding against a connection
the application has given up.

# Subscribing

## The queue descriptor

[`SqsQueue`] describes one subscription, by queue URL or by queue name; a name resolves through
`GetQueueUrl` once and is cached. The parameters that decide cost and latency are the whole
surface:

| Step | Meaning | Default |
| --- | --- | --- |
| [`wait`](SqsQueue::wait) | long-polling wait per receive call | 20 seconds, the protocol cap |
| [`visibility`](SqsQueue::visibility) | the visibility timeout asked for per receive, within `1s..=12h` | the queue's own timeout |
| [`create_if_missing`](SqsQueue::create_if_missing) | creates the queue on subscribe | off |

An empty name, a wait above the cap or a visibility outside the range is
[`SqsError::InvalidQueue`] at startup, before any call to AWS. The same three steps are reachable
at the mount site through [`SqsSubscription`], which is where they go when the registration names
a framework setting first: `b.include(reconcile.batch(nonzero!(10)).wait(..))`.

`#[subscriber("orders")]` takes the descriptor with its defaults, and `#[subscriber(SqsQueue)]`
fixes the kind while leaving the name to `b.include(handle.name("orders"))`, which is how one
definition serves two queues. A name the service uses need not be a legal SQS name: on the way to
the service every character outside `[A-Za-z0-9_-]` becomes `-` and a `.fifo` suffix survives, so
a dotted framework name stays routable. Subscriptions and queue publishes share that mapping.

A subscription is a stream fed by a background pump. Its channel holds one batch, so the pump
never runs more than one receive ahead of the handler. Dropping the stream stops the pump, and
deliveries it had already handed over redeliver once their visibility lapses. A receive error
reaches the stream as an item: a queue that does not exist ends the stream, and anything else
backs off for a second, so a persistent failure cannot spin the loop.

## Settlement

Every settlement is a native call, and deferred retry among them:
[`ack`](ruststream::runtime::HandlerOutcome::ack) deletes the message,
[`retry`](ruststream::runtime::HandlerOutcome::retry) zeroes its visibility,
[`retry_after`](ruststream::runtime::HandlerOutcome::retry_after) sets that visibility to the
delay, capped at the protocol's 12 hours, and
[`drop`](ruststream::runtime::HandlerOutcome::drop) deletes as well, SQS having no verb for
rejecting a message without deleting it. A deferred delivery therefore waits in place and comes
back on the same queue with its receive count intact: nothing is republished and no copy is made,
so the framework's broker-agnostic fallback never runs here and `.out_retry(..)` does not compile
on an [`SqsQueue`].

A handler may outlive the visibility timeout, and SQS has no lease renewal call, so the crate
re-arms the visibility every half period for as long as the delivery is alive. It re-arms the
value the delivery is held under: what the descriptor named, or the queue's own timeout when the
descriptor named none. That timeout is read once when the subscription opens, which is why a
subscription naming no visibility of its own refuses to start without `sqs:GetQueueAttributes`.
Dropping a delivery unsettled makes no call at all, and the message returns when its current
visibility lapses.

## Capping the attempts

`max_attempts(n)` and `dead_letter(name)` right after `include` are one setting here, the queue's
redrive policy: the cap is its `maxReceiveCount`, the destination is the queue it points at, and
the subscription writes both onto the queue when it opens. SQS then counts the receives and
carries a spent delivery away itself, so nothing leaves the service.

```
use ruststream_sqs_sns::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Invoice {
    id: u64,
}

#[subscriber(SqsQueue::new("invoices"))]
async fn settle(invoice: &Invoice) -> HandlerOutcome {
    if invoice.id == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(settle)
            .max_attempts(nonzero!(3u32))
            .dead_letter("invoices-dead");
    })
}
```

The queue is named either way. A registration mounted by a bare name, with no descriptor
between it and the broker, has nowhere of its own to keep the declaration, so the broker takes it
and writes the same policy onto the queue that name opens.

Both halves are needed, because half a redrive policy is not one: a cap without a destination, or
a destination without a cap, is [`SqsError::IncompleteRedrive`] at startup and names the half
that is missing. The dead-letter queue has to exist by then unless the descriptor carries
`create_if_missing`, and it has to match the queue it serves - a FIFO queue takes a FIFO
dead-letter queue.

One thing changes under a declaration. A discard normally deletes, but the delivery that has used
up the policy's receives is returned to the queue instead, because being received once more is
how SQS moves it to the dead-letter queue and a delete there would lose it.

## Batches

Batches are the transport's own. `ReceiveMessage` is already a batching call, so the size a
registration names becomes `MaxNumberOfMessages` and one receive is one batch; nothing buffers on
the client. A batch comes back shorter when that is all the queue had, and never empty - a long
poll that times out yields no batch at all. `ReceiveMessage` returns at most ten messages, so a
larger size is clamped to ten and the clamp is logged once for the subscription.

```
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue::new("orders"))]
async fn reconcile(orders: &[Order]) -> HandlerOutcome {
    println!("reconciling {} orders", orders.len());
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        // The framework's step first, then this crate's vocabulary on the same chain.
        b.include(
            reconcile
                .batch(nonzero!(10))
                .wait(Duration::from_secs(20))
                .create_if_missing(),
        );
    })
}
```

A single-message subscription still asks for a whole call's worth, because SQS bills per request
rather than per message, and hands the messages over one at a time. How many of them run at once
is `workers(n)`, a framework setting.

## What a delivery carries

Each header is one SQS message attribute, `String` for a value the service takes as text and
`Binary` for the rest. No envelope is invented, so any other SQS producer or consumer reads the
same message, and which attribute type carried a value is invisible to a handler.

Two headers have meanings of their own. [`PARTITION_KEY_HEADER`] carries the FIFO message group
and never becomes an attribute: a FIFO destination sends it as the native `MessageGroupId` and
returns it on delivery, a standard queue drops it. [`RECEIVE_COUNT_HEADER`] carries the queue's
own `ApproximateReceiveCount`, which counts the delivery in hand, so the first attempt reads one
and a handler can treat the last attempt differently from the first.

The body is the one transport constraint. SQS bodies are text, and what counts as text there is
narrower than UTF-8, so a payload the service would refuse travels base64-encoded with a marker
attribute and is decoded again on receive - a binary payload, and equally the valid-UTF-8 control
characters a binary codec emits. A handler that parses the bytes itself takes a
`#[derive(Deserialized)]` newtype over `&[u8]`, and sees what the producer sent, the base64 step
already undone.

This crate registers no per-delivery context field, so there is nothing for a `Ctx<K>` parameter
to read here; what a delivery carries beyond its payload is in its headers.

# Publishing

A publish policy is pure declaration, constructible anywhere, and the runtime pairs it with the
connected broker at startup. Which policy a position is bound to picks the destination kind:
[`SqsPublish`] sends directly to a queue, [`SnsPublish`] publishes a notification to a topic,
named by ARN or by name through the idempotent `CreateTopic`. [`SqsPublish`] is also the broker's
default policy, so a queue-to-queue service binds nothing.

A publisher can also be taken from the broker - [`SqsBroker::publisher`] before the application
starts, [`ConnectedSqsBroker::publisher`] and [`ConnectedSqsBroker::sns_publisher`] from the
connected form. Each shares the broker's connection and reports [`SqsError::NotConnected`] after
shutdown.

## Replies and fan-out

The reply type says where the reply goes and the mount site says who takes it there. The example
below is the whole of the fan-out wiring: without `.out_reply(..)` the same reply would ride
[`SqsPublish`] and land on a queue of that name.

```
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct PlaceOrder {
    id: u64,
}

/// The type fixes its destination, so the clause is the bare `publish`.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "orders-events")]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue::new("orders"), publish)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(accept).out_reply(SnsPublish::default());
    })
}
```

Attaching the queues to the topic is provisioning work, not application wiring, and it runs on
the broker's own ladder:
[`subscribe_queue_to_topic`](ConnectedSqsBroker::subscribe_queue_to_topic) turns on raw message
delivery, so payloads and headers arrive unwrapped as plain SQS messages instead of inside an SNS
envelope. In production the topic and its subscriptions are infrastructure; the `sns_fanout`
example wires them from an `after_startup` hook, which is also where a service publishes outside
a handler: `b.after_startup(Publish::default(), hook)`.

## FIFO message groups

A `.fifo` destination requires a message group, and [`SqsPublishOptions`] is the pair of settings
one publish may differ from the next in: a group id and a deduplication id. The call site names
either with the steps of [`SqsPublishSteps`], and the mount site fixes a position's group on the
policy, `Publish::default().group_id("orders")`. A group belongs to a position; a deduplication
id is the idempotency key of one message, and a constant one would collapse a position's whole
output into a single delivery, so the policy does not carry it. Left unnamed, it is a
process-unique id, which is what keeps two identical payloads sent on purpose from being
collapsed.

Three answers can be in play at once and they resolve from the most specific: the call's own
step, then the message's [`PARTITION_KEY_HEADER`] - the spelling that travels unchanged to every
other broker - then the group the mount site fixed. A send that names none at all goes under
`"default"`.

```
use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    customer: String,
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Shipments;

// A body that names a per-message setting is the one handler file that globs this crate's
// prelude, and it bounds the slot on the options type.
#[subscriber(SqsQueue::new("orders"))]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome {
    if shipments
        .message(order)
        .to("shipments.fifo")
        .group_id(&order.customer)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(ship)
            .out(Shipments, Publish::default().group_id("orders"))
            .build();
    })
}
```

A step is a position on the builder rather than a wrapper around the publisher, so the publish it
finishes still encodes with the codec the mount site named and runs the transforms it mounted.
The same steps are on every publish surface: an injected slot, a publisher a lifecycle hook is
handed, a publisher taken from the broker. A reply adjusts nothing, having no call site - the
policy `.out_reply(..)` bound is its whole answer.

Both settings are FIFO settings. Naming either for a standard queue or topic is
[`SqsError::NotFifo`] rather than a value dropped in silence. A [`PARTITION_KEY_HEADER`] is not
an ask of this broker, so a standard queue keeps ignoring it.

# The prelude

`use ruststream_sqs_sns::prelude::*;` is the whole import list of a routes file: the framework's
own prelude, plus this crate's broker, its queue descriptor and mount-site settings trait, its
publish policies and its live publishers. [`SqsPublish`] arrives there under the uniform name
`Publish`, the one a mount site and the lifecycle hooks write; [`SnsPublish`] keeps its own name,
fan-out being the departure rather than the default. Everything the framework contributes comes
through unchanged, so a service on two brokers globs both preludes and what they share resolves
to one item.

A handler file globs the framework's prelude instead, where `Publisher` is the capability an
injected publisher is bounded on. The one exception is the body that names a per-message setting,
as in the FIFO example above: it globs this crate's prelude for [`SqsPublishSteps`]. See
[`prelude`](crate::prelude) for the full list.

# The generated document

The `asyncapi` feature forwards the framework's and fills in what only SQS knows. Every channel a
queue descriptor opens carries an `sqs` channel binding:

```json
{
  "sqs": {
    "bindingVersion": "0.3.0",
    "queue": {
      "name": "orders",
      "fifoQueue": false,
      "visibilityTimeout": 30,
      "receiveMessageWaitTime": 20
    }
  }
}
```

`name` and `fifoQueue` come from the queue's name, the `.fifo` suffix being what makes a queue
FIFO, and the two timings from the polling settings the descriptor names. A setting left to the
queue is left out rather than guessed: reading it takes a connection and the document is built
before anything connects. For the same reason a queue's ARN never appears, and neither does a
credential, even when the broker is configured from an endpoint that carries one. One server
describes the crate, with the protocol `sqs` and no protocol version, and SNS publishes share it.

A publish position describes its destination too, because the framework hands the policy the name
the document reports as the channel's address. A reply through [`SqsPublish`] carries an `sqs`
binding with the queue's `name` and `fifoQueue`; one through [`SnsPublish`] carries an `sns`
binding with the topic's `name`. A `.fifo` topic reports `ordering.type` as `FIFO` and
`ordering.contentBasedDeduplication` as `false`, since every FIFO send this crate makes carries a
deduplication id of its own and an explicit id wins over one the topic would derive; a standard
topic reports no ordering, which the specification reads as unordered. The polling settings stay
on the subscription's half of the channel, a publish position having none.

Two things the document does not say. Neither policy answers a reply address, so none is
reported. And the specification's `redrivePolicy` and `deadLetterQueue` fields stay empty,
because a descriptor's bindings are read where the handler is included and the declaration
arrives after that; the framework reports the cap on the operation and the dead-letter queue as a
channel the registration sends to, which covers the same ground.

# Testing

The `testing` feature ships [`SqsTestBroker`](crate::testing::SqsTestBroker), an in-process
transport on the same ladder as the real one, teardown included. The declaration a service ships
mounts on it unchanged: [`SqsQueue`] is a subscription source there too, and [`SqsPublish`] and
[`SnsPublish`] pair into one publisher, so a routes file is tested as written rather than
rewritten. Drive it with the framework's harness, whose overview covers the assertions and what
a test can say:
<https://docs.rs/ruststream/latest/ruststream/testing/index.html#what-a-test-can-say>.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue::new("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::main(worker_threads = 2)]
pub async fn run() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(SqsTestBroker::new(), |b| {
            b.include(handle);
        });
    let tb = TestApp::start(app).await.expect("the app starts");

    tb.broker::<SqsTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("the publish drives the handler to a standstill");
    tb.broker::<SqsTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the app shuts down");
}
# }
# fn main() {
#     #[cfg(feature = "testing")]
#     demo::run();
# }
```

What the stand-in answers is routing by exact queue name and the settlement a handler asked for,
the delay and the cap included: `retry_after` holds a delivery back and returns it once the delay
has passed, receives are counted the way the queue counts them, and a registration's redrive
policy moves a spent delivery to the dead-letter queue. What belongs to the queue itself it does
not answer - a visibility timeout that lapses on its own, what a long poll costs, FIFO ordering,
and the onward delivery of an SNS fan-out. Those hold against SQS, and the repository's live
suite asserts them against the service itself.

# Operations

* Credentials and region resolve on `connect` through the SDK's default chain (environment,
  shared profile, IMDS, SSO). [`SqsBroker::from_config`] takes a prebuilt `aws_config::SdkConfig`
  instead, which is also how a service pins an SDK behaviour version rather than tracking the
  latest.
* [`endpoint`](SqsBroker::endpoint), [`region`](SqsBroker::region) and
  [`test_credentials`](SqsBroker::test_credentials) point the broker at a local stack; queue URLs
  the service returns are rebased onto that endpoint, so a stack's host-rewriting strategy changes
  nothing.
* Transport security is the SDK's default HTTPS client; this crate adds no TLS surface of its own.
* `connect` sets a per-attempt timeout of 25 seconds, above the 20 a long poll can wait, since a
  shorter one would kill every receive.
* A subscription needs `sqs:GetQueueUrl`, `sqs:ReceiveMessage`, `sqs:DeleteMessage` and
  `sqs:ChangeMessageVisibility`; one that names no visibility also needs
  `sqs:GetQueueAttributes`. A declared cap adds `sqs:SetQueueAttributes` on the queue and
  `sqs:GetQueueAttributes` on the dead-letter queue, `create_if_missing` adds `sqs:CreateQueue`,
  and publishing to a topic adds `sns:CreateTopic` and `sns:Publish`.
* The service's own limits are the crate's: ten messages per receive, twenty seconds of long
  polling, twelve hours of invisibility - which is also the ceiling on a deferred retry.
* Known gaps: no transactions, no request/reply and no seeking, none of which SQS offers; SNS has
  no subscriber here; and a publish contributes nothing to the generated document.
