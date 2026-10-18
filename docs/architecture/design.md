---
title: Design
sidebar_position: 2
description: "Where ruststream-sqs-sns sits: between the RustStream runtime and the AWS SDK, one pump per subscription, one set of SDK clients per runtime."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the AWS SDK clients and SQS and SNS, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement",
  "crate": "ruststream-sqs-sns",
  "subscriber": ["SqsSubscriber", "a pump per queue"],
  "publishers": ["SqsPublisher,", "SnsPublisher"],
  "message": ["SqsMessage", "visibility extender"],
  "policies": ["SqsPublish,", "SnsPublish policies"],
  "broker": ["SqsBroker", "configuration"],
  "descriptor": ["SqsQueue", "descriptor"],
  "sdk": "AWS SDK for Rust",
  "clients": ["aws-sdk-sqs and aws-sdk-sns clients", "one set per runtime that sends"],
  "aws": "Amazon SQS and Amazon SNS",
  "inProcess": ["Under the testing feature, an in-process account", "takes the place of the clients and of AWS"],
  "mounted": "mounted",
  "polls": "polls the stream",
  "https": "HTTPS requests"
};

# Design

Where `ruststream-sqs-sns` sits: between the RustStream runtime and the AWS SDK, one pump per
subscription, one set of SDK clients per runtime.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over the
[AWS SDK for Rust](https://docs.rs/aws-sdk-sqs/latest/aws_sdk_sqs/). The runtime above it is the
same for every broker. Everything SQS-specific is in this crate: the
[`SqsQueue`](../components/subscriptions/sqs-queue.md) descriptor, [what an outcome does to a
message](../components/settlement/index.md), and how a reply reaches a queue or a topic. The crate subscribes, takes batches and partition keys, and
describes its server in the generated document. Publishing to SNS needs the `sns` feature.

Each subscription is one queue, read by a pump task of its own. The pump long-polls the queue and
keeps at most one batch ahead of the handlers. Each delivery starts a task that extends its
visibility while a handler holds it. The pump and the extenders run on the runtime that connected
the broker, whichever thread opened the stream.

The connected broker and every publisher share one connection: the SDK clients, a closed flag, and
caches of queue URLs and topic ARNs. The SDK opens an HTTP connection on the runtime of the
request that needs it, and the connection makes progress only while that runtime runs. So each
runtime sends through clients of its own. The broker's runtime uses the set that connect built. A
handler thread with a runtime of its own builds a set on its first request, from the same
configuration and credentials, and a busy thread never stalls the broker's connections.

[`SqsPublish`](../components/publishing/index.md) sends to a queue with `SendMessage`, and
[`SnsPublish`](../components/sns/index.md) publishes to a topic with `Publish`. The
publishers take the buffer the reply was encoded into, and a body SQS accepts as text becomes the
request body without a copy. Any other payload travels base64-encoded with a marker attribute and
is decoded on receive.

## In-process transport

Under the `testing` feature, a [test](../components/testing/index.md) connects the broker in process instead. The connected broker,
its subscriptions, its publishers and its deliveries then talk to an account that lives in the
test. A build without the feature carries no trace of it: the compiler checks that the transport
and the delivery are exactly the size of their live forms.

The account answers the calls this crate makes, and refuses what SQS refuses with the error a live
call reports. It models what a service's logic depends on: competing receivers, receives of up to
ten, visibility and its extension, the receive count, the redrive to a dead-letter queue, FIFO
groups and the five-minute deduplication window, and SNS fan-out to subscribed queues.

Some things belong to the live service only. Every queue and topic the service names exists in
process, and a message sent to a queue no subscription reads is dropped. A descriptor that names
no visibility gets 30 seconds, the default of a new queue. A standard queue keeps its order in
process, while SQS does not, so a test asserts order only on a FIFO queue. Credentials are not
checked.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).
