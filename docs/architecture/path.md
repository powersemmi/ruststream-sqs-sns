---
title: Path
sidebar_position: 4
description: "The way of an SQS message through a RustStream service: the receive, the delivery, the handler, the reply and the delete, one at a time or in batches."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of a message, one at a time with its reply on the left and in batches on the right, from the receive to the delete",
  "text": "The diagram as text",
  "single": "One at a time",
  "batches": "Batches",
  "receive": ["ReceiveMessage", "10, wait 20 s"],
  "channel": ["Channel", "holds one batch"],
  "message": ["SqsMessage", "handed over one by one"],
  "handler": ["Handler", "decodes and runs"],
  "reply": ["Reply: SendMessage", "or an SNS Publish"],
  "ack": ["ack", "DeleteMessage"],
  "batchReceive": ["ReceiveMessage", "size n, up to 10"],
  "batchChannel": ["Channel", "holds one batch"],
  "batchMessages": ["Vec<SqsMessage>", "never empty"],
  "batchHandler": ["Batch handler", "one call per batch"],
  "batchAck": ["Each element", "its own DeleteMessage"],
  "batch": "a batch"
};

# Path

The way of an SQS message through a RustStream service: the receive, the delivery, the handler, the
reply and the delete, one at a time or in batches.

<Diagram name="path" words={words} />

The pump asks for ten messages per receive, with every message attribute and system attribute,
and waits up to 20 seconds. It names a visibility timeout only when the descriptor names one. A
long poll that ends empty yields nothing, so a batch is never empty. The channel holds one batch,
so the pump is at most one receive ahead of the handlers.

Each message becomes an [`SqsMessage`](api:ruststream_sqs_sns::SqsMessage). The body is copied out
of the SDK's answer, and the message attributes become headers, together with the message group and
the receive count. The delivery also starts its visibility extender. These are a few allocations
per message, and [Benchmarks](/architecture/benchmarks/) publishes what a message costs.

A subscription that handles one message at a time takes the batch apart and hands its messages
over in order. A batch subscription asks for its own size, capped at ten, and its handler takes
the whole batch in one call. Every message of a batch is still deleted with a call of its own.

A reply is encoded into a buffer the dispatch loop reuses. The
[`SqsPublisher`](../components/publishing/index.md) takes that buffer as the body of one
`SendMessage`, or the [`SnsPublisher`](../components/sns/index.md) as the body of one
`Publish`. The queue URL or topic ARN is resolved once and cached.

The delivery settles after the reply. Each settlement goes through the SDK clients of the runtime
that settles it, and what each outcome does is on [Settlement](settlement.md).
