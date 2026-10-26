---
title: Lanes
sidebar_position: 5
description: "One subscription reads one queue through one pump; workers(n, by_key) spreads its messages over lanes keyed by the FIFO message group."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: a FIFO queue, one pump, one stream and two lanes keyed by message group",
  "text": "The diagram as text",
  "queue": ["Queue orders.fifo", "message groups alice, bob and carol"],
  "pump": ["Pump", "one receive ahead of the handlers"],
  "stream": ["SqsSubscriber stream", "one message at a time"],
  "keyed": ["workers(2, by_key)", "lane = hash of the message group"],
  "lane0": ["Lane 1", "alice and carol,", "each in order"],
  "lane1": ["Lane 2", "bob, in order"],
  "standard": ["A standard queue carries no group,", "so its messages rotate over the lanes"],
  "replicas": ["Replicas of the service poll the same queue,", "and SQS hands each message to one of them"],
  "receives": "ReceiveMessage",
  "oneBatch": "a batch",
  "oneAtATime": "one at a time"
};

# Lanes

One subscription reads one queue through one pump; `workers(n, by_key)` spreads its messages over
lanes keyed by the FIFO message group.

<Diagram name="lanes" words={words} />

A subscription's stream yields one message at a time. Without `workers`, one loop runs the handler
for each message in turn, and nothing runs side by side. `workers(n)` runs up to `n` handlers at
once, in no particular order.

`workers(n, by_key)` runs `n` lanes, and a message goes to the lane its key hashes to. The key is
the message's [`PARTITION_KEY_HEADER`](../components/delivery/partition-key.md), which holds
its [FIFO message group](../components/subscriptions/fifo.md). So a group never spreads over two lanes, and the order [Groups](sqs/fifo.md)
keeps on the queue is kept in the service. Lanes fewer than groups serve several groups each. A
standard queue has no groups: a message published there with the header carries it as an
attribute and goes to its key's lane, and a message without it rotates over the lanes.

The pump stays at most one receive ahead of the handlers. A message it received and has not handed
over yet is kept invisible by its extender meanwhile. A FIFO queue also holds back a group while one of its messages is in flight, which keeps a
group's next message out of the service until the previous one settles or its timeout lapses.

Several replicas of a service, or several subscriptions on one queue, compete for its messages:
SQS hands each message to one of them. The in-process transport keeps the order of a standard queue,
which SQS does not, so a test that asserts order uses a FIFO queue.

A [batch handler](path.md) takes one receive at a time, up to the size the mount site names and at
most ten, so a batch is often smaller than that size.
