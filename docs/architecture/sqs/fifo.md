---
title: Groups
sidebar_position: 2
description: "A FIFO queue delivers each message group in order: while a group has a message in flight, its next message waits, and the other groups go on."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A FIFO queue with three message groups: the group with a message in flight waits, the other two deliver",
  "text": "The diagram as text",
  "queue": "FIFO queue orders.fifo",
  "g0": "alice",
  "g1": "bob",
  "g2": "carol",
  "inFlight": "in flight",
  "held": "waits",
  "delivered": "delivered",
  "next": "queued",
  "consumer": ["Consumer: one ReceiveMessage", "gets bob 1 and carol 1, alice waits"],
  "deduplication": ["A repeat of a MessageDeduplicationId within five minutes", "is accepted and not delivered"],
  "receives": "receive"
};

# Groups

A FIFO queue delivers each message group in order: while a group has a message in flight, its next
message waits, and the other groups go on.

<Diagram name="groups" words={words} />

Every send to a FIFO queue names a `MessageGroupId`. The messages of one group are delivered in the
order they were sent. While a message of a group is in flight, the queue hands out no other message
of that group. The group moves on once that message is deleted or its visibility timeout lapses.
Different groups do not wait for each other, so the groups are the unit of parallel work.

Each send also carries a `MessageDeduplicationId`. A send that repeats an id within five minutes is
accepted and not delivered. A queue with content-based deduplication derives the id from a SHA-256
hash of the body, and an id the send names takes precedence.

This crate picks the group of a publish from the most specific place that names one: the call's
[`SqsPublishOptions`](../../components/publishing/options.md), then the message's
[`PARTITION_KEY_HEADER`](../../components/delivery/partition-key.md), then the group the policy
fixed, then `default`. It always sends a deduplication id: the one the call names, or one unique to
the process. So two sends of the same body are two messages unless the service names the same id
for both. A group or deduplication id set for a standard queue is refused with
[`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo). On delivery, the group arrives in
`PARTITION_KEY_HEADER`, and [Lanes](../lanes.md) keys the workers by it.

An SNS FIFO topic delivers only to FIFO queues, and the message keeps its group on the way, as
[Topics](topics.md) draws.

The behaviour is Amazon SQS's own
([FIFO key terms](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html),
[message group id](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html),
[deduplication id](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)).
