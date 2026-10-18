---
title: Queues
sidebar_position: 0
description: "An SQS queue stores each message until a consumer deletes it, and the consumers that poll it compete: each message goes to one of them."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Producers send to one queue, and two consumers poll it, each message going to one of them",
  "text": "The diagram as text",
  "producers": ["Producers"],
  "queue": "Queue orders",
  "kinds": ["Standard: at least once, best-effort order", "FIFO: in order within a message group"],
  "first": ["Consumer 1", "ReceiveMessage:", "up to 10, waits up to 20 s"],
  "second": ["Consumer 2", "ReceiveMessage:", "up to 10, waits up to 20 s"],
  "retention": ["Each message goes to one consumer. It stays in the queue until", "it is deleted or its retention ends: 4 days by default, up to 14"],
  "sends": "SendMessage",
  "receives": "receive"
};

# Queues

An SQS queue stores each message until a consumer deletes it, and the consumers that poll it
compete: each message goes to one of them.

<Diagram name="queue" words={words} />

A producer sends one message per `SendMessage` call. The queue keeps it until a consumer deletes
it or the retention period ends, four days by default and fourteen at most. A message is a text
body of up to 1 MiB and up to ten message attributes.

A consumer asks for messages with `ReceiveMessage`. One call returns at most ten. With long polling
the call waits up to 20 seconds and answers as soon as a message is there. A received message is
not removed: it stays in the queue, hidden from the other consumers, as [Visibility](visibility.md)
draws. So the consumers that poll one queue share its messages.

A standard queue delivers every message at least once, sometimes twice, and keeps the order only
on a best-effort basis. A FIFO queue, whose name ends in `.fifo`, delivers the messages of one
message group in order, as [Groups](fifo.md) draws.

SQS bills per request, not per message. This crate always asks for ten messages per receive, even
for a subscription that handles one message at a time, and hands the batch over one by one.

The model is Amazon SQS's own
([basic architecture](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-basic-architecture.html),
[long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html),
[message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)).
