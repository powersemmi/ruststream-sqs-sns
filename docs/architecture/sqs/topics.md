---
title: Topics
sidebar_position: 3
description: "An SNS topic delivers a copy of every message to each queue subscribed to it, and each queue is then consumed like any other."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A publish to one SNS topic becomes a copy in each of two subscribed queues, each with a consumer of its own",
  "text": "The diagram as text",
  "publisher": ["Publisher", "SnsPublish"],
  "topic": ["Topic orders-events", "a copy for every subscription"],
  "billing": ["Queue billing"],
  "shipping": ["Queue shipping"],
  "billingConsumer": ["Billing service"],
  "shippingConsumer": ["Shipping service"],
  "envelope": ["Without raw delivery, the queue receives the body wrapped", "in an SNS JSON envelope, and the attributes inside it"],
  "publish": "Publish",
  "raw": "raw delivery",
  "receive": "ReceiveMessage"
};

# Topics

An SNS topic delivers a copy of every message to each queue subscribed to it, and each queue is
then consumed like any other.

<Diagram name="topics" words={words} />

A queue subscribes to a topic by its ARN. Every `Publish` to the topic becomes one message in each
subscribed queue, so two services that read two queues both see every message. Within one queue,
the consumers compete as on [Queues](index.md).

With raw message delivery, the queue receives the body as it was published, and the message
attributes become SQS message attributes. Without it, the queue receives a JSON envelope that holds
the body and the attributes. A subscription made by
[`subscribe_queue_to_topic`](api:ruststream_sqs_sns::ConnectedSqsBroker::subscribe_queue_to_topic)
turns raw delivery on, so a handler on the queue reads the same body and headers the publisher
sent.

This crate publishes to SNS and does not consume from it: a topic is read through its queues. A
topic named by name resolves through `CreateTopic`, which returns the ARN of an existing topic and
creates a missing one. The ARN is cached after the first publish. A name that ends in `.fifo` makes
a FIFO topic, which delivers only to FIFO queues. An SNS message is 256 KiB at most by default.

The behaviour is Amazon SNS's own
([SQS as a subscriber](https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html),
[raw message delivery](https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html),
[`CreateTopic`](https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html),
[quotas](https://docs.aws.amazon.com/general/latest/gr/sns.html)).
