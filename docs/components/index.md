---
sidebar_position: 0
description: "The parts of ruststream-sqs-sns an SQS service uses, one page per component."
---

# Components

The parts of `ruststream-sqs-sns` an SQS service uses, one page per component: the broker
([`SqsBroker`](api:ruststream_sqs_sns::SqsBroker)), its queue subscriptions, its publish policies
for queues and SNS topics, and the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.

One subscription reads one queue with long polling, and SQS settles each delivery with its own calls.
A slice handler receives a whole `ReceiveMessage` call as one batch.
A FIFO queue carries the framework's partition key as its message group.
SNS is a publisher here: a topic fans a message out to the queues subscribed to it, and each queue is read the ordinary way.
With the `asyncapi` feature, the generated document describes the queues and topics in SQS and SNS terms.

| Category | What it covers |
|---|---|
| [Broker](./broker/index.md) | the broker, its credentials and endpoint, startup, shutdown, the prelude and the error type |
| [Subscriptions](./subscriptions/index.md) | the queue a handler reads, long polling, visibility, queue creation, batches and FIFO queues |
| [Settlement](./settlement/index.md) | what each outcome does to the message, deferred retries and dead-letter queues |
| [Delivery](./delivery/index.md) | what a delivery carries: headers as message attributes, the receive count, the message group and the body |
| [Publishing](./publishing/index.md) | sending to queues, FIFO message groups and deduplication ids |
| [SNS](./sns/index.md) | publishing to SNS topics and subscribing queues to them |
| [AsyncAPI](./asyncapi/index.md) | the server, channel and topic bindings in the generated document |
| [Testing](./testing/index.md) | the in-process account a test runs on, and tests against a running stack |
