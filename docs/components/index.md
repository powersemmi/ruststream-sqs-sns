---
sidebar_position: 0
description: "The parts of ruststream-sqs-sns an SQS service uses, one page per component."
---

# Components

The parts of `ruststream-sqs-sns` an SQS service uses, one page per component: the broker
([`SqsBroker`](api:ruststream_sqs_sns::SqsBroker)), its queue subscriptions, its publish policies
for queues and SNS topics, and the in-process transport a
[`TestApp`](api:ruststream::testing::TestApp) test runs on.
