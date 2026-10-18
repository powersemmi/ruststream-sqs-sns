---
title: 主题
sidebar_position: 3
description: "SNS 主题把每条消息的一份副本投递给订阅它的每个队列，然后每个队列像其他队列一样被消费。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "向一个 SNS 主题发布一次，两个已订阅的队列中各得到一份副本，每个队列有自己的消费者",
  "text": "以文字阅读本图",
  "publisher": ["发布者", "SnsPublish"],
  "topic": ["主题 orders-events", "每个订阅一份副本"],
  "billing": ["队列 billing"],
  "shipping": ["队列 shipping"],
  "billingConsumer": ["计费服务"],
  "shippingConsumer": ["发货服务"],
  "envelope": ["不启用原始消息传输时，队列收到的正文包装在", "SNS JSON 信封中，属性也在信封里"],
  "publish": "Publish",
  "raw": "原始消息传输",
  "receive": "ReceiveMessage"
};

# 主题

SNS 主题把每条消息的一份副本投递给订阅它的每个队列，然后每个队列像其他队列一样被消费。

<Diagram name="topics" words={words} />

队列通过 ARN 订阅主题。向主题的每次 `Publish` 都在每个已订阅的队列中变成一条消息，因此读取两个队列的两个服务都能看到每条消息。在一个队列内部，消费者相互竞争，与[队列](index.md)一页相同。

启用原始消息传输时，队列收到的正文与发布时相同，消息属性变成 SQS 消息属性。不启用时，队列收到一个 JSON 信封，正文和属性都在其中。由 [`subscribe_queue_to_topic`](api:ruststream_sqs_sns::ConnectedSqsBroker::subscribe_queue_to_topic) 建立的订阅会开启原始消息传输，因此队列上的处理器读到的正文和消息头与发布者发送的相同。

本 crate 向 SNS 发布，但不从 SNS 消费：主题通过它的队列读取。按名称指定的主题通过 `CreateTopic` 解析：主题已存在时它返回该主题的 ARN，不存在时它创建主题。第一次发布后，ARN 被缓存。以 `.fifo` 结尾的名称会创建 FIFO 主题，它只投递到 FIFO 队列。SNS 消息默认最大 256 KiB。

这一行为就是 Amazon SNS 自己的行为（[SQS 作为订阅者](https://docs.aws.amazon.com/zh_cn/sns/latest/dg/sns-sqs-as-subscriber.html)、[原始消息传输](https://docs.aws.amazon.com/zh_cn/sns/latest/dg/sns-large-payload-raw-message-delivery.html)、[`CreateTopic`](https://docs.aws.amazon.com/zh_cn/sns/latest/api/API_CreateTopic.html)、[配额](https://docs.aws.amazon.com/zh_cn/general/latest/gr/sns.html)）。
