---
title: 队列
sidebar_position: 0
description: "SQS 队列保存每条消息，直到某个消费者删除它；轮询同一队列的消费者相互竞争，每条消息只交给其中一个。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "生产者向一个队列发送消息，两个消费者轮询它，每条消息交给其中一个",
  "text": "以文字阅读本图",
  "producers": ["生产者"],
  "queue": "队列 orders",
  "kinds": ["标准队列：至少一次，尽力保持顺序", "FIFO：消息组内保持顺序"],
  "first": ["消费者 1", "ReceiveMessage：", "最多 10 条，最长等 20 秒"],
  "second": ["消费者 2", "ReceiveMessage：", "最多 10 条，最长等 20 秒"],
  "retention": ["每条消息交给一个消费者。它留在队列中，", "直到被删除或保留期结束：默认 4 天，最长 14 天"],
  "sends": "SendMessage",
  "receives": "接收"
};

# 队列

SQS 队列保存每条消息，直到某个消费者删除它。轮询同一队列的消费者相互竞争：每条消息只交给其中一个。

<Diagram name="queue" words={words} />

生产者每调用一次 `SendMessage`，就发送一条消息。队列保存这条消息，直到某个消费者删除它，或者保留期结束。保留期默认四天，最长十四天。一条消息由最大 1 MiB 的文本正文和最多十个消息属性组成。

消费者用 `ReceiveMessage` 请求消息。一次调用最多返回十条。使用长轮询时，调用最长等待 20 秒，一有消息就立即返回。收到的消息并不会被移除：它留在队列中，对其他消费者隐藏，[可见性](visibility.md)一页用图说明了这一点。因此，轮询同一队列的消费者分担它的消息。

标准队列把每条消息至少投递一次，有时投递两次，只尽力保持顺序。FIFO 队列的名称以 `.fifo` 结尾，它按顺序投递同一消息组的消息，[消息组](fifo.md)一页用图说明了这一点。

SQS 按请求计费，而不是按消息计费。本 crate 每次接收总是请求十条消息，即使订阅一次只处理一条消息也是如此，然后把这一批消息逐条交出。

这一模型就是 Amazon SQS 自己的模型（[基本架构](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-basic-architecture.html)、[长轮询](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)、[消息配额](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)）。
