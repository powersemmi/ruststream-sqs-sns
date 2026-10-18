---
title: 消息组
sidebar_position: 2
description: "FIFO 队列按顺序投递每个消息组：组中有消息在传输中时，它的下一条消息等待，其他组照常进行。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个带三个消息组的 FIFO 队列：有消息在传输中的组等待，另外两个组照常投递",
  "text": "以文字阅读本图",
  "queue": "FIFO 队列 orders.fifo",
  "g0": "alice",
  "g1": "bob",
  "g2": "carol",
  "inFlight": "传输中",
  "held": "等待",
  "delivered": "已投递",
  "next": "排队中",
  "consumer": ["消费者：一次 ReceiveMessage", "取得 bob 1 和 carol 1，alice 等待"],
  "deduplication": ["五分钟内重复的 MessageDeduplicationId", "被接受，但不投递"],
  "receives": "接收"
};

# 消息组

FIFO 队列按顺序投递每个消息组：组中有消息在传输中时，它的下一条消息等待，其他组照常进行。

<Diagram name="groups" words={words} />

每次向 FIFO 队列发送都要指定一个 `MessageGroupId`。同一组的消息按发送顺序投递。组中有一条消息在传输中时，队列不会交出该组的其他消息。这条消息被删除，或者它的可见性超时到期之后，该组才继续。不同的组互不等待，因此组就是并行工作的单位。

每次发送还带有一个 `MessageDeduplicationId`。五分钟内重复某个 id 的发送会被接受，但不会投递。启用基于内容的重复数据删除时，队列用正文的 SHA-256 哈希生成这个 id，发送时指定的 id 优先。

本 crate 从最具体的指定处选择一次发布的组：先看调用的 [`SqsPublishOptions`](api:ruststream_sqs_sns::SqsPublishOptions)，再看消息的 [`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER)，再看策略固定的组，最后是 `default`。它总是发送一个重复数据删除 id：调用指定的那个，或者在本进程内唯一的一个。因此，同一正文发送两次就是两条消息，除非服务为两次发送指定同一个 id。为标准队列设置组或重复数据删除 id 会被拒绝，返回 [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo)。投递时，组出现在 `PARTITION_KEY_HEADER` 中，[工作分区](../lanes.md)按它划分各个工作者。

SNS FIFO 主题只投递到 FIFO 队列，消息在途中保留它的组，[主题](topics.md)一页用图说明了这一点。

这一行为就是 Amazon SQS 自己的行为（[FIFO 关键术语](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-key-terms.html)、[消息组 ID](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html)、[重复数据删除 ID](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)）。
