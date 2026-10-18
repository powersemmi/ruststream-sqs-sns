---
title: 可见性
sidebar_position: 1
description: "收到的 SQS 消息留在队列中，在可见性超时内不可见，直到删除结束它，或者它回来等待下一次接收。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条消息：接收隐藏它，删除结束它，超时到期或可见性设为零让它回来",
  "text": "以文字阅读本图",
  "stored": ["已存储", "下一次接收可见"],
  "inFlight": ["传输中", "在可见性超时内不可见，", "带接收句柄，接收计数 + 1"],
  "deleted": ["DeleteMessage：", "消失"],
  "lapsed": ["超时", "到期"],
  "zero": ["ChangeMessage", "Visibility(0)"],
  "again": ["再次可见", "下一次接收再计数一次"],
  "deadLetter": ["已接收 maxReceiveCount 次：", "移入死信队列"],
  "extend": ["ChangeMessageVisibility(n) 再隐藏 n 秒，", "最晚到接收后 12 小时"],
  "receive": "ReceiveMessage",
  "pastCap": "在下一次接收时"
};

# 可见性

收到的 SQS 消息留在队列中，在可见性超时内不可见，直到删除结束它，或者它回来等待下一次接收。

<Diagram name="visibility" words={words} />

接收把消息连同一个接收句柄交出，并在可见性超时内对其他所有接收隐藏它。除非接收指定了超时，否则使用队列自己的超时：默认 30 秒，最长 12 小时。消费者用这次接收的接收句柄删除消息。

没有及时删除的消息会再次可见，并被再投递一次。`ChangeMessageVisibility` 设置一个新的超时，从调用时开始计算。超时为零时，消息立即可见。如果新超时的结束时间晚于接收后 12 小时，调用会被拒绝。

每次接收都让消息的 `ApproximateReceiveCount` 加一。带重新驱动策略的队列指定一个死信队列和一个 1 到 1000 之间的 `maxReceiveCount`。消息被接收这么多次仍未删除时，下一次接收不再投递它，而是把它移入死信队列。死信队列与源队列类型相同：FIFO 队列对应 FIFO 死信队列。

SQS 没有可续期的租约。处理器持有投递期间，本 crate 每隔超时的一半调用一次 `ChangeMessageVisibility`，最多每秒一次。因此，处理较慢的处理器不会遇到同一条消息的第二次投递与它竞争。投递结算或被丢弃时，延长随之停止。超时为零的队列不做延长。

这一行为就是 Amazon SQS 自己的行为（[可见性超时](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)、[`ChangeMessageVisibility`](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html)、[死信队列](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)）。
