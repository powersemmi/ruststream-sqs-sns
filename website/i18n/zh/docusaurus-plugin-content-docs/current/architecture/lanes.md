---
title: 工作分区
sidebar_position: 5
description: "一个订阅通过一个拉取任务读取一个队列；workers(n, by_key) 把消息分到按 FIFO 消息组划分的工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：一个 FIFO 队列、一个拉取任务、一个流，以及按消息组划分的两个工作分区",
  "text": "以文字阅读本图",
  "queue": ["队列 orders.fifo", "消息组 alice、bob 和 carol"],
  "pump": ["拉取任务", "比处理器领先一次接收"],
  "stream": ["SqsSubscriber 流", "一次一条消息"],
  "keyed": ["workers(2, by_key)", "工作分区 = 消息组的哈希"],
  "lane0": ["工作分区 1", "alice 和 carol，", "各自保持顺序"],
  "lane1": ["工作分区 2", "bob，保持顺序"],
  "standard": ["标准队列不带消息组，", "因此它的消息轮流分配到各个工作分区"],
  "replicas": ["服务的多个副本轮询同一个队列，", "SQS 把每条消息交给其中一个"],
  "receives": "ReceiveMessage",
  "oneBatch": "一批",
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅通过一个拉取任务读取一个队列；`workers(n, by_key)` 把消息分到按 FIFO 消息组划分的工作分区上。

<Diagram name="lanes" words={words} />

订阅的流一次产出一条消息。不用 `workers` 时，一个循环依次为每条消息运行处理器，没有任何东西并行运行。`workers(n)` 同时运行最多 `n` 个处理器，不保证顺序。

`workers(n, by_key)` 运行 `n` 个工作分区，消息进入其键的哈希所对应的工作分区。键是消息的 [`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER)，其中存放它的 FIFO 消息组。因此一个组绝不会分散到两个工作分区上，[消息组](sqs/fifo.md)一页所说的队列上的顺序在服务中得以保持。工作分区少于组时，每个工作分区服务多个组。标准队列没有组：带着该头发布到标准队列的消息以属性携带它，进入其键对应的工作分区；不带该头的消息轮流分配到各个工作分区。

拉取任务最多比处理器领先一次接收。它已经接收、尚未交出的消息，在此期间由延长器保持不可见。某个组有消息处于传输中时，FIFO 队列还会扣住这个组。因此，在上一条消息结算或其超时到期之前，该组的下一条消息不会进入服务。

服务的多个副本，或者同一队列上的多个订阅，相互竞争队列的消息：SQS 把每条消息交给其中一个。进程内传输会保持标准队列的顺序，而 SQS 不保证这一点，所以断言顺序的测试使用 FIFO 队列。

[批处理器](path.md)每次处理一次接收取回的消息，最多到挂载处指定的大小，且不超过十条，因此一个批往往小于这个大小。
