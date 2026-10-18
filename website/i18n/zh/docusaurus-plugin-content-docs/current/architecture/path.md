---
title: 路径
sidebar_position: 4
description: "一条 SQS 消息在 RustStream 服务中的路径：接收、投递、处理器、应答和删除，逐条或按批进行。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条消息从接收到删除的路径，左边逐条处理并带应答，右边按批处理",
  "text": "以文字阅读本图",
  "single": "逐条",
  "batches": "按批",
  "receive": ["ReceiveMessage", "10 条，等待 20 秒"],
  "channel": ["通道", "容纳一批"],
  "message": ["SqsMessage", "逐条交出"],
  "handler": ["处理器", "解码并运行"],
  "reply": ["应答：SendMessage", "或 SNS Publish"],
  "ack": ["ack", "DeleteMessage"],
  "batchReceive": ["ReceiveMessage", "大小 n，最多 10"],
  "batchChannel": ["通道", "容纳一批"],
  "batchMessages": ["Vec<SqsMessage>", "绝不为空"],
  "batchHandler": ["批处理器", "每批调用一次"],
  "batchAck": ["每个元素", "各自 DeleteMessage"],
  "batch": "一批"
};

# 路径

一条 SQS 消息在 RustStream 服务中的路径：接收、投递、处理器、应答和删除，逐条或按批进行。

<Diagram name="path" words={words} />

拉取任务每次接收请求十条消息，带上所有消息属性和系统属性，最长等待 20 秒。只有描述符指定了可见性超时，它才在请求中指定。以空结果结束的长轮询不产出任何东西，因此批绝不为空。通道容纳一批，因此拉取任务最多比处理器领先一次接收。

每条消息变成一个 [`SqsMessage`](api:ruststream_sqs_sns::SqsMessage)。正文从 SDK 的应答中复制出来，消息属性连同消息组和接收计数一起变成消息头。投递还会启动它的可见性延长器。每条消息因此有几次内存分配，[基准测试](/architecture/benchmarks/)公布了一条消息的开销。

逐条处理消息的订阅把批拆开，按顺序交出其中的消息。批订阅按自己的大小请求，上限为十，它的处理器一次调用接收整批。批中的每条消息仍然各用一次调用删除。

应答编码进分发循环复用的缓冲区。[`SqsPublisher`](api:ruststream_sqs_sns::SqsPublisher) 把这个缓冲区作为一次 `SendMessage` 的正文，[`SnsPublisher`](api:ruststream_sqs_sns::SnsPublisher) 则把它作为一次 `Publish` 的正文。队列 URL 或主题 ARN 只解析一次，然后缓存。

投递在应答之后结算。每次结算都经过结算它的那个运行时的 SDK 客户端，每种结果做什么，见[结算](settlement.md)。
