---
title: 设计
sidebar_position: 2
description: "ruststream-sqs-sns 的位置：在 RustStream 运行时与 AWS SDK 之间，每个订阅一个拉取任务，每个运行时一套 SDK 客户端。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下：服务、RustStream 运行时、本 crate、AWS SDK 客户端，以及 SQS 和 SNS",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算",
  "crate": "ruststream-sqs-sns",
  "subscriber": ["SqsSubscriber", "每个队列一个拉取任务"],
  "publishers": ["SqsPublisher,", "SnsPublisher"],
  "message": ["SqsMessage", "可见性延长器"],
  "policies": ["SqsPublish,", "SnsPublish 策略"],
  "broker": ["SqsBroker", "配置"],
  "descriptor": ["SqsQueue", "描述符"],
  "sdk": "AWS SDK for Rust",
  "clients": ["aws-sdk-sqs 与 aws-sdk-sns 客户端", "每个发送请求的运行时一套"],
  "aws": "Amazon SQS 与 Amazon SNS",
  "inProcess": ["在 testing 特性下，进程内账户", "取代客户端和 AWS"],
  "mounted": "挂载",
  "polls": "轮询流",
  "https": "HTTPS 请求"
};

# 设计

`ruststream-sqs-sns` 所处的位置：在 RustStream 运行时与 AWS SDK 之间，每个订阅一个拉取任务，每个运行时一套 SDK 客户端。

<Diagram name="design" words={words} />

本 crate 在 [AWS SDK for Rust](https://docs.rs/aws-sdk-sqs/latest/aws_sdk_sqs/) 之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。SQS 特有的一切都在本 crate 里：[`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) 描述符、各种结果对消息做什么，以及应答如何到达队列或主题。本 crate 支持订阅、批和分区键，并在生成的文档中描述它的服务器。向 SNS 发布需要 `sns` 特性。

每个订阅对应一个队列，订阅者通过一个属于自己的拉取任务读取它。拉取任务对队列做长轮询，最多比处理器领先一批。每次投递都启动一个任务，在处理器持有它期间延长它的可见性。拉取任务和延长任务运行在连接 Broker 的那个运行时上，不管流是在哪个线程上打开的。

已连接的 Broker 和所有发布者共用一个连接：SDK 客户端、一个关闭标志，以及队列 URL 和主题 ARN 的缓存。SDK 在发出请求的那个运行时上打开该请求所需的 HTTP 连接，这个连接只在该运行时运行时才会推进。因此，每个运行时都通过自己的客户端发送。Broker 的运行时使用 connect 构建的那套客户端。带有自己运行时的处理器线程在第一次请求时，用同样的配置和凭证构建一套客户端，繁忙的线程永远不会拖住 Broker 的连接。

[`SqsPublish`](api:ruststream_sqs_sns::SqsPublish) 用 `SendMessage` 发送到队列，[`SnsPublish`](api:ruststream_sqs_sns::SnsPublish) 用 `Publish` 发布到主题。发布者接收应答编码所用的缓冲区。SQS 能作为文本接受的正文直接成为请求正文，不做复制。其他载荷以 base64 编码传输，并带一个标记属性，接收时再解码。

## 进程内传输 {/* #in-process-transport */}

在 `testing` 特性下，测试改为在进程内连接 Broker。这时，已连接的 Broker、它的订阅、发布者和投递都与一个存在于测试中的账户通信。不带该特性的构建中没有它的任何痕迹：编译器会检查传输和投递的大小与它们的线上形态完全相同。

这个账户应答本 crate 发出的调用，并拒绝 SQS 会拒绝的调用，返回的错误与线上调用报告的相同。它模拟服务逻辑所依赖的行为：相互竞争的接收者、每次最多接收十条、可见性及其延长、接收计数、重新驱动到死信队列、FIFO 消息组和五分钟的重复数据删除窗口，以及 SNS 向已订阅队列的扇出。

有些行为只属于线上服务。服务指定的每个队列和主题在进程内都存在，发送到无人订阅的队列的消息会被丢弃。没有指定可见性的描述符得到 30 秒，即新队列的默认值。标准队列在进程内保持顺序，而 SQS 不保证这一点，所以测试只在 FIFO 队列上断言顺序。凭证不做检查。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。
