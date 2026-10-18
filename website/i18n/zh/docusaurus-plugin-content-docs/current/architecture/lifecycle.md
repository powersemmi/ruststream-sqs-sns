---
title: 生命周期
sidebar_position: 3
description: "SqsBroker::new 只记录配置，connect 加载 AWS 配置，启动时为每个队列打开一个拉取任务，关闭时关闭连接。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的各个状态作为类型，以及每次转换在 AWS 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "aws": "在 AWS 上",
  "unconnected": ["SqsBroker::new()"],
  "connected": ["ConnectedSqsBroker"],
  "closed": ["Closed = ()", "无可报告"],
  "config": ["只记录配置，", "无 I/O"],
  "load": ["加载 AWS 配置，", "每次尝试 25 秒，", "构建 SDK 客户端"],
  "subscriptions": ["每个订阅：", "GetQueueUrl 或", "CreateQueue、重新驱动", "策略、可见性，", "启动拉取任务"],
  "publish": ["发布时解析一次", "它的队列或主题，", "然后缓存"],
  "flag": ["连接", "标为已关闭"],
  "afterShutdown": ["比连接活得更久的发布者", "返回 SqsError::NotConnected，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new) 只记录配置，connect 加载 AWS 配置，启动时为每个队列打开一个拉取任务，关闭时关闭连接。

<Diagram name="lifecycle" words={words} />

`new`、`endpoint`、`region`、`test_credentials` 和 `from_config` 都不做 I/O，所以服务的构建函数保持同步。Broker 的克隆与它共用连接，启动钩子正是通过克隆拿到运行时打开的连接。运行时在应用启动时连接 Broker。

connect 通过 SDK 的默认链加载 AWS 配置，并使用 Broker 指定的端点和区域，或者直接采用传给 `from_config` 的 `SdkConfig`。它把每次尝试的超时设为 25 秒，比 20 秒的长轮询更长，然后构建 SDK 客户端。SDK 延迟获取凭证，直到第一次请求。在服务中，这次请求就是第一个订阅的 `GetQueueUrl`，所以凭证有误时，启动在订阅这一步失败。

每个订阅的 [`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) 首先被检查：名称为空、等待时间超过 20 秒，或者可见性不在一秒到 12 小时之间，都会在调用 AWS 之前返回 [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue)。然后订阅解析队列 URL，在 `create_if_missing` 下则创建队列。注册声明了 `max_attempts` 和 `dead_letter` 时，它写入重新驱动策略。描述符没有指定可见性超时时，它读取队列自己的超时。最后它启动拉取任务。

接收错误作为一个条目到达订阅的流。队列不再存在时，拉取任务结束；其他错误则让它等待一秒，再进行下一次接收。

关闭把连接标为已关闭，并返回 `()`：SDK 客户端没有需要关闭或刷出的东西。关闭之前创建的发布者此后返回 [`SqsError::NotConnected`](api:ruststream_sqs_sns::SqsError::NotConnected)。
