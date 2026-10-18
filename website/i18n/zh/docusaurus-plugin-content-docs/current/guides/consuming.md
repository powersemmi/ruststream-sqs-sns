---
title: 消费队列
sidebar_position: 2
description: "用长轮询读取 SQS 队列，让慢处理器的消息保持不可见，并行运行处理器，按最多十条的批接收消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 消费队列

本指南读取 SQS 队列：长轮询、慢处理器的可见性超时、并行处理器、批，以及一个处理器读取多个队列。

示例是一个仓库服务。
它打包订单、记账付款，并记录订单的变更。

## 队列及其长轮询 {/* #a-queue-and-its-long-poll */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#pack"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#pack"
```

</TabItem>
</Tabs>

订阅用 `ReceiveMessage` 对它的队列做长轮询。
每次调用最长等待 20 秒，这是 SQS 允许的上限。
[`wait`](../components/subscriptions/wait.md) 设置更短的等待时间。
队列可以用名称或 URL 指定，名称在启动时用 `GetQueueUrl` 解析一次。

逐条处理的订阅每次调用仍然请求最多十条消息，因为 SQS 按请求计费。
它把这些消息逐条交给处理器。
处理器工作时，一个后台任务接收下一批，它最多领先一批。

## 可见性与慢处理器 {/* #visibility-and-a-slow-handler */}

收到的消息在队列的可见性超时内对其他消费者不可见。
如果到那时还没有删除，SQS 会再次交出它。
[`visibility`](../components/subscriptions/visibility.md) 在每次接收时请求自己的超时，范围是一秒到十二小时。
SQS 按整秒计算超时，所以不足一秒的部分向上取整。
`pack` 请求一分钟。
不用这一步时，订阅在打开时读取一次队列自己的超时。

处理器可以运行得比超时更久。
投递存活期间，本 crate 在每个超时过半时延长它的可见性，因此慢处理器的消息不会被交给另一个消费者。
SQS 从接收起最多隐藏消息十二小时。
处理器运行这么久时，本 crate 把可见性最后一次延长到这十二小时结束，并记录一条警告日志。之后消息会交给另一个消费者。

## 并行处理器 {/* #parallel-handlers */}

`workers(4)` 同时运行最多四个 `orders` 的投递。
四个处理器分享每次接收返回的消息。
标准队列没有需要保持的顺序，所以不需要别的设置。
[FIFO 队列](fifo.md)用 `workers(n, by_key)` 让一个消息组保持顺序。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#reconcile"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#reconcile"
```

</TabItem>
</Tabs>

接收切片的处理器就是[批处理器](../components/subscriptions/batches.md)。
挂载处指定的大小成为 `MaxNumberOfMessages`，因此一次接收就是一批。
一批包含队列当时拥有的消息，什么也没取回的长轮询不产出批。
`ReceiveMessage` 最多返回十条消息，所以更大的大小会被降为十，服务为此记录一次日志。
批中的每条消息各自结算；[确认](acknowledgement.md)一页展示了各种结果。

## 一个处理器，多个队列 {/* #one-handler-several-queues */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#audit"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#audit"
```

</TabItem>
</Tabs>

`#[subscriber(SqsQueue)]` 固定来源的种类，队列名称留空。
挂载处用 `.name(..)` 指定名称，每个队列一次。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#app"
```

</TabItem>
</Tabs>

队列的步骤也可以写在挂载处，放在框架自己的步骤之后，由 [`SqsSubscription`](../components/subscriptions/sqs-subscription.md) 提供。
队列名称为空、等待时间超过 20 秒，或者可见性超出范围，都会让服务在启动时以 [`SqsError::InvalidQueue`](../components/broker/errors.md) 停止，这发生在调用 AWS 之前。
名称可以包含 SQS 不接受的字符：`A-Z`、`a-z`、`0-9`、`_` 和 `-` 以外的每个字符都会变成 `-`，`.fifo` 后缀保留不变。

[`create_if_missing`](../components/subscriptions/create-if-missing.md) 创建不存在的队列。
生产环境的服务通常从基础设施获得队列，不用这一步；这时缺失的队列会让服务在启动时停止。

## 投递携带什么 {/* #what-a-delivery-carries */}

每个消息属性都作为[投递](../components/delivery/index.md)的消息头到达，发布时的每个消息头都作为属性发出。
有两个消息头是本 crate 自己的。
[`RECEIVE_COUNT_HEADER`](../components/delivery/receive-count.md) 携带队列对这条消息的接收计数。
[`PARTITION_KEY_HEADER`](../components/delivery/partition-key.md) 携带 FIFO 队列的消息组。
SQS 的[正文](../components/delivery/body.md)是文本，所以二进制载荷以 base64 编码传输，带一个标记属性，到达时已经解码。
Base64 让这样的载荷增大三分之一，而 SQS 会拒绝超过大小上限的消息。

示例文件中的测试在进程内向应用发布订单、付款和变更。
[测试 SQS 服务](testing.md)介绍测试工具。
