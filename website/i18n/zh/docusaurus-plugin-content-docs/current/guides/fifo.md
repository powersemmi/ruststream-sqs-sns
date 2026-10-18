---
title: FIFO 队列
sidebar_position: 5
description: "按消息组向 SQS FIFO 队列发送，用重复数据删除 id 丢弃重复的发送，并按顺序同时处理多个组。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FIFO 队列

本指南按消息组向 SQS FIFO 队列发送，用重复数据删除 id 丢弃重复的发送，并同时处理多个组，每个组保持顺序。

[FIFO 队列](../components/subscriptions/fifo.md)在一个消息组内保持消息的顺序，只有前一条消息被删除后，才交出该组的下一条消息。
每次发送还带有一个重复数据删除 id，队列在五分钟内丢弃带相同 id 的第二次发送（[AWS：重复数据删除](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)）。
名称以 `.fifo` 结尾的队列就是 FIFO 队列。

示例是一个发货服务。
它把每个订单变成一次发货，同一客户的发货按客户下单的顺序发出。

## 在组中发送 {/* #sending-in-a-group */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#dispatch"
```

</TabItem>
</Tabs>

`dispatch` 通过 `Shipments` 槽把一次发货发布到 `shipments.fifo`。
这个槽以 [`SqsPublishOptions`](../components/publishing/options.md) 为约束，所以发布可以使用 [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps) 的步骤。
使用这些步骤的处理器要导入本 crate 的 prelude。

[`group_id`](../components/publishing/group-id.md) 把这次发货放进客户的组。
发布没有指定组时，组取自消息的 [`PARTITION_KEY_HEADER`](../components/delivery/partition-key.md)，其次取自策略用 `Publish::default().group_id(..)` 固定的组，最后是 `"default"`。
这个消息头是每个 RustStream Broker 都读取的写法，所以设置它的生产者不需要任何 SQS 专用步骤。

[`deduplication_id`](../components/publishing/deduplication-id.md) 是订单的 id。
`orders` 把一个订单投递两次时，第二次发货就是一次被队列丢弃的发送。
没有指定重复数据删除 id 的发布会得到一个在本进程内唯一的 id，因此有意发送的两个相同载荷都会到达。
策略不携带重复数据删除 id：一个固定的 id 会让第一次之后的每次发送都被丢弃。

在标准队列上设置组或重复数据删除 id 会被拒绝，返回 [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo)。

## 同时处理多个组 {/* #several-groups-at-once */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#ship"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#ship"
```

</TabItem>
</Tabs>

来自 FIFO 队列的投递在 `PARTITION_KEY_HEADER` 中携带它的组。
`workers(4, by_key)` 运行四个工作分区，并把一个组的每条消息都送到同一个工作分区。
所以四个客户同时发货，而同一客户的发货一个接一个地发出。
一次接收可能返回一个组的多条消息，普通的 `workers(4)` 会让它们并行运行。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#app"
```

</TabItem>
</Tabs>

[`create_if_missing`](api:ruststream_sqs_sns::SqsQueue::create_if_missing) 把 `shipments.fifo` 创建为启用基于内容的重复数据删除的 FIFO 队列。
FIFO 队列对应 FIFO 死信队列；[重试与死信队列](retries.md)介绍次数上限。
SQS 限制单个组的吞吐量，所以服务把负载分散到许多组上。

示例文件中的测试检查一次发货所在的组，把一个订单发送两次，并同时发送两个客户的订单。
[测试 SQS 服务](testing.md)介绍测试工具。
