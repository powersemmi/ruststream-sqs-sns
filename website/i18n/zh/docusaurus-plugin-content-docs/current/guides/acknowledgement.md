---
title: 确认与投递保证
sidebar_position: 3
description: "处理器的每种结果对 SQS 消息做什么，消息到达两次时服务如何保持正确，以及批如何结算。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 确认与投递保证

本指南说明处理器的每种结果对 SQS 消息做什么，消息到达两次时处理器如何保持正确，以及批如何结算。

SQS 至少投递一次。
收到的消息留在队列中，保持不可见，直到消费者删除它。
标准队列还可能把一条消息投递两次，顺序也可能与发送顺序不同（[AWS：至少一次投递](https://docs.aws.amazon.com/zh_cn/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html)）。

示例是一个付款服务，它把付款记入账本，并支付退款。

## 每种结果做什么 {/* #what-each-outcome-does */}

每种[结果](../components/settlement/index.md)都是对处理器收到的那条消息的一次 SQS 调用：

| 结果 | SQS 调用 | 消息会怎样 |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | `DeleteMessage` | 离开队列 |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | `DeleteMessage` | 离开队列：SQS 没有拒绝消息又保留它的调用 |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | `ChangeMessageVisibility` 设为零 | 立即再次交出 |
| [`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) | `ChangeMessageVisibility` 设为延迟时间 | 延迟结束时再次交出 |

处理器从未结算的消息不会产生调用，比如进程在中途停止时。
它在可见性超时结束时回来。
所以处理器的工作发生在结果之前，消息只在工作完成之后才删除。

## 只记账一次的处理器 {/* #a-handler-that-books-once */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#book"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#book"
```

</TabItem>
</Tabs>

`book` 为一笔付款写入一条账本记录，然后确认它。
SQS 第二次投递的付款会在已记账的 id 中被找到，于是直接确认，不再写第二条记录。
应用状态中的集合代替了服务保存已记账 id 的存储。这里的 id 是付款自己的 id，而不是 SQS 消息 id，因为发送两次的生产者发出的是两条消息。
记录无法发布时，什么也不记账，`retry` 把付款交还，再试一次。

处理器读取 [`RECEIVE_COUNT_HEADER`](../components/delivery/receive-count.md)，即队列自己的 `ApproximateReceiveCount`。
它也计入本次接收，所以第一次投递读到 1。
审核中账户的付款用 `retry_after` 等待一分钟，到第三次接收时，处理器放弃并丢弃它。
[重试与死信队列](retries.md)展示队列本身如何限制接收次数并保留失败的消息。

## 批逐条消息结算 {/* #a-batch-settles-message-by-message */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#refunds"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#refunds"
```

</TabItem>
</Tabs>

批处理器为每条消息返回一个结果，顺序与切片相同。
每个结果都是对其消息的一次单独调用。
这里小额退款被删除，大额退款留在队列中，五分钟后单独回来。
只返回一个结果的批处理器用它结算批中的每条消息。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

`book` 通过 `Entries` 槽发布它的记录，这个槽在挂载处绑定到队列发布者。
FIFO 队列在五分钟内丢弃带相同重复数据删除 id 的第二次发送，这能防止生产者把一条消息发送两次；[FIFO 队列](fifo.md)展示了这一点。
标准队列上的处理器仍然要自己记录做过的事。

示例文件中的测试把一笔付款投递两次，在暂停的时钟上让审核中的付款用尽接收次数，并在一批中发送三笔退款。
[测试 SQS 服务](testing.md)介绍测试工具。
