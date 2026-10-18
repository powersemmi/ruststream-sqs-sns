---
title: 测试 SQS 服务
sidebar_position: 7
description: "用 TestApp 在进程内测试 SQS 服务：生产环境的应用、回复、暂停时钟上的延迟，以及在 LocalStack 上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 SQS 服务

测试用 [`TestApp`](../components/testing/index.md) 运行服务自己的应用，连接进程内 SQS 或一个正在运行的环境。

被测服务是一个结账服务。
订单到达 `checkout-orders`，`confirm` 用一条确认回复它，确认发往 `checkout-confirmations`，由 `notify` 读取。
没有金额的订单被丢弃。
缺货的订单用 `retry_after` 等待一小时，然后回来再次检查。
应用由 `service(broker)` 构建，所以测试选择 Broker，而 `main` 使用生产环境的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-sqs-sns` 的 `testing` feature。

## 订单及其确认 {/* #an-order-and-its-confirmation */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`SqsBroker`](api:ruststream_sqs_sns::SqsBroker) 连接到进程内 SQS。
不调用任何端点，也不需要凭证。
测试按生产环境的类型称呼 Broker：`tb.broker::<SqsBroker>()`。
`publish` 在它到达的处理器全部结算之后才返回，包括回复及其读取者。
所以接下来的几行断言的是已完成的状态：`confirm` 如何结算订单，以及 `notify` 收到了什么。
测试主体把已启动的应用作为参数，所以下面的线上测试也运行它。

## 空订单 {/* #an-empty-order */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何确认。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delay"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delay"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一小时的 `RESTOCK` 不花任何时间。
`retry_after` 让消息在延迟期间不可见，与 SQS 相同，时钟越过延迟时，队列再次交出它。
`published` 显示 `checkout-orders` 上只有一个订单：第二次投递是同一条消息，而不是副本。

## 进程内 SQS 模拟了什么 {/* #what-the-in-process-sqs-models */}

进程内 SQS 是测试内部的一个账户，服务指定的每个队列和主题都存在于其中。
它模拟服务逻辑所依赖的行为：

- 队列把每条消息交给它的一个订阅，收到的消息保持不可见，直到被删除或它的可见性超时结束。
- 接收计数随每次接收增长，重新驱动策略把用尽接收次数的消息移入死信队列。
- 一批最多包含一次接收返回的十条消息。
- FIFO 组在它的某条消息处于传输中时等待，重复数据删除 id 会被记住五分钟。
- 启用 `sns` 时，主题把一份副本投递给用 `subscribe_queue_to_topic` 订阅的每个队列。

SQS 或 SNS 拒绝的东西，在这里也会被拒绝：超过十个消息头、SQS 不接受的消息头名称、空的正文或消息头值、超过大小上限的消息、超过 80 个字符的队列名称、类型不同的死信队列、超过 1000 的 `maxReceiveCount`，以及订阅标准主题的 FIFO 队列。

标准队列在这里保持消息的顺序，而 SQS 并不保证这一点。
没有指定可见性的描述符得到 30 秒，即新队列的默认值。
发送到无人订阅的队列的消息会被丢弃。
运维人员在队列上设置的可见性、服务期望找到的队列和主题，以及凭证，要在正在运行的环境上检验。

## 在正在运行的环境上运行同一个测试 {/* #the-same-test-on-a-running-stack */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到一个正在运行的 SQS。
测试用 `live::endpoint` 从 `SQS_TEST_ENDPOINT` 读取端点，这是示例自己的辅助函数；变量未设置时，测试跳过。
它先删除服务的队列，因此之前的运行留在上面的东西不会被计入；服务启动时会重新创建它们。
断言与进程内的完全相同。

```bash
cargo test
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
