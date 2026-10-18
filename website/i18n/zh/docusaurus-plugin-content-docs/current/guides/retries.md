---
title: 重试与死信队列
sidebar_position: 4
description: "立即或稍后重试 SQS 消息，限制它的投递次数，让 SQS 把反复失败的消息移入死信队列。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信队列

本指南立即或稍后重试 SQS 消息，限制它的投递次数，并让 SQS 把反复失败的消息移入死信队列。

这两件事都由 SQS 自己完成。
重试就是让同一条消息再次可见，它的接收计数保持不变。
次数上限和死信队列构成队列的重新驱动策略，由挂载处声明。

示例是一个发票服务，它向客户收取发票款项。

## 立即重试与稍后重试 {/* #retry-now-and-retry-later */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

超出信用额度的发票要等待信用检查。
[`retry_after`](../components/settlement/retry-after.md) 把消息的可见性超时设为 30 秒，这段时间结束时，SQS 再次交出它。
SQS 按整秒计数，所以不足一秒的部分向上取整。
SQS 从接收起最多隐藏消息十二小时，所以延迟在这十二小时内结束。
更长的延迟会缩短到这十二小时结束为止，本 crate 同时记录一条注明队列名称的警告日志。
消息始终留在它的队列中，所以延迟的重试在服务重启后依然有效。

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 把可见性设为零，消息立即再次交出。
没有客户的发票每次尝试都得到同样的答案，所以 `charge` 丢弃它。

## 次数上限与死信队列 {/* #the-cap-and-the-dead-letter-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 与 [`dead_letter`](../components/settlement/dead-letters.md) 合起来就是队列的重新驱动策略。
次数上限成为它的 `maxReceiveCount`，死信队列成为它的目标。
订阅打开时把这个策略写到队列上。
从那以后，由 SQS 统计接收次数，并移走用尽次数的消息；服务不发布任何东西。
这里一张发票被投递四次，第五次接收时，SQS 把它移入 `invoices-dead`。

这两个步骤要一起使用。
只有次数上限而没有死信队列，或者只有死信队列而没有次数上限，服务都会在启动时以 [`SqsError::IncompleteRedrive`](../components/broker/errors.md) 停止，错误会指出缺少的那一半。
SQS 接受的 `maxReceiveCount` 最大为 1000。

订阅打开时，死信队列必须已经存在，除非应用的某个描述符用 `create_if_missing` 创建它。
它必须与源队列类型相同：FIFO 队列对应 FIFO 死信队列。
写入策略需要源队列上的 `sqs:SetQueueAttributes` 权限，以及死信队列上的 `sqs:GetQueueAttributes` 权限。

`drop` 删除消息。
在设有次数上限的队列上，如果一次投递用掉了最后一次接收，丢弃它会改为交还消息，于是 SQS 把它移入死信队列，而不是让它丢失。

## 读取死信队列 {/* #reading-the-dead-letter-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

死信队列是一个普通队列，`review` 像读取其他队列一样读取它。
消息按发送时的样子到达，带着它的属性。
没有读取者的队列一直保存消息，直到保留期结束，所以也可以由人在控制台中查看它们。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试在进程内用暂停的时钟运行应用，所以 `tb.advance(CREDIT_CHECK)` 让 30 秒一下子过去。
一个测试让一张发票经过四次投递进入 `invoices-dead`，另一个测试检查只有次数上限而没有死信队列的应用无法启动。
[测试 SQS 服务](testing.md)介绍测试工具。
