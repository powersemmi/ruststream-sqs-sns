---
title: 结算
sidebar_position: 6
description: "ack、retry、retry_after 和 drop 在 SQS 上变成什么：每种结果都是针对队列的一次调用，重试在队列本身中等待。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const settlement = {
  "title": "处理器的每种结果、它发出的 SQS 调用，以及消息的去向",
  "text": "以文字阅读本图",
  "outcome": "结果",
  "call": "SQS 调用",
  "effect": "效果",
  "ackOutcome": "ack",
  "ackCall": "DeleteMessage",
  "ackEffect": "消失",
  "retryOutcome": "retry()",
  "retryCall": ["ChangeMessage", "Visibility(0)"],
  "retryEffect": ["立即可见"],
  "retryAfterOutcome": "retry_after(d)",
  "retryAfterCall": ["ChangeMessage", "Visibility(d)"],
  "retryAfterEffect": ["d 之后回来，", "不晚于接收后", "12 小时"],
  "dropOutcome": "drop",
  "dropCall": ["DeleteMessage；", "达到上限时：", "Visibility(0)"],
  "dropEffect": ["消失，", "或移入", "死信队列"],
  "unsettledOutcome": ["未结算", "就被丢弃"],
  "unsettledCall": ["不发调用，", "延长停止"],
  "unsettledEffect": ["超时到期", "后回来"]
};

export const retries = {
  "title": "SQS 上的重试：消息在队列中等待，由重新驱动策略在下一次接收时决定去向",
  "text": "以文字阅读本图",
  "delivery": ["一次投递", "接收计数表明这是第几次尝试"],
  "outcome": ["处理器返回", "retry_after(d) 或 retry()"],
  "wait": ["ChangeMessageVisibility(d)", "消息在队列中等待"],
  "cap": ["下一次接收：这条消息是否已被", "接收 maxReceiveCount 次？"],
  "again": ["还没有：", "再次投递，", "仍是同一条消息"],
  "deadLetter": ["是：SQS 把它", "移入死信队列"],
  "drop": ["在最后一次允许的接收上 drop，会以可见性 0 交还消息，", "于是 SQS 移走它，而不是丢失它"],
  "crash": ["队列保存着等待中的消息：", "服务崩溃不会丢失任何重试"],
  "handled": "已处理",
  "retries": "一次重试",
  "after": "d 之后",
  "below": "还没有",
  "past": "是"
};

# 结算

`ack`、`retry`、`retry_after` 和 `drop` 在 SQS 上变成什么：每种结果都是针对队列的一次调用，重试在队列本身中等待。

<Diagram name="settlement" words={settlement} />

`retry_after(d)` 以整秒计算延迟并向上取整，因此消息绝不会早于 `d` 回来。超出接收后 12 小时的延迟会缩短到这 12 小时结束为止，本 crate 同时记录一条注明队列名称的警告日志。未结算就被丢弃的投递不发出任何调用：它的延长停止，消息在当前超时到期时回来。

SQS 没有只拒绝而不删除消息的操作，所以 `drop` 会删除消息。注册声明了重新驱动策略的队列是例外。在这种队列上，已被接收 `maxReceiveCount` 次的投递会被交还而不是删除，因为正是下一次接收把它移入死信队列，删除则会让它丢失。

投递的 [`redelivery_count`](api:ruststream::IncomingMessage::redelivery_count) 就是队列的 `ApproximateReceiveCount`，第一次投递算作一次。它统计每一次接收，包括超时到期后的接收，而不仅是服务请求的重试。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

重试从不离开队列。`retry_after(d)` 让消息在 `d` 内不可见，然后它作为同一条消息回来，id 不变，接收计数加一。没有任何东西被重新发布，因此运行时的重新发布回退在这里从不运行，`.out_retry(..)` 在 [`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) 上无法编译。可见性的结束时间晚于接收后 12 小时时，SQS 会拒绝它。

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) 与 [`dead_letter(q)`](api:ruststream::runtime::Mounting::dead_letter) 合起来就是队列的重新驱动策略：`maxReceiveCount` 是 `n`，死信队列是 `q`。订阅打开时把这个策略写到队列上，由 SQS 统计接收次数，并自行移走用尽次数的消息。只设其中一项，启动时就会得到 [`SqsError::IncompleteRedrive`](api:ruststream_sqs_sns::SqsError::IncompleteRedrive)。

队列保存着等待中的消息，所以服务崩溃不会丢失任何重试：消息在可见性结束时回来，交给下一个轮询的消费者。
