---
title: 方法论
description: "ruststream-sqs-sns 如何测量自己：三个循环、在 SQS 端点上的运行、受 Broker 限制的判定计算，以及代码计数。"
---

# 方法论

`ruststream-sqs-sns` 如何测量自己：三个循环、在 SQS 端点上的运行、受 Broker 限制的判定计算，以及代码计数。流程就是框架自己的流程（[RustStream 方法论](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)），本页说明它在 SQS 上是什么样子。

## 三个循环 {/* #the-three-loops */}

一个进程把每个场景跑三遍，作为三个循环，它们只有一处不同：承载消息的是什么。

- **裸客户端。** 直接驱动 `aws-sdk-sqs`：每次调用接收十条消息，逐条解码，逐条删除。循环结算手头这一批时，保持一次接收在途，与本 crate 的拉取任务相同。
- **Broker crate。** 本 crate，上面什么也没有：Broker 建立连接，[`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) 描述符打开一个订阅，一个循环从它的流中取出投递、解码并 ack。
- **RustStream。** 用户写的服务：一个 `#[subscriber]` 处理器及其外围的应用。

其余一切在三个循环中都相同：AWS 配置、25 秒的单次尝试超时、每次接收十条消息并等待 20 秒、每条消息一次 `DeleteMessage`、解码成同一个结构体、正文字节、带四个工作线程的 tokio 运行时，以及二进制程序。

消息体是 512 字节的 JSON。它解码成一个包含两个整数字段的结构体，因此解码不做内存分配，数字反映的是本 crate，而不是 JSON 解析器。

## 运行 {/* #the-run */}

- **每次运行使用新的队列。** 队列在运行前创建、运行后删除，因此一次运行永远看不到之前的运行留下的东西。
- **消费者先接入。** 然后八个发布者填充队列，每条消息一次 `SendMessage`，最多领先消费者 4096 条消息。
- **在三个循环中，计时窗口都从第一次投递开始，到最后一条消息处理完时结束。** 创建队列、构建客户端和打开订阅都在窗口之外。
- **消息条数来自一次探测**，它测量裸循环的速率，因此在任何机器上，测量的运行都至少持续五秒。
- **各循环交替进行**：裸、crate、RustStream，共三轮。每个循环报告它最佳、中位和最差的一轮。

当到端点的往返至少占一次投递耗时的一半时，该行标为 `受 Broker 限制`。这是算出来的。一次探测在自己的客户端上，为端点能应答的最小请求计时，共调用 2000 次。一次投递的开销是它的 `DeleteMessage` 的一次往返，加上承载它所在批的 `ReceiveMessage` 的十分之一。往返时间与这一请求数之积达到每条消息测得时间的一半时，该行就被标记。往返时间与机器信息一起公布，因此这个计算可以核对。

## 代码计数 {/* #the-code-count */}

[开销图](/architecture/benchmarks/)的每个场景都是用户写的服务，基于 [`SqsBroker`](api:ruststream_sqs_sns::SqsBroker)，连接同一个端点，构建时不带 `testing` 特性，并在单线程运行时上启动。另一个线程在测量区间打开之前把队列填满，该线程不计入。

指令数来自 callgrind，内存分配来自 DHAT。一个场景依次在一次投递、1000 次、2000 次上运行。后两次之差是一条消息的稳态开销，单次投递就是冷启动。

计数覆盖服务线程运行的一切：运行时、本 crate，以及 AWS SDK 及其 HTTP 栈。SDK 自己运行的线程不计入，往返中端点那一侧也不计入。同一个二进制程序运行四次，指令数相差不到百分之零点五，因为端点如何应答、套接字的读取如何切分，每次运行都会略有变化。与保存的基线对照时，某个场景的指令数多出百分之二就算失败。内存分配在每次运行中都受每个场景声明的下限约束：观测到的最高计数再加千分之一。

## 自己运行 {/* #running-it-yourself */}

```bash
just bench
```

这条命令按 `docker-compose.test.yml` 启动 LocalStack 测试环境，运行两个对照场景，停止测试环境，并重写 `docs/benchmarks/results.json`。它需要几分钟，并且需要独占机器。它会清空 `RUSTFLAGS`，因此二进制程序不会绑定到构建它的 CPU 上。两条命令都按发布版本的方式构建基准测试，跨 crate 启用完整的链接时优化（fat LTO），因此这些数字描述的正是服务实际使用的代码。

```bash
just bench-code
```

代码计数需要几分钟，并且需要 valgrind。`just bench-code 5000` 让每个场景在五千次投递上测量，而不是一千次。在 `main` 上运行 `just bench-code --save-baseline=main` 记录基线，在分支上运行 `just bench-code --baseline=main` 与它对照。
