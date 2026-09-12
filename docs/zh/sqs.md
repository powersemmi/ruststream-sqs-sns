# Amazon SQS

`ruststream-sqs-sns` 让 RustStream 服务跑在 Amazon SQS 上，并支持 SNS 扇出发布。SQS 是一个队列：
一次投递或者删除掉，或者退回队列，什么时候回来由可见性超时决定。这个 crate 覆盖长轮询、基于可见性
的重试和 FIFO 消息组，并在 `testing` feature 下提供一个进程内的测试 Broker。框架概念（怎么写订阅
者、路由、编解码器和中间件）见 [RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

服务文件只导入 `ruststream_sqs_sns::prelude::*`，两个 crate 里别的都不导入：这个 glob 把框架自己的
prelude，连同本 crate 的 Broker、队列描述符、发布策略和发布者一起重新导出。框架的条目原样通过，
因此跑在两个 Broker 上的服务把两个 prelude 都 glob 进来，两边共有的部分解析到同一个条目。

本 crate 的 MSRV 是 1.94，跟随 AWS SDK；框架核心停在 1.88，依赖方 crate 可以高于它所依赖的那个
下限。

## 能力 { #capabilities }

框架的可选能力，以及这个 Broker 原生实现了其中哪些。Broker 没有实现的能力，在挂载点就是一个编译
错误。

| 能力 | 原生 | 原因 |
| --- | --- | --- |
| `Subscribe` | 是 | 有队列名就够：`#[subscriber("orders")]` 不用描述符也能绑定 |
| `BatchSubscriber` | 是 | `ReceiveMessage` 本来就是批量调用：一次接收就是一个批次（见[批次](#batches)） |
| `TransactionalPublisher` | 否 | SQS 没有事务性发送 |
| `OwnedTransactions` | 否 | SQS 没有事务性发送 |
| `RequestReply` | 否 | SQS 没有接收回复的信箱；回复就是往另一个队列的普通发送 |
| `Partitioned` | 是 | 一次投递把自己的 FIFO 消息组 ID 放在 `partition-key` 消息头里报出来，一次发布也从这个消息头读自己的组（见 [FIFO 消息组](#fifo-message-groups)） |
| `Seekable` / `Positioned` | 否 | 队列没有可以移动的游标；投递次数用尽的消息，从 redrive 策略指定的死信队列里取回 |
| `DescribeServer` | 是 | `SqsBroker` 把自己连接的主机和端口、以及 `sqs` 协议，报进框架生成的 AsyncAPI 文档 |

## 生命周期 { #the-lifecycle }

Broker 是一串消费 `self` 的转换，因此每个状态都是各自独立的类型：

```text
SqsBroker::new()          只有配置，同步，没有 I/O
  .connect()   ->  ConnectedSqsBroker    活跃的 SDK 客户端；订阅和发布者
  .shutdown()  ->  ()                    终结转换
```

运行时在启动时调用一次 `connect`，时间点在第一个订阅打开之前，区域和凭证就在那里从环境解析
（profile、IMDS 和 SSO）。I/O 不放进 `new`，SQS 服务才能和别的 Broker 一样，用同一个
`#[ruststream::app]` 宏组装起来。`shutdown` 消费已连接的 Broker，因此在它之后发布或订阅无法通过
编译。它还把共享的客户端状态标记为已关闭，于是早先交出去的发布者返回 `SqsError::NotConnected`，
而不是在应用已经放弃的连接上照常成功。

配置写在同步的构建器上：

- `SqsBroker::new()` 一切都从环境解析。
- `SqsBroker::from_config(config)` 接受一份已经建好的 `aws_config::SdkConfig`；服务也用这个办法
  固定 SDK 的行为版本，而不用最新的那个。
- `endpoint(url)`、`region(name)` 和 `test_credentials()` 把 Broker 指向本地服务栈。SQS 返回的
  队列 URL 会重新落到配置的 endpoint 上，因此本地服务栈惯用的那些主机改写手法，对适配器没有影响。

`connect` 还在它构建的 SDK 配置上，把单次尝试的超时设成 25 秒，高于一次长轮询可能等待的 20 秒。

## 队列描述符 { #queue-descriptors }

`SqsQueue` 描述一个队列订阅。决定成本和延迟的参数，都明确写在它上面：

| 方法 | 含义 | 默认值 |
| --- | --- | --- |
| `wait(Duration)` | 每次接收调用的长轮询等待时间，上限是协议规定的 20 秒 | 20 秒 |
| `visibility(Duration)` | 每次接收所请求的可见性超时，范围在 `1s..=12h` 之内 | 队列自己配置的超时 |
| `create_if_missing()` | 订阅时队列不存在就创建它 | 关闭 |

批次大小不在其中。它属于注册：批量处理器在挂载点用 `batch(n)` 写出它（见[批次](#batches)）。

描述符在任何 I/O 之前就校验。空名字、超过上限的等待时间，或者超出范围的可见性，都在订阅时返回
`SqsError::InvalidQueue`，不会调用 AWS。

描述符直接写在 `#[subscriber(..)]` 宏里，用 URL 或者名字指出队列；名字通过 `GetQueueUrl` 解析，
并且会缓存。示例里加了 `create_if_missing()`，因为它们跑在什么都没预置的本地服务栈上；生产服务
不写这一项：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:handler"
```

把它挂到 Broker 上：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

纯字符串写法 `#[subscriber("orders")]` 采用描述符的默认值。`#[subscriber(SqsQueue)]` 固定种类，
把名字留给挂载点，也就是 `b.include(handler.name("orders"))`；一份处理器定义要服务两个队列，
就是这么写。

服务使用的队列名，不必是合法的 SQS 名字：去往 SQS 的路上，`[A-Za-z0-9_-]` 之外的每个字符都变成
`-`，`.fifo` 后缀保留下来。订阅和向队列的发布共用这套映射，因此带点的框架名字在队列上仍然可以
路由。

同样这些选项在挂载点也够得着，用本 crate 自己的词汇，通过 prelude 里的 `SqsSubscription` trait。
先写出批次大小的注册，接着就在那里写它们，因为链上先来的是框架自己的步骤：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_batches.rs:mount"
```

`create_if_missing` 需要创建的队列是普通队列；名字以 `.fifo` 结尾时，创建的是开启了基于内容去重的
FIFO 队列。生产队列通常作为基础设施来管理。

一个订阅是一条流，由后台任务用长轮询调用 `ReceiveMessage` 来填充。它的通道只装一个批次，因此这个
任务最多只领先处理器一次接收。丢弃这条流就停掉任务；它已经投递出去、又没有结算的消息，会在各自的
可见性到期后重新投递。接收错误作为流的元素出现：队列不存在会结束这条流，其他任何错误都退避一秒，
因此持续的错误不会让循环空转。

## 批次 { #batches }

这里的批次是传输自己的批次。批量处理器在挂载点写出一个大小，这个大小成为 `MaxNumberOfMessages`，
一次 `ReceiveMessage` 调用就是一个批次。客户端不做缓冲。

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_batches.rs:handler"
```

大小写在挂载链的前面，队列选项链接在它后面，像上面的示例那样。队列里只有这么多时，回来的批次比那个
大小短。它绝不会是空的：超时的长轮询根本不产生批次。

`ReceiveMessage` 每次调用最多返回十条消息，因此更大的尺寸会夹到十，并且这次夹取会为该订阅记录一次
日志。

单条消息的处理器没有批次。它的订阅每次接收仍然按协议上限来要，因为 SQS 按请求计费而不是按消息
计费，然后把消息逐条交给处理器。处理器同时处理多少条，由 `workers(n)` 决定，那是框架的设置。

## 结算与延迟重试 { #settlement-and-deferred-retry }

每一个结算动作都对应一个原生的 SQS 操作：

| 处理器结果 | SQS 操作 |
| --- | --- |
| `HandlerOutcome::ack()` | `DeleteMessage` |
| `HandlerOutcome::retry()` | `ChangeMessageVisibility` 设为 0，消息立即重新投递 |
| `HandlerOutcome::retry_after(delay)` | `ChangeMessageVisibility` 设为该延迟 |
| `HandlerOutcome::drop()` | `DeleteMessage` |

延迟重试同样是原生的：`retry_after(delay)` 把消息的可见性设成这个延迟，上限是协议规定的 12 小时。
消息原地等待，在同一个队列上重新投递，接收计数保持不变，因为什么都没有重新发布，也没有制作副本。

框架自己那条重新发布延迟副本的退路，因此在这里从不执行。它仍然接好了：问到延迟副本该去哪里时，
队列用自己的名字作答，因此用 `BrokerScope::retry_via` 搭起来的作用域在这个 Broker 上能启动，
而不是拒绝启动。从 SNS 主题接收消息的队列，答案也一样：副本直接到达队列，跳过扇出。那一个订阅的
重新投递，本来就是这个意思。

SQS 除了删除之外没有别的丢弃办法，因此毒消息的路由归队列的 redrive 策略管：投递达到
`maxReceiveCount` 次之后，SQS 自己把消息移进死信队列。处理器在 `sqs-receive-count` 消息头
（`RECEIVE_COUNT_HEADER`）里读到这个计数，也就是 SQS 报出的近似接收次数，于是可以把最后一次尝试
和第一次区别对待。

## 可见性续期 { #the-visibility-extender }

处理器可能跑得比消息的可见性超时还久，而 SQS 没有续租的调用。一次投递还活着的时候，这个 crate
每过半个周期就重新拉起它的可见性，队列因此不会在第一个 worker 还在处理时，把同一条消息交给第二个。
它重新拉起的是这次投递所依据的那个值：描述符写出的那个，或者描述符什么都没写时队列自己的可见性
超时。那个超时在订阅打开时从队列读一次，因此没有写出自己数值的订阅，需要在它的队列上有
`sqs:GetQueueAttributes` 权限，拿不到就不打开。消息一旦结算或者丢弃，续期立刻停止。续期失败会以
debug 级别记录日志，并在下一拍重试。处理器能跑多久由进程决定，不由队列的超时决定。

丢弃一次没有结算的投递，不会再产生任何调用：消息在当前可见性到期后重新投递，这就是至少一次的契约。

## FIFO 消息组 { #fifo-message-groups }

目的地以 `.fifo` 结尾时，每一次发送都带一个消息组 ID，因为 FIFO 队列要求它。用
[`group_id`](#per-message-settings) 步骤逐条指定，或者在策略上为整个发布位置固定一个：
`Publish::default().group_id("orders")`。一次投递到达时，它的组在 `partition-key` 消息头里；消息
也可以在同一个消息头里写出自己的组，这种写法原样通用于其他每一个 Broker。什么都没写的发送，走
`"default"`。

三个答案可能同时在场，解析顺序从最具体的开始：本次调用自己的步骤，然后是消息的 `partition-key`
消息头，再然后是挂载点固定的那个组。

每一次 FIFO 发送还会补上一个进程内唯一的去重 ID，除非调用用 `deduplication_id` 步骤写出自己的。
显式的 ID 优先于基于内容的去重，因此故意发出的两份相同载荷，绝不会塌缩成一条。

这两项都是 FIFO 设置。为普通队列或者普通主题写出其中任何一项，都是一个发布错误
（`SqsError::NotFifo`）：调用方要的顺序在那里不会发生，而默默丢掉一个值是更差的答案。
`partition-key` 消息头不是对这个 Broker 的要求，因此普通队列照旧忽略它。

## 发布 { #publishing }

发布策略构造出活跃的发布者，运行时在启动时把它实例化在已连接的 Broker 上。写出哪个策略，就选定了
目的地的种类：

- `SqsPublish` 是构造 `SqsPublisher` 的策略：它直接发往队列，用 URL 或者名字指出。它同时是
  Broker 的默认发布策略，因此挂载时没写自己策略的回复处理器，就走它发送。
- `SnsPublish` 构造 `SnsPublisher`：它把一条通知发布到 SNS 主题，用 ARN 或者名字指出。主题名字
  原样到达幂等的 `CreateTopic`，因此主题用 SNS 自己接受的名字来寻址。

回复类型声明回复发往何处：它上面的 `#[outgoing(name = "..")]` 就是目的地。没有声明的回复类型，
从处理器的 `publish("..")` 子句取目的地。

挂载点写出由谁送达。`.out(Reply, policy)` 绑定它：`Reply` 是回复处理器返回值的标记，调用之后的
那些步骤（`.codec(..)` 和 `.transform(..)`）作用于它指出的那个位置。没写策略的挂载保留
`SqsPublish`，因此队列到队列的服务写 `b.include(handler)` 就够了。把同一个回复改发到主题，
是链上的一步：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:reply"
```

绑定它的那次挂载在 [SNS 扇出](#sns-fan-out)。同一个策略值也交给生命周期钩子，
`b.after_startup(Publish::default(), ..)`，服务在处理器之外发布，就从那里发。

prelude 还把 `SqsPublish` 以 `Publish` 之名导出，那是每个 Broker crate 给挂载点和生命周期钩子
所接受的那个策略起的名字；示例里写的就是它。`SnsPublish` 保留自己的名字：扇出是偏离默认的那一项，
不是默认本身。两者都仍然以带前缀的名字可用，供混用两者的文件使用。

发布者也可以从 Broker 本身取得：应用启动之前用 `SqsBroker::publisher()`，从已连接形态用
`ConnectedSqsBroker::publisher()` 和 `ConnectedSqsBroker::sns_publisher()`。它们各自共享 Broker
的连接，`shutdown` 之后的每一次发布都返回 `SqsError::NotConnected`。

### 逐条消息的设置 { #per-message-settings }

一条消息和下一条可以有哪些不同，写在 `SqsPublishOptions` 里：一个消息组 ID 和一个去重 ID，两个都
可选。发布构建器把它们当作步骤接受，来自 prelude 导出的 `SqsPublishSteps` trait：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_fifo_group.rs:publish"
```

一个步骤是构建器上的一个位置，不是发布者外面的一层包装，因此它完成的这次发布，仍然用挂载点指定的
编解码器编码，并运行那次挂载指定的变换。同样这些步骤在每个发布面上都有：注入的 `Out` 槽位、交给
生命周期钩子的发布者，以及从 Broker 取来的发布者。

写出某个步骤的处理器函数体，是处理器文件唯一一处导入本 crate 的 prelude 而不是框架 prelude 的
地方，并且它按选项类型约束自己发布所用的那个槽位：

<!-- inline-rust: the signature alone is the point here; a compiled snippet would drag in the slot marker, the payload type and a body that says nothing -->
```rust
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = SqsPublishOptions>, Shipments>,
) -> HandlerOutcome
```

调用没有动的部分，就是挂载点固定的部分：`.out(Shipments, Publish::default().group_id("orders"))`。
回复什么也不调整，因为它没有调用点：绑定到 `Reply` 位置的那个策略，就是它的全部答案。

出站路上，组变成原生的 `MessageGroupId`，而不是一条消息属性，投递回来时把它放在 `partition-key`
消息头里带回。

## SNS 扇出 { #sns-fan-out }

SNS 只以发布者的身份出现：它的投递目标是队列和 HTTP 端点，不是本 crate 会拥有的消费方。向主题
发布一次，就到达每一个订阅了它的队列，而每个队列由一个普通的 `SqsQueue` 订阅来消费。

`subscribe_queue_to_topic` 用原始消息投递把队列挂到主题上，因此载荷和消息头以普通 SQS 消息的形态
原样到达，不裹在 SNS 信封里：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:wiring"
```

拓扑管理走 Broker 自己那串生命周期转换，而不经过应用构建器；生产环境里，主题和它的订阅作为基础
设施预置。示例从 `after_startup` 钩子把它们接起来，那时队列已经存在，因为订阅把它们打开了；接着
往队列里放一个订单，让处理器的回复扇出去。`.out(Reply, SnsPublish::default())` 就是扇出接线的
全部：

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sns_fanout.rs:app"
```

## 载荷与消息头 { #payloads-and-headers }

每个消息头变成一条 SQS 消息属性：SQS 当作文本接受的值用 `String`，其余的用 `Binary`。没有发明
任何信封格式，因此其他任何 SQS 的生产方或消费方都能读同一条消息。消息头的值在框架 `HeaderMap` 的
两侧都是字节，因此是哪一种属性类型承载了它，对服务不可见。

消息体是传输上唯一的约束。SQS 的消息体是文本，而那里算作文本的范围比 UTF-8 更窄：除制表符、
换行符和回车符之外的 C0 控制字符不在其列，基本平面末尾的两个非字符也不在其列。SQS 接受的载荷原样
通过。其余的一切都以 base64 编码，并带上一个标记属性，接收时再解码回来：二进制载荷是这样，带着
那些字符的合法 UTF-8 也是这样，而后者正是二进制编解码器常常产出的东西。同一条规则也为消息头的值
挑选属性类型，因此一次发布绝不会因为 SQS 不接受的字节而遭拒。

处理器也可以自己解析消息体，用于由本框架之外的生产方供给的队列，或者没有 `serde` 模型的传输格式：
`&[u8]` 之上带 `#[derive(Deserialized)]` 的 newtype 走框架的字节路径，整条路上没有编解码器。
base64 那一步到这时已经撤销，因此处理器看到的就是生产方发出的字节。

## 用 LocalStack 做本地开发 { #local-development-with-localstack }

仓库里带了一个 LocalStack 的 compose 文件，其中有 `sqs` 和 `sns` 两个服务，还有围绕它的那些
`just` 配方：

```bash
just brokers-up                 # 在 127.0.0.1:4566 上启动 LocalStack
cargo run --example sqs_service
cargo run --example sqs_batches
cargo run --example sns_fanout
just brokers-down
```

用 `endpoint`、`test_credentials` 和明确的 `region` 把 Broker 指向这个服务栈，示例就是这么做的。
设置了 `SQS_TEST_ENDPOINT` 时，真实环境的测试套件运行；没设置时跳过：

```bash
just test-brokers               # 起 LocalStack，跑集成测试和 conformance，再关掉 LocalStack
```

或者，针对已经在跑的服务栈：

```bash
SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --workspace --all-features -- --test-threads=1
```

每次改动，CI 都对着 LocalStack 跑同一套测试。

这个 crate 的能力所证成的每一套 conformance，都跑两遍：一遍对着进程内传输，一遍对着服务栈。
生命周期的那串转换和 `capabilities::batches` 两条腿都有；路由套件只在进程内跑。`request_reply`、
`transactions`、`owned_transactions` 和 `seeking` 在这里一条腿都没有：SQS 不提供请求-回复通道，
没有事务，也没有可以定位的游标，因此这个 crate 这几项能力一个都不实现，它们的套件也就不适用。

## 测试 { #testing }

`testing` feature 提供 `SqsTestBroker`：一个进程内的 Broker，不用服务器也不用网络就复现本 crate
的核心路由，并且和真实的那个走同一串转换，拆除也算在内：那里一个曾共享连接的发布者会报出
`NotConnected`，而不是路由进一个已经清空的路由器。它住在本 crate 的 `testing` 模块里，测试文件
按名字导入：`use ruststream_sqs_sns::testing::SqsTestBroker;`。
把应用建在它上面，`TestApp` 测试套件就会驱动你真实的处理器、编解码器和中间件：发布一个输入，
然后对处理器收到了什么、又向下游发布了什么做断言。参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

服务交付的那份声明，在那里原样挂载。`SqsQueue` 对进程内传输来说同样是订阅来源，因此
`#[subscriber(SqsQueue::new("orders").wait(..))]` 以及链在它后面的挂载点设置，跑的就是服务真正跑
的那套接线。选项到描述符为止：进程内没有长轮询给 `wait` 去限制，没有重新投递的时钟给 `visibility`
去拉起，也没有队列给 `create_if_missing` 去创建。

发布这一半也对得上。`SqsPublish` 和 `SnsPublish` 对着进程内传输同样构造得出发布者，而它把
`SqsPublish` 定为自己的默认策略，因此 `.out(Reply, Publish::default())`，以及带 `publish(..)` 的
处理器的默认回复，都按写法挂上。逐条消息的设置也到得了那里：函数体写出的步骤到达进程内传输的
发布者，和到达真实发布者一样，测试套件会把它记下来
（`tb.out::<Shipments>().assert_called_once().with_options(..)`），解析出的组也会放进
`partition-key` 消息头投递，和 SQS 投递的方式完全一致。两个策略构造出的是同一个
`SqsTestPublisher`，
因为路由器没有可供扇出的主题：一个测试证明的是回复去了策略指出的那个目的地，而不是 SNS 把它继续
投递给了订阅那个主题的队列。

它按队列名精确路由。属于 SQS 自己的那些东西（可见性计时、重新投递、经 redrive 策略进死信队列、
FIFO 顺序和 SNS 扇出），由对着 LocalStack 的真实环境测试套件来回答。

批次是两种传输在内部唯一不同的地方：进程内由框架的客户端缓冲来攒批，而真实的订阅者从
`ReceiveMessage` 取。挂载写出一个大小，两边拿到的批次都不超过那个大小，正是这一点让批量处理器在
进程内也能测。
