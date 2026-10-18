---
sidebar_position: 6
description: "Ответ публикуется в тему SNS один раз, а SNS доставляет его всем подписчикам разом: каждой очереди SQS, подписанной на тему."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Доставка всем подписчикам разом через SNS

Это руководство публикует сообщение в тему SNS один раз, а SNS доставляет копию в каждую очередь SQS, подписанную на эту тему.

Связка SNS и SQS - это способ, которым сервисы AWS доставляют сообщения всем подписчикам разом: одна тема и по очереди на каждого потребителя.
Крейт публикует в темы, а каждую очередь за темой сервис читает обычным образом.
Для публикации в SNS нужна фича `sns`:

```toml title="Cargo.toml"
[dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["sns"] }
```

Пример - магазин.
Он принимает заказ и объявляет о нём в теме, а оплата и доставка читают это объявление каждая из своей очереди.

## Ответ в тему {/* #a-reply-to-a-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#accept"
```

</TabItem>
</Tabs>

`accept` отвечает на каждый заказ сообщением `OrderPlaced`, и тип называет, куда оно уходит: `shop-order-events`.
Очередь это или тема, решает точка монтирования.

## Очереди за темой {/* #the-queues-behind-the-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#consumers"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#consumers"
```

</TabItem>
</Tabs>

`bill` и `ship` - обычные обработчики очередей.
Каждая очередь получает собственную копию каждого объявления, поэтому оплата и доставка работают в своём темпе и обрабатывают сообщения заново независимо друг от друга.

## Подписка очередей {/* #subscribing-the-queues */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#topology"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#topology"
```

</TabItem>
</Tabs>

[`subscribe_queue_to_topic`](../components/sns/subscribe-queue-to-topic.md) подписывает очередь на тему с прямой доставкой сообщений (raw message delivery).
Поэтому копия приходит обычным сообщением SQS: полезная нагрузка - это тело, а каждый заголовок - атрибут сообщения.
Без прямой доставки SNS обернул бы сообщение в собственный JSON-конверт.
Вызов создаёт тему, если её нет, а повторная подписка той же очереди ничего не меняет.

Топология обычно относится к инфраструктуре и создаётся через Terraform или консоль.
Здесь её создаёт хук старта на копии брокера приложения, которая использует соединение, уже открытое приложением.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(SnsPublish::default())` привязывает ответ к [`SnsPublish`](../components/sns/index.md), и ответ становится вызовом SNS `Publish` в тему.
Без этого шага тот же ответ уходит через политику брокера по умолчанию, [`SqsPublish`](api:ruststream_sqs_sns::SqsPublish), и попадает в очередь с именем `shop-order-events`.
`SnsPublish` открывает тему по имени через `CreateTopic`, который возвращает существующую тему, если она есть, или принимает ARN темы в качестве имени.
Та же политика работает в слоте `Out` и в хуке старта, а [`ConnectedSqsBroker::sns_publisher`](api:ruststream_sqs_sns::ConnectedSqsBroker::sns_publisher) даёт издателя вне приложения.

Тема, имя которой оканчивается на `.fifo`, - это тема FIFO.
Она передаёт группу сообщений публикации подписанным на неё очередям FIFO, с теми же [шагами группы](fifo.md), которые принимает очередь FIFO.
Очередь FIFO подписывается только на тему FIFO.

Для публикации сервису нужны `sns:CreateTopic` и `sns:Publish`, а если он сам подписывает очереди, то и `sns:Subscribe`.

## Тестирование доставки всем подписчикам {/* #testing-the-fan-out */}

Файл примера запускает одно тело теста дважды.
Внутри процесса аккаунт доставляет сообщения темы в очереди, подписанные через `subscribe_queue_to_topic`, поэтому хук настраивает тест так же, как промышленную среду.
Очередь, которую оператор подписал вне сервиса, называется на политике через [`fans_out_to`](../components/sns/fans-out-to.md) - шаг фичи `testing`.
Если задана переменная `SQS_TEST_ENDPOINT`, то же тело выполняется на LocalStack через `TestApp::start_live`:

```bash
cargo test --features sns
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test --features sns -- --test-threads=1
```

Обвязку и режим работы со стендом разбирает руководство [Тестирование сервиса на SQS](testing.md).
