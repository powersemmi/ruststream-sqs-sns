---
sidebar_position: 5
description: "Отправка в очередь SQS FIFO по группам сообщений, отбрасывание повторных отправок по идентификатору дедупликации и обработка нескольких групп одновременно, каждой по порядку."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Очереди FIFO

Это руководство отправляет сообщения в очередь SQS FIFO по группам сообщений, отбрасывает повторные отправки по идентификатору дедупликации и обрабатывает несколько групп одновременно, каждую по порядку.

Очередь FIFO сохраняет порядок сообщений внутри группы сообщений и выдаёт следующее сообщение группы только после того, как удалено предыдущее.
Каждая отправка также указывает идентификатор дедупликации, и в течение пяти минут очередь отбрасывает повторную отправку с тем же идентификатором ([AWS: дедупликация](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)).
Очередь, имя которой оканчивается на `.fifo`, - это [очередь FIFO](../components/subscriptions/fifo.md).

Пример - сервис доставки.
Он превращает каждый заказ в отправление, и отправления одного клиента уходят в том порядке, в каком клиент делал заказы.

## Отправка в группе {/* #sending-in-a-group */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#dispatch"
```

</TabItem>
</Tabs>

`dispatch` публикует отправление в `shipments.fifo` через слот `Shipments`.
Слот ограничен [`SqsPublishOptions`](../components/publishing/options.md), поэтому публикация принимает шаги [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps).
Обработчик, который вызывает эти шаги, импортирует прелюдию этого крейта.

[`group_id`](../components/publishing/group-id.md) помещает отправление в группу клиента.
Если публикация не называет группу, группа берётся из заголовка [`PARTITION_KEY_HEADER`](../components/delivery/partition-key.md) сообщения, затем из группы, которую зафиксировала политика через `Publish::default().group_id(..)`, и наконец это `"default"`.
Этот заголовок читает каждый брокер RustStream, поэтому отправителю, который его задаёт, шаг SQS не нужен.

[`deduplication_id`](../components/publishing/deduplication-id.md) - это идентификатор заказа.
Когда `orders` доставляет заказ дважды, второе отправление - это отправка, которую очередь отбрасывает.
Публикация, которая не называет идентификатор дедупликации, получает уникальный для процесса идентификатор, поэтому две одинаковые полезные нагрузки, отправленные намеренно, приходят обе.
У политики нет идентификатора дедупликации: один постоянный идентификатор отбросил бы каждую отправку после первой.

Группу или идентификатор дедупликации на стандартной очереди крейт отвергает с ошибкой [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo).

## Несколько групп одновременно {/* #several-groups-at-once */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#ship"
```

</TabItem>
</Tabs>

Доставка из очереди FIFO хранит свою группу в `PARTITION_KEY_HEADER`.
`workers(4, by_key)` запускает четыре конвейера обработки и отправляет все сообщения одной группы в один и тот же конвейер.
Поэтому заказы четырёх клиентов отправляются одновременно, а отправления одного клиента уходят одно за другим.
Одно получение может вернуть несколько сообщений одной группы, и простой `workers(4)` выполнил бы их параллельно.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#app"
```

</TabItem>
</Tabs>

[`create_if_missing`](api:ruststream_sqs_sns::SqsQueue::create_if_missing) создаёт `shipments.fifo` как очередь FIFO с дедупликацией по содержимому.
Очереди FIFO нужна DLQ типа FIFO; предел разбирает руководство [Переобработка и DLQ](retries.md).
SQS ограничивает пропускную способность одной группы, поэтому сервис распределяет нагрузку по многим группам.

Тесты в файле примера проверяют группу, в которой уходит отправление, отправляют один заказ дважды и отправляют заказы двух клиентов одновременно.
Обвязку разбирает руководство [Тестирование сервиса на SQS](testing.md).
