# ruststream-sqs-sns

**`ruststream-sqs-sns`** - брокер Amazon SQS для фреймворка обмена сообщениями
[RustStream](https://powersemmi.github.io/ruststream/) с публикацией через SNS всем подписчикам
разом. Подписчик читает свою очередь длинным опросом и умеет брать встроенные пакеты через
`ReceiveMessage`. Повторные попытки идут через тайм-аут видимости сообщения, а в очередь
недоставленных сообщений его перекладывает политика перенаправления этой очереди. Группу сообщений
вы задаёте на каждую публикацию в FIFO-очередь или закрепляете за всей позицией публикации, а в
полученном сообщении читаете её обратно.

Транспорт построен на официальных клиентах [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) и
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns). Фича `testing` даёт брокеру внутрипроцессный
режим, и тест запускает рабочее приложение без сервера. Фича `asyncapi` добавляет привязки `sqs` в
документ, который создаёт фреймворк.

Крейт следует за выпущенной линейкой `ruststream` 0.7:

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

Собственная документация крейта - это его rustdoc на docs.rs, и открывается он тем руководством,
которое раньше лежало на этом сайте: что очередь умеет сама, как завершается доставка, куда
уходит исчерпавшее попытки сообщение и что может нести публикация.

<div class="grid cards" markdown>

- :material-download: **[Подписка](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#subscribing)** - дескриптор очереди, завершение доставки, предел попыток, пакеты и то, что несёт доставленное сообщение.
- :material-upload: **[Публикация](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#publishing)** - две политики публикации, ответы, доставка через SNS всем подписчикам разом и группы сообщений FIFO.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, роутинг, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-sqs-sns)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт - входная страница. Что делают SQS и SNS, описано в
[rustdoc крейта](https://docs.rs/ruststream-sqs-sns), там же тестирование рабочего приложения
внутри процесса и эксплуатационная сторона: издатель, права доступа и пределы службы. Понятия фреймворка,
общие для всех брокеров, описаны в
[документации RustStream](https://powersemmi.github.io/ruststream/).
