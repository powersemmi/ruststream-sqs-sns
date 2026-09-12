# ruststream-sqs-sns

**`ruststream-sqs-sns`** - брокер Amazon SQS для фреймворка обмена сообщениями
[RustStream](https://powersemmi.github.io/ruststream/) с публикацией через SNS всем подписчикам
разом. Подписчик читает свою очередь длинным опросом и умеет брать встроенные пакеты через
`ReceiveMessage`. Повторные попытки идут через тайм-аут видимости сообщения, а в очередь
недоставленных сообщение перекладывает политика redrive этой очереди. Группу сообщений вы задаёте
на каждую публикацию в FIFO-очередь или закрепляете за всей позицией публикации, а в полученном
сообщении читаете её обратно.

Транспорт построен на официальных клиентах [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) и
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns). Фича `testing` поставляет внутрипроцессный тестовый
брокер.

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

<div class="grid cards" markdown>

- :material-aws: **[Руководство по SQS](sqs.md)** - дескрипторы очередей, завершение доставки, пакеты, FIFO-группы, доставка через SNS всем подписчикам разом и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, роутинг, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-sqs-sns)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает SQS и SNS. Понятия фреймворка, общие для всех брокеров, описаны в
[документации RustStream](https://powersemmi.github.io/ruststream/).
