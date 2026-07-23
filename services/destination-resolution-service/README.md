# Destination Resolution Service

**Основание:** `development_plan.md` Фаза 2.1 — "реализация каждого сервиса из LLD-методов на своём языке", первый срез. Выбран как первый сервис для реализации намеренно: самый маленький и самый ранний в графе пайплайна (`hld.md` жёсткое правило — Destination Resolution всегда первая стадия), с минимумом внешних зависимостей (никакого Redis/PostgreSQL в рантайме, только immutable snapshot).

**Статус:** реально компилируется и тестируется — `cargo build`/`cargo test`, не псевдокод. 10/10 тестов проходят.

```bash
brew install librdkafka
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/destination-resolution-service
cargo build
cargo test
```

## Что реализовано по service_internal_methods.md §1.4a

| Метод | Где |
|---|---|
| `resolve_operator_by_range` | `src/resolver.rs` — MNP overlay проверяется до number_range (точное совпадение перекрывает диапазон, `data_infrastructure_spec.md` §1.9a), Live HLR не в скоупе (HLD §26) |
| `handle_destination_resolution_execute` / `publish_stage_completed` | `src/kafka_io.rs::handle_command` (чистая функция, тестируется без брокера) + `run_loop` (реальная Kafka-обвязка через `rdkafka`) |

## Реальная сквозная проверка контрактов, не только описание

`build.rs` компилирует **те же** `platform-contracts/common/{enums,types,stage_contract}.proto`, что уже провалидированы `protoc --descriptor_set_out` на шаге `platform_contracts.md`, через `prost-build` — в реальные Rust-типы (`StageExecuteCommand`, `StageCompletedEvent`, `DestinationResolutionExtension`/`Result`). Это первый раз, когда контракты реально скормлены компилятору целевого языка сервиса, а не только протестированы на уровне protoc.

## Тесты — что доказано, не только запущено

`src/resolver.rs`:
* **9 задокументированных префиксов** (`migrations/V011__number_range.sql`) резолвятся в правильного оператора — `998901331835` (реально прогнан на PostgreSQL в этой сессии, `migrations/README.md`) и ещё 8, сконструированных внутри тех же верифицированных диапазонов.
* **MNP override побеждает диапазон** — номер из диапазона beeline с override на ucell в снапшоте резолвится в ucell, контрольная проверка соседнего номера без override — в beeline.
* **Непокрытый префикс возвращает `NotFound`**, не тихо резолвится в произвольного оператора (Perfectum/UMS — реальные операторы Узбекистана, не входящие в подтверждённый неполный список).
* Нечисловой destination_address не паникует.

`src/kafka_io.rs` (`handle_command` — чистая функция, без сети):
* Успешный резолв → `Outcome::Succeeded` + `DestinationResolutionResult.resolved_operator_id`.
* Не найден → `Outcome::Rejected` + `reason_code="OPERATOR_NOT_FOUND"`.
* Отсутствие `DestinationResolutionExtension` в команде → `Rejected`, не panic.
* `stage_execution_id`/`message_id` пробрасываются в результат — нужны Pipeline Engine для идемпотентности (HLD §4).

`src/health.rs`: `/healthz` всегда 200; `/readyz` — 503 до загрузки снапшота, 200 после (то есть k8s readinessProbe реально не пустит трафик на под, пока snapshot не готов, не просто предполагается).

## Данные

`data/number_range_snapshot.json` — та же форма, что уже провалидирована в `config_schemas/number_range.schema.json`, наполнена реальными 9 диапазонами из `migrations/V011`. `portability_overrides["998901339999"]` — **синтетический пример для теста**, не реальные данные MNP (источник/периодичность MNP-синка по-прежнему открытый вопрос, см. `development_plan.md` 5.2).

В проде снапшот строится из `config.changes` (`entity_type=number_range`/`number_portability_override`), не из локального файла — `SNAPSHOT_PATH` здесь заглушка на то же самое содержимое.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — тот же недоступный Docker daemon, что помешал в начале сессии с PostgreSQL (`migrations/README.md`). `Dockerfile` написан по стандартному multi-stage паттерну, сверен с реальными зависимостями `Cargo.toml`, но не прогнан.
* **Ни разу не запущено против реального Kafka-брокера** — `src/kafka_io.rs::run_loop` использует настоящие `rdkafka` типы (`StreamConsumer`/`FutureProducer`), компилируется, но end-to-end (реальный `docker`/`kind` кластер с Strimzi) не поднят в этом окружении.
* Retry/backoff на транзиентные ошибки брокера, DLQ-публикация на `stage.destination-resolution.dlq` (топик уже спланирован в `infra/kafka/`, producer сюда не подключён), обработка consumer group rebalance за пределами дефолтного поведения `StreamConsumer` — см. `src/kafka_io.rs` docstring.
* `/metrics` — плейсхолдер Prometheus exposition format (валидный, но без реальных счётчиков resolved/not_found) — реальные метрики вне скоупа этого среза.
* `ArcSwap` заявлен в `services_specifictaion.md` §2.4a для hot-reload снапшота из `config.changes`, но здесь снапшот грузится один раз при старте — hot-reload не реализован.
