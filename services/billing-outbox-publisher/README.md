# Billing Outbox Publisher

**Основание:** `development_plan.md` — Субагент 1, Billing периметр (не hot-path `billing-service`). `services_specifictaion.md` §6.1: Billing Redis Stream → `billing.ledger`.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Redis-путь (XREADGROUP/XACK) протестирован против реального локального Redis (brew) — не мок.

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
brew services start redis   # если ещё не запущен
cd services/billing-outbox-publisher
mvn test
```

## Что реализовано по service_internal_methods.md §5.1

| Метод | Где | Как проверено |
|---|---|---|
| `poll_redis_stream` | `redisio/OutboxStreamReader::pollAllShards` — Lettuce `XREADGROUP` по всем `billing:outbox:{shard}` | `OutboxStreamReaderTest` — реальный `XADD`/`XREADGROUP` на локальном Redis: запись реально читается консьюмер-группой |
| `publish_ledger_event` | `kafkaio/LedgerEventBuilder` (чистая сборка `LedgerEvent`) + `LedgerEventPublisher` (реальный `kafka-clients`) | `LedgerEventBuilderTest` — charge/compensating, `billing_ledger_compensating_source_required` инвариант (compensating без `source_charge_id` отклоняется на уровне кода, не только БД) + `LedgerEventPublisherTest` (`MockProducer`, ключ = `account_id`) |
| `ack_stream_entry` | `redisio/OutboxStreamReader::ack` — `XACK` | `OutboxStreamReaderTest` — реальный `XPENDING` до/после подтверждает, что запись реально снимается с pending |

`core/StreamEntry` несёт `shard` явно (не восстанавливается из `redis_entry_id`) — `pollAllShards` знает, какой шард дал каждую запись, `ack` использует его напрямую, не гадая.

## Проверено кодревью (CODE_REVIEW.md): CRITICAL #1 — записи, застрявшие в PEL, никогда не перечитывались — исправлено

`pollAllShards` читает ТОЛЬКО через `XREADGROUP ... >` (никогда не доставленные записи) — запись, чей `publish`/`ack` не завершился (сбой Kafka, краш процесса между `XREADGROUP` и `XACK`), оставалась в PEL (pending entries list) consumer group навсегда: ни один код в сервисе никогда её не перечитывал. Поскольку charge уже атомарно применён к hot-балансу в Redis ДО записи в outbox stream (`hld.md §15.4`), это означало перманентную потерю ledger-события — до того, как reconciliation вообще заметит расхождение (возможно, днями позже), без единого сигнала до этого момента.

Исправлено: `redisio/OutboxStreamReader::reclaimStalePending` — `XAUTOCLAIM`, переносит записи, простаивающие в PEL дольше `RECLAIM_MIN_IDLE` (30с), на текущего consumer'а. Заявляет их заново независимо от исходного consumer'а — естественно переживает рестарт под новым `HOSTNAME` (тоже задокументированная в исходной находке проблема), не только транзиентный сбой publish. `Main.java` гоняет отдельный, более редкий цикл (`RECLAIM_INTERVAL_SECONDS=10`, не каждые 500мс, как обычный poll — `XAUTOCLAIM` сканирует весь PEL на каждый вызов) поверх ВСЕХ шардов, переиспользуя тот же `publishAndAck` путь, что обычные свежие записи.

Тесты (`OutboxStreamReaderTest`, реальный Redis): `reclaimStalePendingRecoversEntryThatWasNeverAcked` — читает запись, намеренно НЕ ack'ает (симулирует краш), подтверждает, что `pollAllShards` её больше не видит (доказывает исходную находку), затем что `reclaimStalePending` находит и возвращает её же (тот же `redisEntryId`), и что она реально ack'ается после. `reclaimStalePendingIgnoresEntryStillWithinMinIdleTime` — свежепрочитанная запись (< minIdleTime) не реклеймится — не мешает нормальной, ещё выполняющейся обработке.

## Открытый вопрос — форма полей billing:outbox:{shard}

`data_infrastructure_spec.md` §2.3 документирует `billing:outbox:{shard}` как STREAM с "финансовым событием (charge/compensating)", но не специфицирует точные имена полей внутри STREAM entry. `core/StreamEntry`/`OutboxStreamReader::toStreamEntry` предполагают `charge_id, account_id, partner_id, amount_minor_units, currency_code, entry_type, source_charge_id, reason, created_at_epoch_ms` — рабочее предположение, симметричное `billing.billing_ledger` (PostgreSQL, `migrations/V008`) и `LedgerEvent` (proto). Реальный формат пишет Billing Service (Главный агент, `billing-service`) — нужна сверка полей при интеграции, не тихо предполагается совпадение.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера.**
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (в т.ч. глубина PEL — не видно, сколько записей ждут reclaim, см. "Проверено кодревью" выше).