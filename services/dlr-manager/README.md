# DLR Manager

**Основание:** `development_plan.md` Фаза 2.1, девятый сервис "ходового скелета" (Главный агент) — нормализует сырые DLR операторов, сопоставляет их с исходными сообщениями через `dlr.dlr_correlation` (написанную `dlr-correlation-writer`), публикует `delivery.status` (`service_internal_methods.md` §4.2). Второй Go-сервис Главного агента в этой сессии.

**Статус:** `go build ./... && go test ./...`, **26/26 тестов проходят**, 5 из них — реально против живых локальных PostgreSQL 17 и Redis (не моки).

```bash
cd services/dlr-manager
go build ./...
go test ./...
```

## Реальная находка: `SchedulerBackgroundTask` не может нести payload ретраиваемого DLR

`operator.dlr.unresolved` по `platform_contracts.md` (каталог топик→тип) должен нести `mpp.events.v1.OperatorDlr` — тот же тип, что `operator.dlr`. Но `SchedulerBackgroundTask` (`scheduler_events.proto`, единственное, что реально попадает в `scheduler.background.commands` → Scheduler Background Lane → republish в `target_topic`) несёт только `source_event_id` (строку-ссылку), не сам DLR. **Это не гипотеза** — Субагент 1 уже реализовал и закоммитил `scheduler-background-lane` и явно задокументировал то же наблюдение в его README ("Открытые вопросы" п.1): их `DispatchBuilder.buildDlrRetryWakeup` republish'ит саму `SchedulerBackgroundTask` (с `attempt+1`) на `operator.dlr.unresolved`, **не** `OperatorDlr`, и явно помечает это как "предположение... не подтверждено кодом DLR Manager (сервис Главного агента)".

Этот сервис подтверждает и замыкает то предположение: `internal/pending.Store` — кэш в Runtime Redis (`dlr:pending:{event_id}`, TTL = окно корреляции), куда DLR Manager сохраняет исходный `OperatorDlr` при первом планировании retry (`schedule_retry`), и откуда достаёт его обратно по `source_event_id`, когда получает wake-up на `operator.dlr.unresolved`. Без этого кэша retry был бы физически нечем повторно коррелировать — у `SchedulerBackgroundTask` просто нет данных для этого.

`event_id`, под которым кэшируется DLR, **детерминированный**, не случайный (`dlr.DeriveEventID` — SHA-256 от `operator_id|smsc_message_id|segment_id|raw_status|received_at`) — намеренно: at-least-once редоставка одного и того же `operator.dlr` не должна порождать вторую независимую цепочку retry для того же DLR (тот же класс дублирования, что offset-commit-до-подтверждения в других сервисах, только на уровне бизнес-логики, не Kafka-оффсета).

## Что реализовано по service_internal_methods.md §4.2

| Метод | Где | Примечание |
|---|---|---|
| `on_raw_dlr` | `internal/dlr/parser.go::DecodeOperatorDlr` | Чистая функция; `smsc_message_id` обязателен (в отличие от `OperatorSubmitAccepted`) |
| `normalize_operator_status` | `internal/dlr/parser.go::NormalizeStatus` | Стандартный SMPP v3.4 `stat`-словарь (DELIVRD/EXPIRED/DELETED/UNDELIV/REJECTD → терминальные; ACCEPTD/UNKNOWN — намеренно не распознаны, не терминальны) |
| `lookup_correlation` | `internal/correlation/store.go::Store.Lookup` | Реальный `pgx` SELECT из `dlr.dlr_correlation`, `nil, nil` — не найдено, не ошибка |
| `publish_delivery_status` | `internal/dlr/builders.go::BuildDeliveryStatusEvent` + `kafkaio.Producer.PublishDeliveryStatus` | |
| `schedule_retry` | `internal/dlr/builders.go::BuildRetryTask` + `pending.Store.Save` + `kafkaio.Producer.PublishRetryTask` | |
| `evaluate_correlation_window` | `internal/dlr/decision.go::Decide` | Дедлайн = `received_at` (из ИСХОДНОГО DLR, не момента ретрая) `+ correlationWindow`, не количество попыток |
| `publish_dlr_dlq` | `kafkaio.Producer.PublishDlq` | Republish того же `OperatorDlr` на `operator.dlr.dlq` (тот же тип по `platform_contracts.md`) |

## `evaluate_correlation_window` — почему 48 часов

Дефолт `DLR_CORRELATION_WINDOW=48h` — не выдуман отдельно, взят той же границей, что retention `dlr.dlr_correlation` (`dlr.drop_old_correlation_partitions`, default 48ч, `migrations/V009__dlr_correlation.sql`): ретраить дольше бессмысленно — даже если бы correlation появилась, партиция, где она хранится, могла быть уже удалена retention'ом. Оба значения требуют того же уточнения по реальным SLA операторов, которое уже отмечено как открытое в `data_infrastructure_spec.md` §1.8.

## Реальная находка (систематическая, не только этот сервис): секреты k8s — дискретные переменные, не готовая connection string

`k8s/generate_manifests.py`'s `SECRET_DEPENDENCIES` явно называл DLR Manager сервисом, для которого "документ не называет хранилище явно... секрет не назначен" — закрыто здесь: добавлена запись `"dlr-manager": ["postgresql", "redis-runtime"]`. При проверке того, что реально попадёт в под (`envFrom: secretRef`), обнаружилось: `infra/secrets/generate_external_secrets.py` инжектит `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`/`POSTGRES_USER`/`POSTGRES_PASSWORD` и `REDIS_RUNTIME_HOST`/`REDIS_RUNTIME_PORT`/`REDIS_RUNTIME_PASSWORD` **дискретно** — не единой `DATABASE_URL`/`REDIS_RUNTIME_URL`, которую ожидал этот сервис (и, как выяснилось при этой же проверке, **все** предыдущие сервисы этой сессии: billing-service, policy-service, delivery-service, partner-rest-receiver, dlr-correlation-writer). В реальном кластере ни один из них не подключился бы — переменная, которую они читают, никогда не будет установлена, и код тихо падал бы обратно на hardcoded дефолт без креденшлов.

Исправлено здесь (`cmd/dlr-manager/main.go::buildDatabaseURL`/`buildRedisRuntimeURL` — собирают connection string из дискретных переменных, с `DATABASE_URL`/`REDIS_RUNTIME_URL` как явным override для локальной разработки/тестов, приоритет у override) и **тем же паттерном ретроактивно исправлено во всех пяти перечисленных сервисах** отдельными коммитами сразу вслед за этим — см. их README, раздел "Исправлено" в каждом.

## Тесты — что доказано

26 тестов:
* `internal/dlr` (16) — декодирование `OperatorDlr` (обязательный `smsc_message_id`, мусорные байты), нормализация статуса (5 терминальных кодов, ACCEPTD/UNKNOWN/произвольный код — не распознаны), `Decide` (все 4 исхода, включая **найденная correlation побеждает истёкшее окно** — гонка, где retry успевает найти запись прямо перед dlq), `DeriveEventID` (детерминизм, различие для разных DLR), построение `DeliveryStatusEvent`/`SchedulerBackgroundTask` (`attempt` — passthrough, не инкремент; `deadline` считается от `received_at`, не от `now`).
* `internal/correlation` (2, **реально против живой PostgreSQL**) — найденная запись читается корректно; ненайденная — `nil, nil`, не ошибка.
* `internal/pending` (3, **реально против живого Redis**) — save/get round-trip, TTL реально истекает (`time.Sleep` + повторный `Get`), несуществующий ключ — `found=false`, не ошибка.
* `cmd/dlr-manager` (5) — `buildDatabaseURL`/`buildRedisRuntimeURL`: сборка из дискретных секретных переменных, поведение без пароля, явный override побеждает.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **Ни разу не запущено против реального Kafka-брокера** — тот же паттерн, что у всех Kafka-сервисов этой сессии.
* **`docker build` не выполнялся** — недоступен Docker daemon.
* **`KindDropUnrecognizedStatus` — нет DLQ-пути.** `operator.dlr.dlq` предназначен именно для "окно корреляции истекло" (`evaluate_correlation_window` → Expire), не для "статус не распознан" — DLR с ACCEPTD/UNKNOWN/незнакомым кодом сегодня просто логируется и отбрасывается без публикации куда-либо. Требует либо расширения словаря `NormalizeStatus` по мере появления реальных операторов, либо отдельного DLQ-пути — не в этом срезе.
* **Кэш `internal/pending` не имеет верхней границы размера** — TTL = correlation window (до 48ч) ограничивает время жизни записи, но не общий объём одновременно ожидающих DLR — тот же класс, что уже задокументирован как отложенный в billing-service/policy-service (unbounded growth в допустимых пределах TTL).
* **Poison-message (невалидный protobuf на любом из двух входных топиков) не продвигает оффсет**, если это единственная запись с последнего успешного commit — тот же класс, что уже задокументирован (не исправлен) в нескольких сервисах `CODE_REVIEW.md`.
