# Message State Resolver

**Основание:** `development_plan.md` Фаза 2.1, десятый сервис "ходового скелета" (Главный агент) — единственный владелец партнёрского lifecycle-статуса: превращает внутренние `stage.completed`/`delivery.status` в `message.lifecycle` (`service_internal_methods.md` §2.4, `hld.md` §10). Третий Java-сервис серии.

**Статус:** `mvn test`, **28/28 тестов проходят**, компилирует настоящие `platform-contracts/{common,events}/*.proto`. Порт `state_machines/message_lifecycle.py` (10/10 тестов) 1:1 — все 10 Python-тестов перенесены построчно под теми же именами.

```bash
cd services/message-state-resolver
mvn test
```

## Что упрощено относительно `services_specifictaion.md` §3.2 — и что НЕ упрощено

Спека требует Kafka Streams + RocksDB state store + `exactly_once_v2`. Как и `billing-service`/`delivery-service`, этот срез — plain Java, не полный Kafka Streams DSL (минимум зависимостей, реально компилируемая сборка). **Но в отличие от прошлых упрощений в этой сессии, здесь не упрощена сама гарантия из HLD §10.1** ("одна Kafka-транзакция: запись в `message-state.changelog` + `message.lifecycle` + commit input offset") — `KafkaIo.java` использует настоящий transactional `KafkaProducer` (`initTransactions`/`beginTransaction`/`sendOffsetsToTransaction`/`commitTransaction`) — тот же низкоуровневый API, на котором построен `exactly_once_v2` внутри самого Kafka Streams, только без топологии DSL поверх него. Упрощена только **локальная проекция состояния**: `MessageStateStore` — in-memory `ConcurrentHashMap`, не RocksDB — тот же класс упрощения, что `ExecutionState` в `pipeline-engine`.

**Важное ограничение, не мелочь (как и у pipeline-engine):** без RocksDB + restore-from-changelog состояние теряется при рестарте пода — `message-state.changelog` продолжает быть authoritative на бумаге, но здесь ничего не читает его обратно при старте. Это единственный на сегодня блокер перед реальным многоинстансным/переживающим рестарт деплоем этого сервиса.

**Также не решено полностью:** правильная привязка `transactional.id` к владению партицией под ребалансировкой (то, что Kafka Streams EOS решает автоматически через group-instance fencing) — здесь `transactional.id` строится из стабильного per-instance идентификатора (`MSR_INSTANCE_ID`), корректно для одной реплики, не гарантированно safe при ребалансировке с несколькими репликами.

## Реальная находка: явной таблицы "stage_name+outcome → LifecycleStatus" нет ни в одном документе

`hld.md` §10 и `state_machines.md` §1 формализуют полностью, КАКИЕ переходы допустимы между уже-известными статусами (`state_machines/message_lifecycle.py`, перенесено 1:1 в `TransitionValidator.java`) — но ни один документ не говорит, ОТКУДА эти статусы берутся из сырых `stage.completed`/`delivery.status` событий. `CandidateTransitionResolver.java` — построенная здесь таблица, с обоснованием по каждой строке в коде, не угаданная вслепую:

* **`BILLING` stage.completed никогда не производит переход** — чисто внутренняя бухгалтерия, партнёру не видна, независимо от исхода (включая `SUCCEEDED` по `BLOCKED`-тарифу после отклонения Policy — к этому моменту `REJECTED` от `POLICY` уже терминален, дальнейшее Billing-событие безопасный no-op).
* **`retryable=true` никогда не производит переход** — Scheduler ещё повторит стадию (Billing `ACCOUNT_FROZEN`/`STALE_EPOCH` — реальный пример из уже написанного `billing-service`), применять статус сейчас значило бы либо откатывать историю задним числом (запрещено), либо ловить ложный REGRESSION на легитимном следующем событии.
* **`POLICY REJECTED` → `REJECTED`** — единственный явный пример из документов.
* **`DELIVERY SUCCEEDED` → `SUBMITTED`**, **`DELIVERY FAILED` → `UNDELIVERABLE`** (оператор синхронно и окончательно отклонил submit — confirmed отказ ОТ оператора, не внутренняя ошибка), **`DELIVERY TIMED_OUT`/`RETRY_EXHAUSTED` → `SYSTEM_UNAVAILABLE`** (оператор/канал не отвечал).
* **`DELIVERY_RECONCILIATION`** — `ReconciliationOutcome` спроектирован так, что его значения уже почти дословно совпадают с именами `LifecycleStatus` (`DELIVERY_CONFIRMED`/`DELIVERY_FAILED`/`DELIVERY_UNRESOLVED`) — сильное, не притянутое обоснование маппинга 1:1.
* **`DESTINATION_RESOLUTION`/`ROUTING` REJECTED/FAILED/TIMED_OUT/RETRY_EXHAUSTED → `FAILED`** — системная невозможность обработать (нерезолвимый msisdn, нет здорового маршрута), не подтверждённый отказ оператора и не блокировка Policy.
* **`delivery.status.normalized_status`** парсится напрямую в `LifecycleStatus` без отдельной таблицы — by design: `dlr-manager`'s `NormalizeStatus` уже эмитит буквально строки `"DELIVERED"`/`"UNDELIVERABLE"`.

## Реальная находка: лишняя secret-зависимость в `k8s/generate_manifests.py`

`SECRET_DEPENDENCIES["message-state-resolver"]` был выставлен на `["redis-configuration"]`, хотя `service_io_contracts.md` §2.4 перечисляет ровно 2 входа (`stage.completed`, `delivery.status`) и 2 выхода (`message.lifecycle`, `message-state.changelog`) — ни один не Redis. Убрано как необоснованная запись (не додумано заново, просто убрано — сервис реально не использует Redis).

## Тесты — что доказано

28 тестов:
* `TransitionValidatorTest` (11) — все 10 тестов `message_lifecycle.py` перенесены построчно (entry points, `SUBMITTED→DELIVERED/UNDELIVERABLE`, симметричный запрет `DELIVERED↔UNDELIVERABLE`, единственное исключение `DELIVERY_UNRESOLVED→LATE_DELIVERY_CONFIRMED`, все полностью терминальные статусы отклоняют всё, дубликат `event_id` — no-op не ошибка, прямой вход Reconciliation без `SUBMITTED`).
* `CandidateTransitionResolverTest` (12) — вся таблица маппинга выше, включая **BILLING не производит переход ни при одном из 8 возможных `Outcome`** и **retryable=true подавляет переход независимо от стадии**.
* `MessageStateStoreTest` (2), `HealthServerTest` (3).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **Нет RocksDB/restore-from-changelog** — состояние не переживает рестарт пода, см. выше.
* **`transactional.id` не решает fencing под ребалансировкой для >1 реплики** — см. выше.
* **REGRESSION логируется, не пишется в `reconciliation_cases`** — `state_machines.md` §1.3 упоминает эту таблицу как место фиксации аномалий позднего/противоречивого DLR, запись туда не реализована в этом срезе (владелец таблицы — Delivery Reconciliation Service судя по названию, не подтверждено).
* **`docker build` не выполнялся** — недоступен Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера**, включая сами транзакции — тот же паттерн, что у всех Kafka-сервисов этой сессии.
