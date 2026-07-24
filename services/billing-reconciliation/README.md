# Billing Reconciliation

**Основание:** `development_plan.md` — Субагент 1, Billing периметр (Фаза 3.3, freeze/unfreeze). `services_specifictaion.md` §6.3: сравнение Redis balance и PostgreSQL ledger, freeze аккаунта, detect/classify/alert, fenced recovery, compensating entries.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Ядро (`BillingAccountStateMachine`) — прямой перенос 1:1 `state_machines/billing_account_state.py` (8/8 тестов, те же имена). Redis fenced CAS и PostgreSQL-агрегация протестированы против реальных локальных Redis/PostgreSQL. gRPC-клиент к Execution Control протестирован через реальный in-process gRPC round-trip.

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
brew services start redis   # если ещё не запущен
cd services/billing-reconciliation
mvn test
```

## `BillingAccountStateMachine` — перенос 1:1 `state_machines/billing_account_state.py`

`core/BillingAccountStateMachine.java` (`freeze`/`unfreeze`/`applyCharge`) + `core/Account.java` — тот же алгоритм, что уже спроектирован и доказан в Python (8/8 тестов, `state_machines/billing_account_state.py`), перенесённый на Java тем же методом, что `execution-control-service` перенёс гистерезис. `BillingAccountStateMachineTest` — те же 8 тестов, включая ключевое свойство fencing (`testInFlightChargeRejectedByStaleEpochRace`): операция, читавшая `epoch=0` до freeze, обязана быть отклонена, если пытается применить после freeze (`epoch=1`).

`applyCharge` перенесён вместе с `freeze`/`unfreeze`, хотя сам Billing Reconciliation его не вызывает в проде (это путь `billing-service`, сервиса Главного агента) — перенос сохраняет модель целиком, потому что recovery (`apply_fenced_cas`) обязан не сломать то же свойство fencing, которое `applyCharge` проверяет.

## Что реализовано по service_internal_methods.md §5.3

| Метод | Где | Как проверено |
|---|---|---|
| `compute_drift` | `core/DriftReport` | Косвенно через `DriftClassifierTest` |
| `classify_drift` | `core/DriftClassifier` | `DriftClassifierTest` — NONE/LOW/MEDIUM/HIGH, знак дельты не влияет на severity (абсолютное значение) |
| `trigger_freeze` | `redisio/BillingRedisClient::freeze` (реальный `WATCH`/`MULTI`/`EXEC`) + `grpcclient/ExecutionControlClient::triggerFreeze` | `BillingRedisClientTest` (реальный Redis: epoch реально инкрементируется, идемпотентно) + `ExecutionControlClientTest` (реальный in-process gRPC: `PARTNER_STAGE`, `scope_id="{partner_id}:BILLING"`, `PAUSED` отправлены верно) |
| `await_outbox_drain` | **не реализовано** — см. ниже | — |
| `recompute_balance` | `store/BalanceRecomputer` | `BalanceRecomputerTest` — реальный PostgreSQL: charge-only даёт отрицательную дельту, compensating полностью компенсирует, пустой ledger даёт 0 |
| `apply_fenced_cas` | `redisio/BillingRedisClient::applyFencedUnfreeze` — реальный `WATCH`/`MULTI`/`EXEC`, не Lua (см. докстринг класса) | `BillingRedisClientTest` — CAS с верным epoch применяется, с устаревшим — реально отклоняется (`wasDiscarded()`), состояние не меняется |
| `persist_audit` | `store/ReconciliationAuditStore` | **Не протестировано против реальной БД** — таблица не мигрирована в этой сессии, см. "Открытый вопрос" ниже |
| `trigger_unfreeze` | `grpcclient/ExecutionControlClient::triggerUnfreeze` | `ExecutionControlClientTest` — реальный in-process gRPC `ClearOverride` |

## Открытые вопросы (задокументированы, не скрыты)

1. **`billing.reconciliation_audit` не мигрирована.** `migrations/` — общий артефакт сессии (та же категория, что `platform-contracts/`), последняя миграция на момент этого среза — `V017`. Односторонне добавлять `V018` рискует конфликтом нумерации с Главным агентом при параллельной работе — `store/ReconciliationAuditStore` реализован (реальный jOOQ, реальные типы, компилируется), но не протестирован против реальной БД, а предлагаемая DDL-схема задокументирована в докстринге класса для координации.
2. **Список аккаунтов для периодической сверки не специфицирован** ни в одном документе LLD — нет метода "перечислить все billing-аккаунты". `Main.java` использует env-переменную (`RECONCILIATION_ACCOUNT_IDS`, CSV) как заглушку; реальный источник (SCAN по `billing:balance:*` в Redis, либо отдельная таблица счетов в PostgreSQL) не определён.
3. **`account_id` → `partner_id` резолв — placeholder** (`Main.java::reconcileOne` использует `accountId` напрямую как `partnerId` для `trigger_freeze`) — реальная связь между Billing-аккаунтом и партнёром не описана в документах этой сессии в терминах, доступных этому сервису.
4. **`recompute_balance` — дельта, не абсолютный баланс с нуля** — `migrations/V008__billing_ledger.sql` не моделирует top-up/начальное пополнение счёта, только `charge`/`compensating` — см. докстринг `BalanceRecomputer`.
5. **`await_outbox_drain`** — не реализовано: нужен способ узнать, что `billing-outbox-publisher` обработал все pending записи для аккаунта (например, `XPENDING` по `billing:outbox:{shard}` с фильтром по `account_id`, чего Redis Streams не поддерживает напрямую без сканирования) — не решено в этом срезе.

## Отклонение от стека LLD

Собран без Micronaut — та же причина и то же решение, что `services/delivery-reconciliation-service` (см. его README "Отклонение от стека LLD").

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Execution Control Service** — только in-process gRPC-тест с фейковым сервером.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.