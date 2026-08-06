# Billing Reconciliation

**Основание:** `development_plan.md` — Субагент 1, Billing периметр (Фаза 3.3, freeze/unfreeze). `services_specifictaion.md` §6.3: сравнение Redis balance и PostgreSQL ledger, freeze аккаунта, detect/classify/alert, fenced recovery, compensating entries.

**Статус:** реально компилируется и тестируется — `mvn test`, **30/30** (Java 25). Ядро (`BillingAccountStateMachine`) — прямой перенос 1:1 `state_machines/billing_account_state.py` (8/8 тестов, те же имена). Redis fenced CAS и PostgreSQL-агрегация протестированы против реальных локальных Redis/PostgreSQL. gRPC-клиент к Execution Control протестирован через реальный in-process gRPC round-trip. `MainTest` (новое, см. "Проверено кодревью" ниже) — реальный end-to-end прогон `Main.reconcileOne`/`reconcileFrozenAccount` через Postgres+Redis+in-process gRPC разом, не отдельных компонентов по одному.

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
| `persist_audit` | `store/ReconciliationAuditStore` | `MainTest` — реальный PostgreSQL, `migrations/V021__billing_reconciliation_audit.sql` (см. "Проверено кодревью" ниже) |
| `trigger_unfreeze` | `grpcclient/ExecutionControlClient::triggerUnfreeze` | `ExecutionControlClientTest` — реальный in-process gRPC `ClearOverride` |

## Проверено кодревью (CODE_REVIEW.md): CRITICAL #6 + HIGH #7/#8 — freeze без пути назад в ACTIVE — исправлено

`Main.reconcileOne` раньше обрабатывала ТОЛЬКО `ACTIVE`-аккаунты (freeze при HIGH drift); `FROZEN`-аккаунт никогда не пересматривался на последующих циклах — ни `BalanceRecomputer`-driven recompute-then-unfreeze, ни `BillingRedisClient.applyFencedUnfreeze`, ни `ExecutionControlClient.triggerUnfreeze` не вызывались НИКОГДА, несмотря на то, что все три уже существовали и были протестированы по отдельности. Каждый реальный drift-инцидент навсегда сажал аккаунт "на прикол" — доступность-инцидент при каждом срабатывании reconciliation, не self-healing поведение, которое описывает дизайн.

Исправлено в `Main.java` (`reconcileOne` + новый `reconcileFrozenAccount`, оба теперь package-private для тестируемости):

* **CRITICAL #6.** `FROZEN`-аккаунт теперь пересматривается на каждом цикле: если drift всё ещё `HIGH` — остаётся `FROZEN` (см. HIGH #8 ниже); если разрешился — `applyFencedUnfreeze` + `triggerUnfreeze`, реальный возврат в `ACTIVE`. Намеренно НЕ форсирует произвольный "recomputed balance" в Redis при unfreeze (см. HIGH #7 ниже) — сохраняет ТЕКУЩИЙ Redis-баланс как есть, снимает только `FROZEN`-состояние. CAS-конфликт (конкурентная реплика reconciliation) обрабатывается безопасно — просто повтор на следующем цикле, ничего не теряется.
* **HIGH #8.** Раньше исключение из `triggerFreeze` (gRPC на Execution Control) пропускало `persist_audit` целиком, и поскольку на следующем цикле `account.state() != ACTIVE`, `triggerFreeze` никогда не повторялся — ни аудита, ни retry. Теперь: Redis freeze (идемпотентный) применяется независимо от gRPC-исхода, `persist_audit` вызывается ВСЕГДА (`action="FREEZE"` либо `"FREEZE_PARTIAL"`), и `reconcileFrozenAccount` реально повторяет `triggerFreeze` на каждом следующем цикле, пока drift остаётся `HIGH` — упавший gRPC-вызов не теряется навсегда, а автоматически повторяется максимум через 60с.
* **HIGH #7 (частичная митигация, не полный фикс).** `BalanceRecomputer` вычисляет ДЕЛЬТУ (`compensating - charge`), не абсолютный баланс — схема `billing.billing_ledger` не несёт `entry_type` для top-up/начального пополнения счёта, поэтому реально провизированный с ненулевым стартовым балансом аккаунт показывает перманентный "phantom drift". Полноценный фикс требует согласования нового `entry_type`/схемы ledger с Главным агентом (владеет `billing_ledger`) — не сделано в этом проходе. Применена документированная частичная митигация: новая env-переменная `RECONCILIATION_DRIFT_FREEZE_EXEMPT_ACCOUNT_IDS` (CSV, тот же паттерн, что `RECONCILIATION_ACCOUNT_IDS`) — известно-провизированные аккаунты можно явно исключить из авто-freeze, не выдумывая несуществующий ledger `entry_type`.
* **Сопутствующая находка**: `billing.reconciliation_audit` не была мигрирована (см. старый "Открытый вопрос" ниже, ныне закрыт) — без этой таблицы `persist_audit` физически не мог работать, значит и recompute-then-unfreeze нельзя было бы протестировать end-to-end. Добавлена `migrations/V021__billing_reconciliation_audit.sql` (та же схема, что уже была задокументирована в докстринге `ReconciliationAuditStore` — не выдумана заново), применена и провалидирована на локальном PostgreSQL 17.

Тесты (`MainTest`, новый, 6 тестов, **реальный Postgres + реальный Redis + in-process gRPC fake**, тот же паттерн, что `ExecutionControlClientTest`): `frozenAccountWithResolvedDriftIsAutomaticallyUnfrozen` — прямое доказательство фикса #6, `FROZEN` реально возвращается в `ACTIVE`. `frozenAccountWithPersistentDriftStaysFrozenAndReassertsFreeze` — HIGH drift не размораживает, `triggerFreeze` реально re-asserted. `activeAccountFreezesInRedisEvenWhenExecutionControlCallFails` — прямое доказательство фикса #8, Redis freeze + audit `FREEZE_PARTIAL` персистятся, даже когда gRPC ломается намеренно. `driftFreezeExemptAccountIsNeverFrozenDespiteHighDrift` — доказательство #7-митигации. `unfreezeDoesNotOverwriteRedisBalanceWithGuessedAbsoluteFigure` — баланс не переписывается на unfreeze.

## Открытые вопросы (задокументированы, не скрыты)

1. **Список аккаунтов для периодической сверки не специфицирован** ни в одном документе LLD — нет метода "перечислить все billing-аккаунты". `Main.java` использует env-переменную (`RECONCILIATION_ACCOUNT_IDS`, CSV) как заглушку; реальный источник (SCAN по `billing:balance:*` в Redis, либо отдельная таблица счетов в PostgreSQL) не определён.
2. **`account_id` → `partner_id` резолв — placeholder** (`Main.java::reconcileOne` использует `accountId` напрямую как `partnerId` для `trigger_freeze`/`trigger_unfreeze`) — реальная связь между Billing-аккаунтом и партнёром не описана в документах этой сессии в терминах, доступных этому сервису.
3. **`recompute_balance` — дельта, не абсолютный баланс с нуля** — см. "Проверено кодревью" HIGH #7 выше, частичная митигация есть (exempt-list), полный фикс требует схемы.
4. **`await_outbox_drain`** — не реализовано: нужен способ узнать, что `billing-outbox-publisher` обработал все pending записи для аккаунта (например, `XPENDING`/`XAUTOCLAIM` по `billing:outbox:{shard}` с фильтром по `account_id`, чего Redis Streams не поддерживает напрямую без сканирования) — не решено в этом срезе.
5. **Нет leader election/distributed lock для multi-replica деплоя** (CODE_REVIEW.md Medium #10) — две реплики, гоняющие reconciliation параллельно, могут гонять `triggerFreeze`/CAS unfreeze друг у друга под ногами; сам fenced CAS безопасен (не теряет данные), но может дать лишний `UNFREEZE_CONFLICT` в аудите — не решено в этом проходе.

## Отклонение от стека LLD

Собран без Micronaut — та же причина и то же решение, что `services/delivery-reconciliation-service` (см. его README "Отклонение от стека LLD").

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Execution Control Service** — только in-process gRPC-тест с фейковым сервером.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.