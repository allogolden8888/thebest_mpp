# Billing Service

**Основание:** `development_plan.md` Фаза 2.1, третий сервис "ходового скелета" (Главный агент). Первый сервис в **Java** (`services_specifictaion.md` §2.6), первый порт из Python в Java в этой сессии — `state_machines/billing_account_state.py` (спроектирован и протестирован ранее, 8/8) переносится на целевой язык 1:1.

**Статус:** реально компилируется и тестируется — `mvn compile && mvn test`, **27/27 тестов проходят**, компилирует настоящие `platform-contracts/*.proto`. Прошёл независимый кодревью (`CODE_REVIEW.md`, инициирован пользователем параллельно с разработкой) — все 3 critical и 3 из 4 high находки исправлены, см. раздел ниже.

```bash
brew install openjdk@25 maven
export PATH="/opt/homebrew/opt/openjdk@25/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@25"
cd services/billing-service
mvn test
```

## Реальная проблема окружения, найденная и решённая на этом шаге

`mvn compile` изначально падал с `PKIX path building failed` при обращении к Maven Central — не из-за кода, а потому что в этой сети TLS-трафик проходит через инспектирующий прокси (`issuer=Unitel LLC`, FortiGate), чей корневой сертификат есть в системном Keychain macOS (поэтому `curl`/`brew` работали), но не в отдельном truststore (`cacerts`) свежеустановленного JDK — Java по умолчанию не использует системный Keychain. Решено импортом `Unitel Root Certification authority` из `login.keychain-db` в `cacerts` через `keytool -importcert`. Зафиксировано здесь, потому что это тот класс проблем "Docker/PostgreSQL/Kafka" из `development_plan.md` "Координация", который не был предсказан заранее — реальная, специфичная для этой сети ловушка, не Java-специфичная в общем случае.

Второй реальный баг, пойманный компилятором, не предположенный: `protobuf-maven-plugin` изначально был настроен с `protobuf-java:4.28.3`, но системный `protoc` в этом окружении (35.1, установлен в предыдущих шагах сессии для `platform-contracts/`) генерирует Java-код, использующий более новый API `Descriptors` (`EnumDescriptor.getValue(int)`, `FileDescriptor.getMessageType(int)` и т.д.), которого в 4.28.3 просто нет — `cannot find symbol` на голом месте. Исправлено поднятием `protobuf-java` до 4.35.1 (последняя стабильная не-RC версия на Maven Central на момент шага, тот же принцип "latest stable", что был явно запрошен для стека платформы раньше в этом чате).

## Порт Python -> Java — что перенесено 1:1

| Python (`state_machines/billing_account_state.py`) | Java (`BillingAccountState.java`) | Изменилось ли |
|---|---|---|
| `@dataclass(frozen=True) class Account` | `record Account` | Java record — тот же immutable value type, что Python frozen dataclass |
| `freeze`/`unfreeze`/`apply_charge` (свободные функции) | статические методы `BillingAccountState.freeze/unfreeze/applyCharge` | Не изменилось — тот же алгоритм, тот же порядок проверок в `applyCharge` (dedup → epoch → state) |
| `raise ValueError` в `unfreeze` при устаревшем epoch | `throw StaleEpochException` (unchecked) | Механика исключений вместо Python-исключения, семантика та же |
| 8 тестов, один-в-один | 8 тестов в `BillingAccountStateTest`, те же имена в camelCase | Полная сверка 1:1, включая ключевой `inFlightChargeRejectedByStaleEpochRace` |

## Что добавлено сверх Python-версии — оркестрация и две открытые находки

`billing_account_state.py` тестировал только state machine саму по себе — не оркестрацию `handle_billing_execute` (`service_internal_methods.md` §1.6) и не Kafka/Redis. Здесь (`BillingService.java`, `TariffResolver.java`, `KafkaIo.java`, `BillingAccountStore.java`) — то, чего не было:

* **`TariffResolver`** — `resolve_tariff`, чистый lookup `price_per_segment[category] × segment_count`, форма данных = `config_schemas/billing_tariff.schema.json`, тесты грузят реальный `config_schemas/examples/billing_tariff.valid.json` (та же кросс-артефактная сверка, что у Policy Service).
* **`BillingService.handleBillingExecute`** — связывает `resolve_tariff` → `apply_atomic_charge` → `StageCompletedEvent`, с явным маппингом `ChargeOutcome` → `Outcome`/`reason_code`/`retryable` (`ACCOUNT_FROZEN`/`STALE_EPOCH` оба `RETRYABLE=true`, per service_internal_methods.md).
* **Открытая находка №1 — источник `account_epoch` не формализован в контрактах.** Ни `BillingExtension`, ни `StageExecuteCommand` не несут явного поля под epoch — `handleBillingExecute` принимает его отдельным параметром (не читает из того же `account`, который проверяется), чтобы race "freeze произошёл между диспетчеризацией команды и обработкой" вообще было можно смоделировать и доказать тестом (`staleEpochRejectedAsRetryableNotSilentlyApplied`) — если бы epoch читался из того же объекта, что проверяется, свойство fencing было бы структурно недоказуемо через этот метод. Вероятный источник в реальности — Execution State в Runtime Redis, закэшированный Pipeline Engine на момент диспетчеризации (hld.md §15.3), не formalized в текущих `platform-contracts`.
* **Открытая находка №2 — источник `partner_id`/`account_id` тоже не в контрактах**, тот же класс пробела, что нашла Policy Service для `msisdn`/`sender_id`/`body`. `KafkaIo`/`Main` используют `BILLING_ACCOUNT_ID` env var как заглушку на единственного тестового партнёра (Фаза 2.2), не общее решение.

## Исправлено по итогам независимого кодревью (`CODE_REVIEW.md`)

Кодревью запущен пользователем параллельно с разработкой (покрывает обе ветки — `main` и `subagent-1`). Секция про `billing-service` нашла 3 critical, 4 high, 7 medium, 6 low находок. Исправлено:

* **Critical — offset-commit баг, реально терявший сообщения навсегда.** Старая версия коммитила `consumer.commitAsync()` без аргументов после каждой удачной записи — этот вызов коммитит **текущую позицию всего фетча**, не оффсет обработанной записи. Если запись B в батче `[A, B, C]` падала (перехватывалась `try/catch { continue; }`), а C после неё обрабатывалась успешно, коммит после C перепрыгивал через B — при рестарте она не переобрабатывалась, терялась молча. Исправлено: коммит по каждому `TopicPartition` отдельно, только до первой ошибки в этом партишене за этот поллинг (`KafkaIo.OffsetTracker`, вынесенная чистая логика — кодревью также отметило, что баг был **вообще не покрыт тестами**, теперь 4 теста в `KafkaIoTest.java` без единого живого Kafka-брокера).
* **Critical — непровалидированный отрицательный `segment_count` инвертировал списание в начисление.** `TariffResolver`/`BillingAccountState` брали `segment_count` как есть; отрицательное значение делало `perSegment * segmentCount` отрицательным, `balance - amount` — начислением, и всё репортилось как обычный `SUCCEEDED`. Исправлено: `BillingService.resolveTariff` отклоняет `segment_count <= 0` до арифметики (`negativeSegmentCountRejectedBeforeArithmetic`/`zeroSegmentCountRejected` в `BillingServiceTest`).
* **Critical — TOCTOU race между двумя репликами при конкурентном списании с одного счёта.** Обычные ACTIVE-списания не двигают `epoch` (только freeze/unfreeze) — epoch fencing НЕ защищал от этого случая, `save()` был plain `hset`-перезапись, вторая реплика молча затирала первую. Исправлено (не только задокументировано): `BillingAccountStore.applyChargeAtomically` — Redis `WATCH`/`MULTI`/`EXEC` (оптимистичная блокировка), при обнаруженной гонке транзакция откатывается и повторяется до 5 раз. Не полная замена атомарного Lua-скрипта (`development_plan.md` 4.2, всё ещё не написан — тот убрал бы и сетевой round-trip), но устраняет именно потерю данных, а не просто документирует её.
* **High — graceful shutdown.** `Main.java` теперь регистрирует shutdown hook: `consumer.wakeup()` (единственный потокобезопасный способ прервать блокирующий `poll()` из другого потока — `consumer.close()` напрямую из hook-потока был бы отдельным багом), `health.stop()`, `accountStore.close()`; `KafkaIo.run` ловит `WakeupException` и закрывает consumer/producer из своего потока.
* **High — синхронный `producer.send().get()` без таймаута** мог зависать до `max.poll.interval.ms` на медленном/недоступном брокере. Исправлено — `get(10, TimeUnit.SECONDS)`.
* **High — `/readyz` флипался `true` до конструирования Kafka/Redis клиентов.** Переставлено после — с оговоркой в коде, что клиенты ленивые, "сконструирован" не значит "реально достижим" (полноценный readiness-пробник, например Redis `PING`, не реализован).
* **Low — тестовые нити:** `HealthServerTest` теперь биндится на `port=0` (эфемерный, не псевдослучайный диапазон — риск конфликта под CI), переименован `metricsReturns200NotFound` → `metricsReturns200` (было скопипащенное неверное имя), `HealthServer.stop()` — 1с grace period вместо 0.

**Сознательно НЕ исправлено на этом шаге** (medium/low, отложено, не забыто): unbounded рост `processedChargeIds` (нужен TTL/windowing — дизайн-решение, не быстрый фикс), новое TCP-соединение на каждый `fetch`/`applyChargeAtomically` вызов (нужен пул), `NullPointerException` на некорректном tariff JSON вместо явной ошибки, `default_category` fallback из схемы не используется, `ALREADY_PROCESSED`-путь не эхом цены первой попытки, отсутствие валюты в структуре счёта, hardcoded строки reason code вместо производных от enum.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся**, **ни разу не запущено против реального Kafka/Billing Redis** — та же оговорка, что у Destination Resolution/Policy Service.
* **`BillingAccountStore` теперь атомарна (WATCH/MULTI/EXEC), но не через Lua** — см. пункт про Critical TOCTOU выше; полный атомарный Lua/Redis Function — `development_plan.md` 4.2, ещё не написан.
* Полный Micronaut (DI, `application.yml`, health endpoints "из коробки") — заменён на plain Java + `com.sun.net.httpserver`, тот же принцип минимальных зависимостей, что у Rust-сервисов. Не редизайн, следующий шаг.
