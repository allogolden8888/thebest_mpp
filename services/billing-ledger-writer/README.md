# Billing Ledger Writer

**Основание:** `development_plan.md` — Субагент 1, Billing периметр. `services_specifictaion.md` §6.2: `billing.ledger` → PostgreSQL double-entry ledger.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Персистентность — против реального локального PostgreSQL 17 (jOOQ, без codegen, `migrations/V008__billing_ledger.sql`).

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
cd services/billing-ledger-writer
mvn test
```

## Что реализовано по service_internal_methods.md §5.2

| Метод | Где | Как проверено |
|---|---|---|
| `on_ledger_event` | `kafkaio/LedgerEntryMapper::fromProto` — `LedgerEvent` (proto) → `LedgerEntry` (store model) | `LedgerEntryMapperTest` — конверсия `minor_units` → `NUMERIC(18,4)`, charge/compensating маппинг |
| `insert_double_entry` | `store/LedgerStore::insert` — `INSERT ... ON CONFLICT (charge_id) DO NOTHING` | `LedgerStoreTest` — реальная вставка на PostgreSQL, повторная вставка того же `charge_id` реально no-op (`insert` возвращает `false`, не бросает и не дублирует строку) |
| `apply_compensating_entry` | Тот же `LedgerStore::insert`, с `LedgerEntry.entryType="compensating"` | `LedgerStoreTest::compensatingEntryWithSourceChargeIdInsertsSuccessfully` — реальная вставка компенсирующей записи со ссылкой на исходный `charge_id` |

`store/LedgerEntry` — конструктор-валидатор проверяет **в коде**, не только полагаясь на PostgreSQL CHECK, оба инварианта `migrations/V008`: `compensating` требует `source_charge_id`, `charge` его запрещает (`LedgerEntryTest`, 5 тестов) — раннее обнаружение ошибки до похода в БД.

## Открытый вопрос — конвертация minor_units → NUMERIC(18,4)

`LedgerEntryMapper` делит `minor_units` на 100 (предполагает 2 знака после запятой для валюты, как UZS = tiyin). Для валют с другим числом дробных знаков эта константа неверна — сейчас платформа работает только с UZS (`hld.md`), но если появится другая валюта, потребуется таблица экспонент по `currency_code` (аналогично ISO 4217 minor unit exponents) — не реализовано, задокументировано как известное ограничение, не скрыто.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `Main.java` использует реальный `kafka-clients` `KafkaConsumer`, компилируется, не проверялся против `kind`+Strimzi.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`inserted_total`, `duplicate_total`).