# Config payload — JSON Schemas

**Основание:** `platform_contracts.md` §4 flagged "точная форма `payload_json` в `ConfigChangeEvent` по каждому `entity_type`" as unspecified. This closes that gap for all 9 `ConfigEntityType` values (`platform-contracts/common/enums.proto`).

**Статус:** структурно провалидировано `jsonschema` (Draft 2020-12) + семантические проверки, невыразимые чистым JSON Schema, реально прогнаны против примеров — 20/20 совпадений с ожиданием.

```bash
pip3 install jsonschema
python3 config_schemas/validate_all.py
```

## Файлы

| entity_type | Schema | Где уже была форма данных до этого документа |
|---|---|---|
| `pipeline` | `pipeline.schema.json` | Нигде — граф узлов спроектирован здесь впервые, из текстовых правил hld.md §6-7 |
| `routing_table` | `routing_table.schema.json` | Частично — hld.md §11 (route_id/protocol/tps_limit как понятия, не как схема) |
| `operator` | `operator.schema.json` | Частично — hld.md §11.1/§11.3 (точные имена полей SMPP/HTTP-профилей уже зафиксированы, здесь только собраны в JSON) |
| `partner` | `partner.schema.json` | Нигде — credentials/IP allowlist/applications упоминались только текстом |
| `policy_ruleset` | `policy_ruleset.schema.json` | Нигде — требования 2/3/5/7 Policy Engine (anti-spam, time-of-day, банворды, sender validation) впервые сведены в конфиг-схему |
| `billing_tariff` | `billing_tariff.schema.json` | Полностью — `data_infrastructure_spec.md` §1.6a, перенесено как есть |
| `number_range` | `number_range.schema.json` | Полностью — `data_infrastructure_spec.md` §1.9a / `migrations/V011` |
| `policy_template` | `policy_template.schema.json` | Полностью — `data_infrastructure_spec.md` §1.9b / `migrations/V013` |
| `subscriber_consent` | `subscriber_consent.schema.json` | Полностью — `data_infrastructure_spec.md` §1.9c / `migrations/V014` |

## Архитектурные решения, зафиксированные здесь впервые

1. **`pipeline`: граф — не condition-DSL, а `next: {outcome: node_id}`.** Ветвление ключуется по `Outcome` enum (`SUCCEEDED`/`REJECTED`/`FAILED`/...), уже существующему в `stage_contract.proto`, а не по строковым expression-условиям. Это делает фан-аут структурно невозможным (один outcome — один следующий узел, не массив) без отдельной проверки, и не требует придумывать/парсить новый язык условий.

2. **`operator` vs `routing_table`: разделение протокольной механики и выбора маршрута.** `operator.smpp_profile`/`http_profile` — параметры подключения (bind, retry, TPS по протоколу). `routing_table.routes[].tps_limit` — может быть *меньше*, чем в operator-профиле того же протокола (резервный канал сознательно ограничивают ниже возможностей канала). `routing_table.active_route_id` — единственный активный маршрут, а не флаг на каждом route, что напрямую отражает решение из общего чата: "рейт лимитить два канала сразу не нужно".

3. **Два хардкод-правила пайплайна проверяются кодом, не только схемой:**
   - `entry_node_id` обязан указывать на узел `DESTINATION_RESOLUTION` (Policy не может выполниться раньше, чем известен оператор);
   - любой узел `POLICY` обязан вести `REJECTED` в узел `BILLING` (отклонённое сообщение всё равно тарифицируется категорией `BLOCKED`), не терминировать напрямую.

   Оба — не выразимы в чистом JSON Schema (нужен обход графа), поэтому вынесены в `validate_pipeline_graph()` внутри `validate_all.py`, с примерами `pipeline.invalid_entry_not_destination_resolution.json` и `pipeline.invalid_policy_rejected_not_to_billing.json`, доказывающими, что нарушение реально ловится, а не только задокументировано как "должно проверяться".

4. **`policy_ruleset` не включает шаблоны и consent-блэклисты** — они остаются отдельными `entity_type` (`policy_template`, `subscriber_consent`) с собственными таблицами и путями обновления (полная запись против incremental snapshot patch), решение зафиксировано ранее в `services_specifictaion.md` §2.5 и `data_infrastructure_spec.md` §1.9c, здесь только соблюдается, не пересматривается.

## Чего здесь сознательно нет

* JSON Schema не может выразить арифметические/ссылочные инварианты (`range_end >= range_start`, `active_route_id ∈ routes[].route_id`, обход графа пайплайна) — это `SEMANTIC_CHECKS` в `validate_all.py`, отдельные Python-функции, не часть `.schema.json` файлов. Production-реализация (Java/Go/Rust per-service) обязана перенести те же проверки в свой config-loader при приёме `ConfigChangeEvent`, а не полагаться только на JSON Schema validator своего рантайма.
* Полный список префиксов номеров/операторов, полный список банвордов, полный список sender_id — примеры используют тот же неполный, но подтверждённый в чате набор данных, что и `migrations/`.
