# Routing Service

**Основание:** `development_plan.md` Фаза 2.1, четвёртый сервис "ходового скелета" (Главный агент) — сразу после Billing в графе пайплайна. Четвёртый сервис подряд, реально скомпилированный и протестированный (после Destination Resolution, Policy, Billing), Rust.

**Статус:** `cargo build && cargo test`, **11/11 тестов проходят** (было 10 — +1 по итогам development_plan.md 5.5 ниже), компилирует настоящие `platform-contracts/*.proto`.

```bash
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/routing-service
cargo test
```

## Реальная кросс-артефактная сверка

`config_schemas/routing_table.schema.json` и его пример `config_schemas/examples/routing_table.valid.json` были спроектированы на шаге "Kubernetes-манифесты"/"config_schemas" этой же сессии — до того, как этот сервис существовал. Тесты здесь **загружают тот же файл напрямую** (`include_str!`), не переизобретают fixture — если бы схема и этот сервис разошлись в понимании формы `routing_table` (например, разное имя поля), тест бы не скомпилировался/не распарсился, а не молча прошёл на выдуманных данных.

## Порт HLD-методов в код — что объединено, что нет

`service_internal_methods.md` §1.7 перечисляет `select_routes_for_operator` → `filter_by_control_state` → `select_route_and_protocol` → `apply_failover` как четыре отдельных метода. Здесь они — **одна функция** `resolve_final_route` (`routing.rs`), с явным обоснованием в комментарии: "выбрать сконфигурированный primary, если он здоров, иначе — следующий по приоритету среди здоровых" — это одно решение, не четыре шага с промежуточным состоянием, которое стоило бы материализовывать отдельно. `select_routes_for_operator` тоже тривиален здесь, потому что снапшот уже организован `HashMap<operator_id, RouteTable>` (та же форма, что `routing_table.schema.json`), не единый плоский список маршрутов всех операторов.

## Тесты — что доказано

* **`healthy_primary_is_chosen`** — здоровый sконфигурированный primary (`beeline_smpp_primary`, protocol SMPP) выбирается как есть, `route_version` эхом из `routing_table.version`.
* **`paused_primary_fails_over_to_reserve`** — PAUSED primary → failover на `beeline_http_reserve`, **включая смену протокола** (SMPP → HTTP) — failover в этой архитектуре не ограничен одним протоколом, ровно то, что обсуждалось в чате про "primary/reserve могут быть разных протоколов".
* **`degraded_primary_still_preferred_over_reserve`** — DEGRADED (не PAUSED) не исключает маршрут из здоровых, sконфигурированный primary остаётся предпочтительным — деградация не равна недоступности.
* **`all_routes_paused_returns_no_healthy_route_error`** / **`unknown_operator_returns_error_not_panic`** — оба класса ошибок явные типы (`RoutingError`), не паника; `kafka_io`-тесты проверяют, что первое ретраябельно (`NO_HEALTHY_ROUTE` — маршруты могут восстановиться), а второе — нет (`UNKNOWN_OPERATOR` — конфигурационная проблема, повтор не поможет).

## development_plan.md 5.5 — реальные connection-профили операторов, не один тестовый

`Main.rs` до этого шага грузил **ровно один** `routing_table`-файл (`ROUTE_TABLE_PATH`, дефолт `beeline_uz`) — явно помеченная Фаза 2.2 заглушка "один тестовый оператор". Теперь мержит `ROUTE_TABLE_PATH` (основной, обязателен) + `ROUTE_TABLE_EXTRA_PATHS` (запятая-разделённый список, дефолт — `routing_table.ucell_uz.valid.json`/`routing_table.uzmobile_uz.valid.json`) в один `RouteTableSnapshot` — `from_tables` уже строил `HashMap<operator_id, RouteTable>`, ограничение было только в том, что `main.rs` грузил один файл, не в структуре снапшота. По умолчанию сервис теперь резолвит route для всех трёх реально задокументированных операторов Узбекистана (`migrations/README.md` "список заведомо неполный") — `snapshot_merged_from_multiple_files_resolves_each_operator_independently` в `routing.rs` доказывает, что три `operator_id` из трёх разных файлов не коллизируют в одном снапшоте.

**Реальный найденный баг, не гипотетический.** `migrations/V011__number_range.sql` сеяла `routing.number_range.operator_id` БЕЗ суффикса (`beeline`/`ucell`/`uzmobile`), тогда как `routing_table.valid.json` (и остальные `config_schemas/examples/{operator,number_range}.valid.json`) использовали `_uz` (`beeline_uz`). `RouteTableSnapshot::for_operator` — точное совпадение по `HashMap`-ключу, не fuzzy — то есть **реальное сообщение с `resolved_operator_id='beeline'` от destination-resolution-service не находило бы маршрут здесь вообще**, для всех трёх операторов, при первом реальном прогоне пайплайна целиком. Исправлено `migrations/V023__number_range_operator_id_uz_suffix.sql` + синхронизированный `services/destination-resolution-service/data/number_range_snapshot.json` — нормализовано на `_uz` (5 мест уже так называли, 2 — нет). Полный разбор — `migrations/README.md` "V023".

**Сознательно НЕ реализовано на этом шаге:** `operator-smpp-session-manager`/`operator-http-gateway` сами остаются **один процесс = один оператор** (`OPERATOR_ID` env var, дефолт `beeline_uz`) — реальный SMPP bind к ucell_uz/uzmobile_uz одновременно потребовал бы N подов с разным `OPERATOR_ID`/секретами, то есть правки `k8s/generate_manifests.py` (вне зоны правок в этой сессии, файл прямо запрещён к изменению). Этот шаг закрывает **модель данных** (routing-table + operator connection-profile для всех трёх операторов, реально провалидированы схемой, реально резолвятся) — не физическое N-инстансное развёртывание коннекторов, это следующий, отдельный шаг.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено против реального Kafka-брокера** — та же оговорка, что у предыдущих трёх сервисов.
* **`ControlSnapshot` (execution.control, scope=OPERATOR_ROUTE) в `main.rs` — всегда пустой** (fail-open: все маршруты ACTIVE). Реальная проекция из Kafka-топика `execution.control` в локальный snapshot — работа Execution Control Service (Фаза 3.1, владелец Субагент 1) и её потребление здесь — не реализовано в этом срезе, сознательно отложено.
* **Operator Route Registry (Runtime Redis, hld.md §11.4) не подключён — намеренно, не пробел.** `select_route_and_protocol` здесь выбирает `route_id`/`protocol` из конфигурации, не резолвит owning instance того маршрута — резолвинг instance делает Delivery Service при обращении к Operator SMPP Session Manager/Operator HTTP Gateway, вне скоупа Routing. `redis` не добавлен в зависимости этого сервиса вообще (в отличие от первого черновика, где был скопирован по инерции из Policy Service, а затем удалён — этому сервису он не нужен).
* `ArcSwap` для hot-reload снапшота из `config.changes` — снапшот грузится один раз при старте, тот же паттерн упрощения, что у предыдущих сервисов.
