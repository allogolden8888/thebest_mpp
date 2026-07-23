# Routing Service

**Основание:** `development_plan.md` Фаза 2.1, четвёртый сервис "ходового скелета" (Главный агент) — сразу после Billing в графе пайплайна. Четвёртый сервис подряд, реально скомпилированный и протестированный (после Destination Resolution, Policy, Billing), Rust.

**Статус:** `cargo build && cargo test`, **10/10 тестов проходят** на первом прогоне, компилирует настоящие `platform-contracts/*.proto`.

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

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено против реального Kafka-брокера** — та же оговорка, что у предыдущих трёх сервисов.
* **`ControlSnapshot` (execution.control, scope=OPERATOR_ROUTE) в `main.rs` — всегда пустой** (fail-open: все маршруты ACTIVE). Реальная проекция из Kafka-топика `execution.control` в локальный snapshot — работа Execution Control Service (Фаза 3.1, владелец Субагент 1) и её потребление здесь — не реализовано в этом срезе, сознательно отложено.
* **Operator Route Registry (Runtime Redis, hld.md §11.4) не подключён — намеренно, не пробел.** `select_route_and_protocol` здесь выбирает `route_id`/`protocol` из конфигурации, не резолвит owning instance того маршрута — резолвинг instance делает Delivery Service при обращении к Operator SMPP Session Manager/Operator HTTP Gateway, вне скоупа Routing. `redis` не добавлен в зависимости этого сервиса вообще (в отличие от первого черновика, где был скопирован по инерции из Policy Service, а затем удалён — этому сервису он не нужен).
* `ArcSwap` для hot-reload снапшота из `config.changes` — снапшот грузится один раз при старте, тот же паттерн упрощения, что у предыдущих сервисов.
