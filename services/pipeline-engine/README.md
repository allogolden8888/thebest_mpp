# Pipeline Engine

**Основание:** `development_plan.md` Фаза 2.1 — центральный оркестратор "ходового скелета" (Главный агент). Пятый сервис подряд, реально скомпилированный и протестированный, и самый сложный из пяти: единственный, обходящий **весь** граф пайплайна, а не одну стадию.

**Статус:** `cargo build && cargo test`, **12/12 тестов проходят**, компилирует настоящие `platform-contracts/{common,events}/*.proto` (два protobuf package с cross-package ссылками — впервые в этой серии сервисов).

```bash
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/pipeline-engine
cargo test
```

## Реальная находка компилятора: два protobuf package и относительные пути prost

`mpp.events.v1` (`IncomingMessage`) ссылается на типы `mpp.common.v1` (`SmsPayload`, `Channel`) — prost-build генерирует эти ссылки как относительные Rust-пути (`super::super::common::v1::Channel`), которые жёстко предполагают, что итоговая структура модулей в точности повторяет точки в имени package (`mpp.common.v1` → `mod mpp { mod common { mod v1 } } }`). Первая версия `proto.rs` оборачивала сгенерированный код в плоские `pub mod common`/`pub mod events` (тот же паттерн, что у сервисов с одним package) — не собралась: `cannot find common in super`. Все предыдущие сервисы этой сессии (Destination Resolution, Policy, Billing, Routing) использовали только один package (`mpp.common.v1`), поэтому эта проблема физически не могла проявиться раньше — Pipeline Engine первый, кому нужен `IncomingMessage` из `mpp.events.v1`. Исправлено точной вложенностью `pub mod mpp { pub mod common { pub mod v1 {...} } pub mod events { pub mod v1 {...} } }` + короткие `pub use`-алиасы для остального кода.

## Что реализовано по service_internal_methods.md §1.4

| Метод | Где | Примечание |
|---|---|---|
| `handle_incoming` + `resolve_pipeline_version` | `kafka_io::handle_incoming` | Резолв пайплайна упрощён до одного файла (Фаза 2.2, один тестовый партнёр) — реальный `resolve_pipeline_version(partner_id, application_id, snapshot)` не реализован |
| `resolve_next_stage` | `execution_state::resolve_next_stage` | Чистая функция; два хардкод-правила графа (Destination Resolution первый, Policy.REJECTED→Billing) закодированы **в данных** `pipeline.valid.json`, не в этой функции — она их не знает специально, просто обходит граф |
| `handle_stage_completed` **со специальным случаем Billing/BLOCKED** | `execution_state::handle_stage_completed` | Единственное реальное runtime-правило вне графовых данных — см. ниже |
| `build_stage_execute` | `build_stage_execute.rs` | Строит нужный вариант `stage_extension` oneof по имени целевой стадии из накопленного `ExecutionState` |
| `publish_stage_execute` / `handle_stage_completed` (Kafka) | `kafka_io::run_incoming_loop` / `run_completed_loop` | Два независимых консьюмера — `incoming.messages` запускает пайплайн, `stage.completed` продвигает |
| `check_admission`, `cas_transition_and_track_deadline`, `publish_hold_command`, `finalize_pipeline` | — | **Не реализовано в этом срезе** — см. "Что НЕ реализовано" |

## Ключевой тест — правило, которое существует НЕЗАВИСИМО от графа

`service_internal_methods.md` §1.4 формулирует особый случай текстом: "если завершилась Billing, а сохранённая category == BLOCKED — Terminal, **независимо от того, что граф конфигурации мог бы предписать** для обычного SUCCEEDED от Billing". Это не то же самое, что "граф правильно разводит два случая отдельными узлами" (так уже сделано в `pipeline.valid.json` — REJECTED от Policy ведёт в отдельный `n_billing_blocked`) — это **defense-in-depth поверх графа**, страховка на случай, если граф сконфигурирован неправильно.

`billing_blocked_override_fires_even_if_graph_would_route_onward` доказывает именно это: тест конструирует **патологический граф**, где узел `n_billing_blocked` с `SUCCEEDED` ведёт в `n4_routing` (как будто граф ошибочно НЕ развёл случаи), но `category` в состоянии — `BLOCKED`. Override обязан сработать и привести к `Terminal`, несмотря на то, что граф прямым текстом предписывает `ROUTING`. Без этого теста было бы легко реализовать только "правильный граф + обычный traversal" и упустить, что документ требует отдельной, независимой проверки.

## Тесты — что ещё доказано

* **`full_happy_path_walks_entire_real_graph_to_terminal`** — весь путь `n1_destination_resolution → n2_policy → n3_billing → n4_routing → n5_delivery → n6_reconciliation → Terminal` на настоящем `config_schemas/examples/pipeline.valid.json`, с накоплением `resolved_operator_id`/`category`/`route_id` в `ExecutionState` на каждом шаге и передачей их в команду следующей стадии.
* **`policy_rejected_routes_to_billing_blocked_node_then_terminates`** — то же графовое правило, что уже провалидировано структурно в `config_schemas/validate_all.py`, здесь проверено через реальный обход рантайм-кодом.
* **`billing_command_carries_accumulated_operator_and_category`** — `BillingExtension.segment_count` не пересчитывается, а несётся из значения, посчитанного один раз при `handle_incoming` (HLD: "посчитано один раз Pipeline Engine при кэшировании контекста").

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`ExecutionState` хранится в `Arc<Mutex<HashMap>>` в памяти процесса, не в Runtime Redis с атомарным CAS.** Это не мелкое упрощение, как у предыдущих сервисов (Billing/Policy read-then-write из Redis) — это **более серьёзное ограничение**: при нескольких репликах Pipeline Engine (`k8s/generate_manifests.py` планирует 17 инстансов при целевой нагрузке) состояние не разделяется между ними вообще. Сообщение, чей `stage.completed` придёт на другую реплику, чем та, что видела `incoming.messages`, не найдёт своего состояния и будет молча отброшено (`тестировано косвенно — run_completed_loop логирует "нет ExecutionState" и continue, не паникует, но и не восстанавливает`). **Это единственный сервис в серии, где текущая реализация физически не работает с более чем одной репликой** — приоритетный блокер перед Фазой 2.3/2.4, не перед "нагрузкой", а перед **любым** многоинстансным деплоем.
* **`check_admission`/`cas_transition_and_track_deadline`/`publish_hold_command` не реализованы** — Execution Control (Фаза 3.1, владелец Субагент 1) публикует `execution.control`, которое Pipeline Engine должен проверять перед каждой диспетчеризацией; здесь это отсутствует полностью, не заглушка "всегда Admit" (даже такой заглушки нет).
* **`Delivery`/`DeliveryReconciliation` extensions собираются некорректно-заглушечно** (`build_stage_execute.rs`: используется `RoutingExtension` как временный fallback) — эти два варианта `stage_extension` нуждаются в данных (`route_id`/`protocol` для Delivery, `triggering_outcome` для Reconciliation), для которых полный источник в `ExecutionState` ещё не продуман; тесты этого пути не покрывают, только зафиксировано в коде явным комментарием.
* **`docker build` не выполнялся, ни разу не запущено против реального Kafka-брокера** — та же оговорка, что у всех предыдущих сервисов.
* `ArcSwap`/hot-reload графа из `config.changes` — граф грузится один раз при старте.
