# Policy Service

**Основание:** `development_plan.md` Фаза 2.1, второй сервис "ходового скелета" (Главный агент) — сразу после Destination Resolution в графе пайплайна. Порт уже готовой и протестированной Python-реализации (`policy_matching/`) на целевой язык сервиса (Rust, `services_specifictaion.md` §2.5), не написание логики с нуля — минимизирует риск дизайна за счёт того, что алгоритм уже доказан.

**Статус:** реально компилируется и тестируется — `cargo build && cargo test`, **27/27 тестов проходят**, компилирует настоящие `platform-contracts/*.proto`.

```bash
brew install librdkafka
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/policy-service
cargo build
cargo test
```

## Порт Python -> Rust — что перенесено 1:1, что изменилось

| Python (`policy_matching/`) | Rust (`services/policy-service/src/`) | Изменилось ли |
|---|---|---|
| `template_matching.py` (`pyahocorasick`) | `template_matching.rs` (`aho-corasick` крейт) | Только механика автомата: `pyahocorasick` поддерживает "одна строка -> список значений" нативно (`automaton.add_word(key, [...])`), `aho-corasick` крейт — нет. Каждый `(template_id, frag_idx)` фрагмент стал отдельным паттерном в автомате (дубликаты строк разрешены, у каждого свой `PatternID`), `pattern_owner: Vec<(String, usize)>` — обратный маппинг. Алгоритм (2 фазы: кандидаты по литералам -> точечная проверка плейсхолдеров) не изменился |
| `banword_normalization.py` (`unicodedata.normalize`) | `banwords.rs` (`unicode-normalization` крейт, `.nfkc()`) | Не изменилось — та же таблица гомоглифов (11 пар), тот же двойной проход (normalized + despaced) |
| `policy_engine.py` (`evaluate_policy`) | `policy_engine.rs` | Не изменилось — тот же порядок 8 проверок (7→6→1→5→4→3→2), та же семантика short-circuit с `category="BLOCKED"` |

**Все тесты одноимённые** (`test_banword_blocks_even_though_template_matches` -> `banword_blocks_even_though_template_matches` и т.д.) — намеренно, чтобы можно было визуально сверить, что порт не потерял ни один случай.

## Что добавлено сверх Python-версии (там, где Python не заходил до Kafka/Redis)

`policy_matching/policy_engine.py` тестировал только чистую функцию `evaluate_policy` — Kafka и Runtime Redis были явно вне скоупа того шага. Здесь (`kafka_io.rs`, `main.rs`) — то, чего не было:

* **`MessageContextStore`** — трейт `fetch(message_id) -> MessageContext`, реальная `RedisMessageContextStore` (компилируется против `redis` крейта, `HGETALL msgctx:{message_id}`). Нужен, потому что `PolicyExtension` в `stage.policy`-команде несёт только `resolved_operator_id` — `body`/`msisdn`/`sender_id` НЕ дублируются в stage-командах (`service_internal_methods.md` §0), Policy обязан прочитать их из Runtime Redis по `message_id`.
* Kafka-обвязка (`stage.policy` -> `stage.completed`, `PolicyResult{category}`) — тот же паттерн, что `destination-resolution-service/src/kafka_io.rs`: `handle_command` — чистая функция (тестируется без сети), `run_loop` — реальный `rdkafka`-цикл, не интеграционно проверенный.

## Тесты — что доказано

27 тестов, три группы:
* `template_matching.rs` (7) — та же матрица случаев, что в Python: реальный пример из чата матчится, счётчик цифр вне `{1,6}` не матчится, `%w` не допускает внутренний пробел, отсутствующий литеральный фрагмент не матчится, разделители между цифрами игнорируются, неоднозначность двух кандидатов разрешается детерминированно, ведущий/замыкающий плейсхолдер работает.
* `banwords.rs` (7) — включая гомоглиф-обход (кириллическая "о" вместо латинской в `qоtaq`) и letter-spacing обход (`т е р р о р и з м`, ловится только despaced-проходом), и **осознанный false-positive** despaced-прохода как задокументированный компромисс, не баг.
* `policy_engine.rs` (9) + `kafka_io.rs` (3) — полная оркестрация 8 проверок на реальном `config_schemas/examples/policy_ruleset.valid.json` (та же кросс-артефактная сверка, что в Python-версии), включая `banword_blocks_even_though_template_matches` (permissive `%w` не создаёт обход контентной модерации) и `spam_throttled_after_limit_and_counter_not_incremented_on_rejects`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступен Docker daemon в этом окружении (см. `services/destination-resolution-service/README.md` — та же причина).
* **Ни разу не запущено против реального Kafka-брокера или Runtime Redis** — `RedisMessageContextStore`/`run_loop` компилируются против настоящих клиентских крейтов, но live не проверены.
* **`RuntimeState` (consent-блэклисты, spam-счётчик) остаётся in-memory**, не перенесён на Runtime Redis — в реальном проде это разделяемое между репликами состояние (как `MessageContextStore`), перенос не сделан в этом срезе ради объёма. Один инстанс Policy Service, развёрнутый как задумано (`k8s/generate_manifests.py`: 3 реплики), с in-memory `RuntimeState` даст **неверный** spam-throttling (каждая реплика считает свою историю) — это реальное ограничение, не мелочь, зафиксировано как первоочередной технический долг перед Фазой 2.3/2.4.
* Расширение таблицы гомоглифов и приоритизация при реальной неоднозначности нескольких шаблонов — `development_plan.md` 4.3/4.4, отдельные пункты плана, не в этом срезе.
* `ArcSwap` (заявлен в `services_specifictaion.md` §2.5 для hot-reload ruleset/шаблонов из `config.changes`) — снапшот грузится один раз при старте, hot-reload не реализован (тот же паттерн, что у Destination Resolution).
