# A2P MPP — Математическая модель нагрузки и капасити-планирование

**Основание:** `hld.md`, `service_io_contracts.md`, `service_internal_methods.md`, `data_infrastructure_spec.md`
**Статус:** второй расчётный проход — с учётом оптимизаций (Critical Sweep вместо Kafka Streams Critical Lane, Redis pipelining, scoped Policy rules, устранение дублирования истории, консолидация control-plane). Числа по-прежнему оценки, не измерения; каждая цифра подлежит подтверждению нагрузочным тестированием (HLD §23). Формулы приведены полностью, чтобы пересчитать на реальных данных.

**v1 → v2, что изменилось в модели** (полное описание решений — в соответствующих документах, здесь только эффект на цифры):

1. Runtime Redis: 13 → **11 операций/сообщение** (Billing больше не читает `msgctx`; rate limit — периодическая синхронизация, не per-message) и пропускная способность шарда 70 000 → **100 000 ops/s** (пайплайнинг Redis-команд вместо одиночных round-trip).
2. Policy: throughput/ядро 2 400 → **6 000/с** (правила скомпилированы per-partner, а не единым автоматом).
3. Scheduler Critical Lane (Kafka Streams, полное потребление stage-топиков, ≈40 vCPU на burst) заменён на **Critical Sweep** (Go, периодический опрос Redis sorted set, флат ≈8 vCPU, не масштабируется с TPS).
4. Lifecycle Writer перестал читать `stage.completed` — входной поток 140 000 → **≈60 000 событий/сек**.
5. Мелкие Go control-plane сервисы (Execution Control, Configuration Service+workers, DLR Correlation Writer, Billing Outbox/Ledger Writer, Partner API, Backoffice API, Replay Service) — из N независимых HA-фleets в общий пул VM.

---

# 1. Допущения

Как в первом проходе, с обновлениями, отмеченными **жирным**.

| Параметр | Значение | Источник |
|---|---|---|
| λ_peak (пиковый входной TPS) | 20 000 msg/s | HLD §4 |
| λ_avg (средний рабочий TPS) | 13 000 msg/s | HLD §4 |
| Стадий в стандартном pipeline | 4 + Reconciliation-ветка | HLD §5.3 |
| Доля `SUBMISSION_OUTCOME_UNKNOWN` | 0.5–1% от Delivery-исходов | оценка |
| Доля сообщений, доходящих до DLR | ~95% от успешных submit | оценка |
| Retention Kafka (основной поток) | 48 часов | HLD §4 |
| Burst-требование | 2× steady-state, 3× burst-тест | HLD §23 |
| Время жизни сообщения | до 24 ч, 90% завершаются за 2 ч | HLD §4 |
| W_fast / W_slow (для Little's Law) | ~60с / ~1200с | как в v1 |
| DLR SLA (для `dlr_correlation`) | 4 часа | оценка |
| **Runtime Redis: операций на сообщение** | **11** (было 13) | пересчитано в §6.1 после удаления Billing `fetch_message_context` и per-message rate limit |
| **Runtime Redis: пропускная способность шарда** | **100 000 ops/s** (было 70 000) | пайплайнинг command batching вместо одиночных round-trip; CAS+deadline остаются в одном Lua-вызове |
| **Policy: throughput/ядро** | **6 000/с** (было 2 400) | per-partner компиляция правил вместо единого автомата |
| **Scheduler Critical Sweep: vCPU** | **флат ~8 vCPU**, не зависит от TPS | не подписан на stage-топики, нет embedded state |
| **Lifecycle Writer: входной поток** | **λ×3.1** (было λ×7) | без `stage.completed` |
| PostgreSQL: практическая пропускная способность writer'а | 60 000–100 000 строк/с | не изменилось — `message_read_model`/`message_lifecycle_history` никогда не питались от `stage.completed`, только от `message.lifecycle` |
| Kafka: пропускная способность партиции | ~8 000 msg/s | не изменилось |

---

# 2. Модель нагрузки и амплификации

Не изменилась в части бизнес-потока (см. v1 §2 для полной таблицы rate по топикам). Изменения — только в компонентах, которые сами читают эти потоки:

| Компонент | Было (v1) | Стало (v2) |
|---|---|---|
| Pipeline Engine (вход) | ≈100 000 событий/сек | без изменений |
| **Scheduler Critical** | ≈160 000 событий/сек (наблюдение за всеми stage-топиками) | **не масштабируется с TPS** — периодический poll Redis, не Kafka |
| Message State Resolver (вход) | ≈100 000 событий/сек | без изменений (по-прежнему нужна полная transactional валидация, не сокращали) |
| **Lifecycle Writer (вход)** | ≈140 000 событий/сек | **≈60 000–62 000 событий/сек** (`incoming` + `message.lifecycle`, без `stage.completed`) |
| Analytics Writer (вход) | ≈140 000 событий/сек | без изменений (единственный держатель полной пер-стадийной истории) |

---

# 3. Партиционирование Kafka — обновлено

Формула не изменилась:

```text
partitions(topic) = ceil( rate(topic) × burst_factor(3×) / per_partition_capacity(8 000 msg/s) )
```

Убран `scheduler.critical.state.changelog` (был 64 партиции — крупнейший топик в системе, поскольку Critical Sweep больше не имеет собственного changelog). Партиции `stage.*` топиков не изменились: они считаются от объёма **записи** в топик (produce rate), а не от того, кто их читает — то, что Critical Sweep больше не входит в consumer-список этих топиков, не меняет их сайзинг.

**Было:** 28 топиков, ≈330 партиций.
**Стало:** 27 топиков, **≈266 партиций** (−64, −19%). Полная таблица — `data_infrastructure_spec.md` §3.

---

# 4. Стоимость методов и пропускная способность на ядро — обновлено

| Сервис | Метод (из `service_internal_methods.md`) | v1: throughput/ядро | v2: throughput/ядро | Что изменилось |
|---|---|---|---|---|
| REST Receiver | `validate_request_schema`+`build_ack_response` | 7 500/с | 7 500/с | без изменений |
| Partner SMPP Gateway | `validate_submit_pdu`+`send_deliver_sm` | 10 800/с | 10 800/с | без изменений |
| Pipeline Engine | `cas_transition_and_track_deadline`+`resolve_next_stage` | 5 500/с | 5 500/с | Lua-вызов чуть тяжелее (CAS+ZADD/ZREM в одном скрипте), но это серверная нагрузка на Redis, не клиентский CPU Pipeline Engine — throughput/ядро сервиса не меняется |
| **Policy** | `check_sender_rules`+... (per-partner ruleset) | 2 400/с | **6 000/с** | scoped-компиляция правил |
| Billing | `resolve_tariff`+`apply_atomic_charge` | 4 000/с | 4 000/с | не читает `msgctx`, но это Redis-нагрузка, не CPU самого Billing |
| Routing | `select_operator`+`filter_by_control_state` | 13 000/с | 13 000/с | без изменений |
| Delivery | `segment_message`+`call_submit` | 4 000/с | 4 000/с | без изменений |
| **Scheduler Critical Sweep** | `tick_sweep`+`publish_retry`/`publish_timeout_result` | — (было 12 000/с событий у Kafka Streams Critical Lane) | **не throughput-модель** — флат ~8 vCPU независимо от TPS (poll, не consume) | заменена архитектура |
| Message State Resolver | `validate_transition`+`commit_transaction` | 10 000–12 000/с | 10 000–12 000/с | не оптимизировали — нужна полная валидация каждого события |
| DLR Manager | `lookup_correlation` | 6 000/с | 6 000/с | без изменений |
| Partner Notification Service | `resolve_delivery_channel`+`send_*` | 6 000/с | 6 000/с | без изменений |
| Lifecycle Writer / Analytics Writer | `batch_buffer`+`flush_batch` | ограничение Postgres/ClickHouse | ограничение Postgres/ClickHouse, но **входной поток Lifecycle Writer вдвое меньше** | меньше инстансов при том же ограничении на строку |

---

# 5. Сайзинг инстансов и серверов — обновлено

Формулы и стандартные размеры инстансов — как в v1 (Rust 4vCPU/8GB, Java-сессионные 8vCPU/16GB, Go 2vCPU/4GB).

```text
cores_needed(3×) = event_rate_peak × 3 / throughput_practical(core)
```

| Сервис | v1: vCPU @ 3× | v2: vCPU @ 3× | Δ |
|---|---|---|---|
| REST Receiver | 16 | 16 | — |
| Partner SMPP Gateway | 48 | 48 | — |
| Operator Session Manager | 24 | 24 | — |
| Pipeline Engine | 56 | 56 | — |
| **Policy** | 28 | **12** | **−16** |
| Billing | 24 | 24 | — |
| Routing | 8 | 8 | — |
| Delivery | 24 | 24 | — |
| Delivery Reconciliation | 8 | 8 | — |
| **Scheduler Critical (было Kafka Streams) / Critical Sweep (Go)** | 40 | **8** | **−32** |
| Scheduler Standard Lane | 16 | 16 | — |
| Scheduler Background Lane | 16 | 16 | — |
| Message State Resolver | 32 | 32 | — |
| **Мелкие Go control-plane сервисы** (Execution Control, Config Service+workers, DLR Correlation Writer, Billing Outbox/Ledger Writer, Partner API, Backoffice API, Replay Service — суммарно) | ≈61 (по отдельности) | **≈40** (общий пул из 3-4 VM) | **−21** |
| DLR Manager | 12 | 12 | — |
| Billing Reconciliation (Java) | 16 | 16 | — |
| **Lifecycle Writer** | 12 | **8** | **−4** |
| Analytics Writer | 16 | 16 | — |
| Notification | 24 | 24 | — |

**Инфраструктура** (Kafka, все три Redis-кластера, PostgreSQL, ClickHouse, Observability, LB/UI) — формулы не менялись, но Runtime Redis даёт наибольший вклад в итоговую разницу (см. §6.1).

## Итоговая таблица по всей платформе — от 1 000 до 20 000 msg/с

Пересчитано формулой (§6, `capacity_model.md` v1) с обновлёнными коэффициентами:

```text
K' = 0.0029623   (было 0.003984 — без Critical Lane, с новым Policy)
fixed_always = 80 vCPU   (было 72 — плюс флат 8 vCPU Critical Sweep)
Go_increment(λ) = ceil(λ×3.1/100000)×8   (было λ×7/100000 — без stage.completed у Lifecycle Writer)
RuntimeRedis(λ,margin) = ceil(λ×11×margin/100000)×16   (было λ×13×margin/70000×16)
variable_compute(λ,margin) = λ×margin×K'
```

| TPS | v1: vCPU (запас ×1.5) | v2: vCPU (запас ×1.5) | v1: vCPU (burst ×3) | v2: vCPU (burst ×3) |
|---|---|---|---|---|
| 1 000 | 190 | 193 | 196 | 197 |
| 2 000 | 196 | 197 | 224 | 206 |
| 3 000 | 202 | 202 | 236 | 215 |
| 5 000 | 238 | 231 | 284 | 269 |
| 7 500 | 277 | 258 | 354 | 307 |
| 10 000 | 316 | 293 | 424 | 369 |
| 13 000 | 382 | 354 | 524 | 444 |
| 15 000 | 418 | 363 | 572 | 462 |
| 17 500 | 469 | 410 | 654 | 536 |
| 20 000 | 500 | **437** (−13%) | 715 | **574** (−20%) |

**Честное наблюдение:** на очень низком TPS (1 000) экономия почти нулевая, местами v2 даже на 1-3 vCPU больше v1 — потому что флат-стоимость Critical Sweep (8 vCPU) в этой точке сопоставима с тем, что Critical Lane стоил бы при таком низком трафике (при 1000 TPS его нагрузка и так была маленькой). Оптимизации окупаются пропорционально трафику — при 20 000 они дают −13%…−20%, при 1 000 — практически ничего. Это ожидаемо: почти весь выигрыш — от компонентов, которые раньше масштабировались с TPS (Redis ops, Policy CPU, Critical Lane), а на низком TPS они и так были дёшевы.

---

# 6. Узкие места — обновлено, порядок сменился

## 6.1. Runtime Redis — по-прежнему важно, но заметно легче

```text
ops/сообщение = 11 (было 13: убрали Billing fetch_message_context и per-message rate limit)

ops(peak)  = 20 000 × 11 = 220 000 ops/s
ops(3×)    = 660 000 ops/s
shards_needed = 660 000 / 100 000 (было 70 000, пайплайнинг) = 6.6 → 7 шардов (было 12)
```

**Было:** 12 шардов на burst. **Стало:** 7 шардов. Существенное снижение, но природа риска та же — это по-прежнему компонент, от которого зависят почти все hot-path сервисы, и любое добавление нового потребителя Runtime Redis должно пересчитываться здесь в первую очередь.

## 6.2. PostgreSQL — теперь фактически риск №1

Нагрузка записи **не изменилась** — 120 000 rows/s на пике, как в v1. `message_read_model` и `message_lifecycle_history` никогда не питались от `stage.completed` (это было уточнено, а не изменено, см. `hld.md` §18) — поэтому оптимизация Lifecycle Writer снизила его собственную вычислительную нагрузку (меньше событий из Kafka разбирать), но не снизила объём записи в PostgreSQL.

Поскольку Runtime Redis (§6.1) заметно облегчился, а PostgreSQL остался на том же уровне — **PostgreSQL primary на запись теперь самая напряжённая точка модели**, а не вторая. Рекомендации из v1 (NVMe под WAL, минимум индексов на `message_lifecycle_history`/`dlr_correlation`, сниженный fillfactor на `message_read_model`, выделенный writer/pool под `billing_ledger`) остаются в силе и становятся приоритетнее.

## 6.3. Объём `message_lifecycle_history` — без изменений

Вывод v1 не меняется: держать эту таблицу в PostgreSQL дольше 48–72 часов нецелесообразно, retention должен быть коротким, длинная история — в ClickHouse. См. v1 §6.3 (не пересчитывалось, компонент не затронут оптимизацией).

## 6.4. Контрактные `tps_limit` операторов — без изменений

Внешний, инженерно нерешаемый потолок, как в v1 §6.4.

## 6.5. Kafka — по-прежнему не бутылочное горлышко, и стало легче

266 партиций вместо 330, суммарная нагрузка на брокеры пропорционально ниже. Ещё дальше от риска, чем в v1.

## 6.6. Billing Redis — без изменений

Не входил в объём этой оптимизации (Billing по-прежнему делает атомарные Lua-операции для баланса — то, что убрали, это отдельное чтение `msgctx`, которое и так шло в Runtime Redis, не в Billing Redis).

---

# 7. Little's Law — без изменений

Формулы и результат (§7 v1) не зависят от оптимизаций в этом раунде — L определяется λ и временем обработки сообщения (W), а не деталями реализации Scheduler/Redis. Полный расчёт — см. предыдущую версию: L_peak ≈ 3.48 млн одновременных исполнений, память Runtime Redis для `exec`/`msgctx` ≈ единицы ГБ (не проблема), объём `dlr_correlation` за 4-часовое окно ≈ 47–72 ГБ.

Дополнительно: `deadlines:{bucket}` sorted set в Runtime Redis имеет тот же порядок величины записей, что и `exec:{message_id}` (одна запись на активное исполнение стадии) — несколько миллионов элементов, при типичном размере записи sorted set (score + member id, десятки байт) это единицы-десятки МБ суммарно по всем bucket'ам. Не влияет на вывод о том, что Runtime Redis ограничен по ops/сек, а не по памяти.

---

# 8. Латентность — обновлено на один пункт

Как в v1 §8, с одним уточнением: обнаружение таймаута стадии больше не мгновенное (событие), а с точностью до интервала опроса Critical Sweep (**~1 секунда**). Для end-to-end латентности сообщения (доминирует ожидание DLR — секунды/минуты) это несущественно, но стоит явно внести в SLO как отдельный пункт: «просроченный дедлайн обнаруживается и обрабатывается в течение ≤ interval + обработка найденных записей», а не «мгновенно».

---

# 9. Доступность и error budget — обновлено

Одно изменение в характере рисков: Critical Sweep, в отличие от прежнего Kafka Streams Critical Lane, не имеет embedded state и changelog — значит, у него **нет времени восстановления, пропорционального объёму состояния** (раньше recovery после падения зависел от размера RocksDB/changelog; теперь Critical Sweep просто продолжает опрашивать тот же Redis sorted set с нуля). Это делает Critical Sweep даже более надёжным компонентом, чем был Critical Lane — стоит явно отметить как побочный положительный эффект оптимизации, а не только экономию vCPU.

Остальная таблица (§9 v1) не меняется: Kafka и Runtime Redis остаются истинными SPOF-категориями, PostgreSQL — условный SPOF на запись.

---

# 10. Пороги деградации — без изменений

Framework (§10 v1) не зависит от этой оптимизации.

---

# 11. Итоговый вывод — обновлён порядок

Три точки, где система деградирует раньше остальных, **по убыванию значимости, после оптимизации**:

1. **PostgreSQL primary (запись)** — поднялся на первое место. Нагрузка не изменилась (120 000 rows/s на пике, у самого практического предела), а остальные узкие места вокруг него полегчали, поэтому относительная значимость выросла. Требует вертикального масштабирования primary и уже сформулированного явного решения по ретеншну `message_lifecycle_history` (§6.3).
2. **Runtime Redis** — снизился со ставки «нужно 12 шардов» до «нужно 7 шардов» за счёт пайплайнинга и сокращения числа операций на сообщение. По-прежнему требует отдельного пересчёта при любом новом hot-path потребителе.
3. **Контрактные `tps_limit` операторов** — не изменился, внешний потолок вне инженерного контроля.

Дополнительный эффект оптимизации, не входящий в тройку риска, но заметный по деньгам: суммарная стоимость платформы на пике burst снизилась с 715 до **574 vCPU (−20%)**, при этом операционная сложность тоже упала — убрана целая Kafka Streams/RocksDB группа (Critical Sweep) и самый большой топик в системе (`scheduler.critical.state.changelog`, 64 партиции).

---

# 12. v3 — основа мультиканальности и протокольная двойственность (Destination Resolution + Operator HTTP Gateway)

Два новых компонента добавлены в архитектуру (HLD, `services_specifictaion.md`). Эффект на капасити-модель — точечный, не требует пересчёта всей кривой из §5, только двух узлов.

## 12.1. Pipeline Engine — пятая стадия

Событийный поток Pipeline Engine растёт: теперь 5 обязательных стадий (Destination Resolution добавлена первой) вместо 4.

```text
20 000 × (1 incoming + 5 completion) ≈ 120 000 событий/сек   (было 100 000)

cores(3×) = 120 000 × 3 / 5 500 ≈ 65.5 → 66 cores → 17 инстансов × 4vCPU ≈ 68 vCPU   (было 56)
```

**Δ Pipeline Engine: +12 vCPU на burst.**

## 12.2. Destination Resolution Service — новый узел

По профилю идентичен Routing (лёгкое вычисление, без внешних вызовов, throughput 13 000/с/ядро):

```text
cores(3×) = 20 000 × 3 / 13 000 ≈ 4.6 → 5 cores → 2 инстанса × 4vCPU = 8 vCPU
```

Столько же на запасе ×1.5. **+8 vCPU (burst), +8 vCPU (запас ×1.5).**

## 12.3. Operator HTTP Gateway — новый узел, реальный размер неизвестен

В отличие от Destination Resolution, у этого сервиса нет твёрдой оценки нагрузки — она полностью зависит от того, сколько реальных операторов используют HTTP вместо SMPP, что пока не определено (бизнес-решение, не инженерное). Закладываю **плейсхолдер HA-минимума**, не throughput-расчёт:

```text
2 инстанса × 4vCPU (Go, легче Java SMPP-версии) = 8 vCPU
```

**Требует пересчёта, как только будет известен реальный протокольный микс операторского портфеля** — если большинство операторов пойдёт через HTTP, этот узел по нагрузке приблизится к текущему Operator SMPP Session Manager (24 vCPU на burst), не останется на уровне HA-минимума.

## 12.4. Обновлённый итог

| | v2 (было) | v3 (стало) | Δ |
|---|---|---|---|
| Запас ×1.5 (20k) | 437 vCPU | ≈ **453 vCPU** | +16 (Destination Resolution +8, Pipeline Engine рост события ≈+8 на этом margin) |
| Burst ×3 (20k) | 574 vCPU | ≈ **602 vCPU** | +28 (Pipeline Engine +12, Destination Resolution +8, Operator HTTP Gateway-плейсхолдер +8) |

Рост небольшой и ожидаемый — закладка основы мультиканальности стоит одну лёгкую Rust-стадию и один HA-минимум Go-сервиса, а не архитектурного пересмотра. Три риска из §11 (PostgreSQL, Runtime Redis, `tps_limit` операторов) не меняются — оба новых узла не пишут в PostgreSQL, не добавляют значимой нагрузки на Runtime Redis (по одному-два обращения на сообщение, тот же характер, что у остальных лёгких стадий) и не имеют собственного внешнего лимита.
