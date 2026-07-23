# Спецификация сервисов A2P Message Processing Platform

**Версия:** 1.0
**Основание:** утверждённая HLD
**Целевая нагрузка:** до 20 000 входящих сообщений/с и свыше 100 000 внутренних событий/с

---

## 1. Принцип выбора технологий

Несмотря на то что значительную часть кода будет писать ИИ, не следует выбирать отдельный язык для каждого сервиса. Генерация кода упрощается, но эксплуатация, профилирование, расследование ошибок, обновление зависимостей и обучение разработчиков остаются человеческими задачами.

Для платформы фиксируются **три backend-стека**:

### Rust — высоконагруженный Data Plane

Используется там, где важны:

* предсказуемая задержка;
* низкое потребление памяти;
* высокая скорость обработки Kafka;
* CPU-intensive проверки;
* отсутствие runtime GC.

Базовый набор:

```text
Rust stable
Tokio
Axum / Hyper
Tonic gRPC
rust-rdkafka
redis-rs
Serde
Prost / Protobuf
OpenTelemetry
```

Tokio предоставляет асинхронные сетевые операции, планировщик и таймеры, а `rust-rdkafka` построен поверх `librdkafka`, поддерживает асинхронную работу и Kafka transactions. ([tokio.rs][1])

### Java — SMPP и stateful Kafka processing

Используется там, где важны:

* постоянные TCP-сессии;
* сложное протокольное состояние;
* Kafka transactions;
* RocksDB state stores;
* changelog и восстановление после rebalance;
* финансовая консистентность.

Базовый набор:

```text
OpenJDK 25 LTS
Netty
Kafka Streams / Kafka Client
RocksDB
Micronaut для bootstrap, DI и health endpoints
Lettuce Redis
jOOQ
gRPC Java
OpenTelemetry / Micrometer
```

JDK 25 является LTS-релизом для большинства поставщиков. Kafka Streams предоставляет state stores, changelog topics и режим `exactly_once_v2`, поэтому он предпочтительнее самостоятельной реализации transactional stateful processing. ([openjdk.org][2])

### Go — Control Plane, API и пакетные writers

Используется для:

* CRUD и административных API;
* batch-записи;
* проекций;
* интеграционных workers;
* сервисов с преимущественно I/O-нагрузкой.

Базовый набор:

```text
Go 1.26.x
net/http + chi
franz-go
pgx
clickhouse-go
grpc-go
Protobuf
OpenTelemetry
```

Go 1.26 является текущей стабильной веткой на момент подготовки документа. `franz-go` поддерживает production/consumption, transactions и Kafka administration; `pgx` и `clickhouse-go` являются специализированными драйверами PostgreSQL и ClickHouse. ([Go Dev][3])

---

# 2. Сервисы Data Plane

## 2.1. Partner REST Receiver

**Назначение**

* приём REST-запросов;
* протокольная валидация;
* аутентификация партнёра;
* IP/application checks;
* admission control и rate limit;
* генерация `message_id` и `trace_id`;
* публикация `incoming.messages`;
* возврат ACK после Kafka acknowledgement.

**Стек**

```text
Rust
Tokio
Axum / Hyper
rust-rdkafka
redis-rs
Protobuf
OpenTelemetry
```

**Причина выбора**

Сервис выполняет простой, но крайне горячий сетевой поток. Rust обеспечивает низкие накладные расходы и предсказуемую latency без GC.

**Состояние**

Stateless. Rate limit — локальный token bucket в памяти инстанса с периодической синхронизацией счётчика в Runtime Redis (раз в секунду), а не Redis-запрос на каждое сообщение — точность лимита достаточная (допустимо кратковременное превышение на 10-20% в пределах инстанса), а нагрузка на Redis на два порядка меньше. Execution-control и партнёрская конфигурация (credentials, IP allowlist, приложения) — в локальном immutable snapshot, построенном из `config.changes` и `execution.control` (см. HLD §16, §8).

---

## 2.2. Partner SMPP Gateway

**Назначение**

* SMPP bind/unbind партнёров;
* `submit_sm`;
* `deliver_sm`;
* partner-side `query_sm`;
* `enquire_link`;
* SMPP window и sequence numbers;
* регистрация активных сессий;
* адресная доставка уведомлений в конкретную сессию.

**Стек**

```text
Java 25
Netty
Внутренняя SMPP 3.4 library
Kafka Client
Lettuce Redis
gRPC Java
Protobuf
OpenTelemetry
```

Netty предназначен для разработки асинхронных event-driven протокольных серверов и клиентов, поэтому лучше подходит для долгоживущих SMPP-соединений, чем обычный HTTP-фреймворк. ([Netty][4])

**SMPP-библиотека**

Не рекомендуется делать критический Gateway полностью зависимым от старой сторонней SMPP-библиотеки. Следует реализовать внутренний модуль:

```text
smpp-codec
smpp-pdu-model
smpp-session
smpp-window
smpp-timers
smpp-testkit
```

JSMPP и существующие библиотеки можно использовать как референс и для cross-validation, но сетевой слой строится на Netty.

**Состояние**

StatefulSet. TCP socket остаётся внутри экземпляра. Session registry хранится в Runtime Redis. Rate limit — тот же локальный token bucket с периодической синхронизацией, что у Partner REST Receiver, а не Redis-запрос на каждый `submit_sm`. Партнёрская конфигурация — в локальном immutable snapshot, построенном из `config.changes` (см. HLD §16), тем же способом, что и у Partner REST Receiver.

---

## 2.3. Operator SMPP Session Manager

**Назначение**

* SMPP binds с операторами;
* reconnect;
* `enquire_link`;
* SMPP window;
* TPS/throttling;
* отправка `submit_sm`;
* получение `submit_sm_resp`;
* приём DLR;
* опциональный `query_sm`;
* приоритет `submit_sm` над `query_sm`.

**Стек**

Такой же, как у Partner SMPP Gateway:

```text
Java 25
Netty
Общая внутренняя SMPP library
Kafka Client
gRPC Java
Protobuf
OpenTelemetry
```

**Причина выбора**

Оба SMPP-компонента должны использовать один codec, PDU model, механизм windowing и общий SMPP testkit. Разные реализации SMPP на стороне партнёров и операторов приведут к расхождению поведения.

**Состояние**

Stateful. Поддерживает несколько bind на оператора и отдельный bind для `query_sm`, если оператор его предоставляет.

Каждый bind принадлежит одному экземпляру и регистрируется в Runtime Redis (`operator_id`, `route_id`, `protocol=SMPP`, `owning_instance_id`, `endpoint`, `route_epoch`, `heartbeat` — см. HLD §11.4, общий registry с Operator HTTP Gateway). Это тот же принцип instance ownership, что и у Partner SMPP Gateway: `tps_limit`/`max_window` — локальный инвариант владеющего инстанса, а не предмет распределённой координации.

Операторские параметры (`max_window`, `tps_limit`, `bind_count`, `dlr_supported`, `query_sm_*` и т.д.) доставляются через `config.changes` в локальный immutable snapshot — тем же механизмом, что и партнёрская конфигурация у Partner REST Receiver / Partner SMPP Gateway.

---

## 2.3a. Operator HTTP Gateway

**Назначение**

* исходящий submit по HTTP к операторам, принимающим HTTP API вместо SMPP;
* приём DLR через входящий webhook от оператора (валидация подписи/токена);
* throttling в рамках `tps_limit`, закреплённого за владеющим инстансом;
* reconnect/retry на уровне HTTP-клиента при временной недоступности оператора;
* публикация в те же `operator.submit.accepted`/`operator.dlr`, что и SMPP-версия — downstream (DLR Manager, DLR Correlation Writer) не различает протокол.

**Стек**

```text
Go 1.26
net/http + chi (входящий webhook)
grpc-go (внутренний RPC от Delivery)
Стандартный HTTP-клиент с retry/backoff
go-redis
Protobuf
OpenTelemetry
```

**Причина выбора**

В отличие от SMPP, у HTTP нет постоянного сокета и, на первый взгляд, сервис мог бы быть полностью stateless с обычной балансировкой. Сознательно выбрана та же модель владения маршрутом, что у SMPP (HLD §11.4, единый Operator Route Registry) — иначе `tps_limit` становится задачей распределённой координации между независимыми репликами, а этой сложности мы уже избежали для SMPP. Сама протокольная часть (HTTP-клиент, webhook-приёмник) не требует Netty-уровня сложности SMPP-кодека, поэтому сервис на Go, а не в `smpp-platform-java` — общий с SMPP-версией только паттерн владения (registry в Runtime Redis), не код.

**Состояние**

Stateful (владение маршрутом), но не session-heavy — не держит постоянного соединения, только exclusive-право на отправку по конкретному `(operator_id, route_id)` и приём для него DLR-вебхуков.

---

## 2.4. Pipeline Engine

**Назначение**

* обработка `incoming.messages`;
* обработка `stage.completed`;
* выбор Pipeline и его версии;
* определение следующего узла;
* проверка execution control;
* атомарный переход Execution State;
* публикация stage-команды.

**Стек**

```text
Rust
Tokio
rust-rdkafka
redis-rs
ArcSwap для immutable snapshots
Protobuf
OpenTelemetry
```

**Причина выбора**

Pipeline Engine обрабатывает около 120 000 входящих внутренних событий в секунду при внешнем TPS 20 000 (5 обязательных стадий, включая Destination Resolution). Логика сервиса короткая и хорошо подходит для Rust: deserialize → локальное решение → Redis CAS → Kafka publish.

**Состояние**

Сам сервис stateless. Долговременное Execution State хранится в Runtime Redis. Конфигурация хранится в immutable in-process snapshots.

---

## 2.4a. Destination Resolution Service

**Назначение**

* резолв оператора по номерному диапазону MSISDN — первая стадия pipeline, обязательный предшественник Policy (HLD §5.3), т.к. правила сравнения с шаблонами зависят от оператора;
* channel-нейтральное имя намеренно: для email это был бы резолв ESP/MX, для push — платформы (APNs/FCM) по типу токена устройства — не специфицируется в этой версии, но контракт (стадия `Destination Resolution`, результат `resolved operator_id`) уже не привязан к SMS-терминологии;
* публикация результата как обычной stage-команды в `stage.completed`.

**Стек**

```text
Rust
Tokio
rust-rdkafka
ArcSwap для immutable snapshots (номерной диапазон)
Protobuf
OpenTelemetry
```

**Причина выбора**

По профилю нагрузки идентичен Routing — лёгкое, чисто вычислительное сопоставление без внешних вызовов, использует тот же Rust Stage SDK.

**Состояние**

Stateless относительно сообщений. Таблица номерных диапазонов (MSISDN prefix → `operator_id`) — в локальном immutable snapshot, обновляется через `config.changes`, как и остальная конфигурация. Live HLR-резолв для случаев переносимости номера (MNP) не входит в первую версию (HLD §26) — задокументированное упрощение.

---

## 2.5. Policy Service

Policy — не одна проверка, а последовательность независимых под-проверок с разным уровнем конфигурации (партнёр / оператор / абонент / платформа). Детальный дизайн (порядок проверок, структура данных, устройство мини-движка шаблонов) — отдельная тема; здесь фиксируются базовые требования и решение по хранению.

**Назначение**

1. **Template matching + категоризация** — мини-движок внутри Policy. У каждого шаблона есть категория; движок определяет подходящий шаблон и присваивает сообщению категорию. Синтаксис плейсхолдеров (`%w`, `%d{n,m}`) и алгоритм матчинга (гибрид Aho-Corasick для литеральных фрагментов + точечная проверка плейсхолдеров) — `data_infrastructure_spec.md` §1.9b. Плейсхолдеры сознательно **без ограничения длины/символов** — контентные проверки (банворды, п.5) применяются к полному нормализованному тексту независимо от структуры, поэтому пермиссивность не открывает обход модерации, только определяет структурный матч. Правила сравнения с шаблонами различаются **по оператору** (уже известен на входе Policy благодаря Destination Resolution — HLD §5.3). Поведение при отсутствии совпадения (`REJECTED` с категорией `BLOCKED`, либо категория `UNTEMPLATED` и продолжение по pipeline) конфигурируется **по партнёру**.
2. **Anti-spam по частоте** — не заваливать абонента рекламным трафиком; стейтфул, нужен счётчик по абоненту (частота сообщений категории за окно времени).
3. **Time-of-day по категории** — например категория "Реклама" не может быть отправлена абоненту в заданный ночной интервал.
4. **Consent-блэклист по категории** — абонент запросил отключение категории (например "Реклама"), сообщения этой категории ему больше не доставляются.
5. **Банворды с нормализацией обхода** — реальный список смешивает узбекскую латиницу, русскую кириллицу и английский в одном списке (например `qotaq`, `мудак`, `idiot`) — нормализация должна работать мультиязычно (unicode NFKC, схлопывание кириллица↔латиница гомоглифов, снятие разделителей), не для одного алфавита. Подробнее — `data_infrastructure_spec.md` §1.9b.
6. **Consent-блэклист по отправителю** — абонент может заблокировать рассылки от конкретного sender_id, независимо от категории.
7. **Sender validation** — легитимность sender_id для партнёра/оператора (security/compliance, не зависит от абонента).
8. Аналогичные проверки для будущих каналов (email/push) — состав может отличаться, конфигурация с первого дня скоуплена по `channel` (HLD §3.1.1), даже когда заполнен только `SMS`.

Все восемь конфигурируются на нескольких уровнях: партнёр, оператор, абонент, и, где применимо, платформенный mandatory-уровень (например банворды, обязательные по законодательству, не отключаемые партнёром) — модель конфигурации уточняется отдельно.

**Отклонение по любой из проверок 1, 2 (throttle), 3, 4, 5, 6, 7 — единообразно `REJECTED` с категорией `BLOCKED`.** Не разные категории на разные причины — по решению, зафиксированному вместе с бизнесом: одна цена независимо от того, что именно сработало. `REJECTED` не завершает pipeline — Billing тарифицирует и это (HLD §5.3.1, §15.0), только после Billing pipeline останавливается, не доходя до Routing/Delivery. Policy `aggregate_result` поэтому **всегда** возвращает непустую `category` — либо из совпавшего шаблона, либо `UNTEMPLATED`, либо `BLOCKED`, никогда не оставляет её пустой.

**Стек**

```text
Rust
Tokio
rust-rdkafka
redis-rs (anti-spam счётчики, consent-блэклисты — см. ниже)
Aho-Corasick
regex-automata
Trie / hash indexes
Protobuf
OpenTelemetry
```

**Причина выбора**

Policy — CPU- и memory-intensive сервис, может работать с сотнями тысяч шаблонов. Rust позволяет заранее компилировать правила в компактные структуры и безопасно переключать immutable snapshots без паузы GC.

**Состояние и хранение шаблонов — решение**

Оценка порядка величины: даже 100 000–1 000 000 шаблонов — это 10–100 МБ сырого текста, скомпилированный Aho-Corasick — обычно ×3-5 от этого, то есть 30–500 МБ на инстанс. При инстансах 4 vCPU/8 ГБ это некритично по памяти. **Память не является причиной выносить хранение шаблонов в БД.**

Реальная проблема — стоимость пересборки при точечном изменении, если снапшот монолитный. Решение остаётся в рамках уже принятого паттерна `config.changes` + local snapshot, без БД в hot path:

* правила компилируются **per-partner** (см. ниже про throughput);
* снапшот — не один блок, а **карта `partner_id → скомпилированный автомат`** со структурным разделением (persistent/immutable map с structural sharing, например `im`-подобная структура в Rust, свопаемая через `ArcSwap` на уровне карты) — изменение шаблонов одного партнёра пересобирает только его запись, не весь платформенный ruleset;
* consent-блэклисты (пп. 4, 6) и anti-spam счётчики (п. 2) — не часть immutable snapshot (они меняются по действию абонента/трафику, не по релизу конфигурации) — хранятся в Runtime Redis, читаются на каждое сообщение через `redis-rs`.

**Путь масштабирования, если реальный объём окажется на порядок больше** (не сотни тысяч, а десятки миллионов шаблонов): партиционировать `stage.policy` с учётом принадлежности партнёру (не только `message_id`) и закрепить группы партнёров за конкретными Policy-инстансами — аналогично тому, как разделены партнёрские SMPP-сессии. Тогда каждый инстанс держит в памяти не весь платформенный ruleset, а только свою часть. Это документированный путь, не реализуется в первой версии.

**Чего сознательно избегаем:** запроса к БД на каждое сообщение. Это разрушило бы весь смысл per-partner компиляции — переход от 2 400 к 6 000 сообщ/сек/ядро обоснован тем, что Policy — чистое in-memory вычисление без ожидания I/O (см. `capacity_model.md` §4). БД-запрос на сообщение вернул бы Policy к задержке уровня Redis RTT и хуже.

---

## 2.6. Billing Service

Тариф — за сегмент, по категории из `PolicyResult.category` (в т.ч. служебные `UNTEMPLATED`/`BLOCKED` — Billing вызывается и для отклонённых Policy сообщений, HLD §5.3.1, §15.0). Структура тарифа — `data_infrastructure_spec.md` §1.6a. `resolve_tariff` — чистый lookup `(partner_id, category) → price_per_segment × segment_count`, без бизнес-логики о том, продолжать ли pipeline дальше — это решает граф Pipeline Engine, не Billing.

**Назначение**

* определение тарифа (по категории и числу сегментов);
* postpaid/cold billing;
* prepaid/hot billing;
* проверка `account_state`;
* проверка `account_epoch`;
* идемпотентная операция по `charge_id`;
* запись financial outbox.

**Стек**

```text
Java 25
Micronaut
Kafka Client
Lettuce Redis
Protobuf
OpenTelemetry
```

Для атомарных операций используются Redis Functions или Lua scripts. Billing Service не пишет ledger напрямую в PostgreSQL.

**Причина выбора**

Финансовая логика требует хорошей поддержки типов, тестируемости, зрелых Redis/Kafka-клиентов и понятной модели исключений. Java подходит здесь лучше, чем ручная реализация финансового state machine на нескольких низкоуровневых Rust-библиотеках.

**Состояние**

* оперативный баланс — Billing Redis;
* финансовый source of truth — PostgreSQL ledger;
* `charge_id = stage_execution_id`.

Тело сообщения для тарификации не запрашивается отдельно из Runtime Redis — `segment_count`/длина уже приходят полем в `BillingExecute` (посчитаны один раз Pipeline Engine при кэшировании контекста), в отличие от Policy и Delivery, которым нужен полный body.

---

## 2.7. Routing Service

Оператор уже известен на входе (резолвлен Destination Resolution, HLD §5.3) — Routing больше не выбирает оператора «с нуля», а подбирает конкретный маршрут и протокол внутри уже известного оператора.

**Назначение**

* выбор маршрута **и протокола** (SMPP/HTTP) внутри уже известного оператора;
* применение route/operator control;
* failover между primary/reserve маршрутом (может быть тот же протокол — например второй SMPP bind — либо другой, например HTTP primary + SMPP reserve);
* фиксация `route_version`, `route_id`, `protocol`;
* контроль доступной route capacity.

**Стек**

```text
Rust
Tokio
rust-rdkafka
Protobuf
OpenTelemetry
```

**Причина выбора**

Routing является быстрым вычислительным этапом и может использовать тот же Rust Stage SDK, что Pipeline и Policy.

**Состояние**

Stateless. Маршруты и operational state хранятся в локальных immutable snapshots.

---

## 2.8. Delivery Service

**Назначение**

* подготовка операторского submit (SMPP `submit_sm` либо HTTP-эквивалент — по `protocol` из результата Routing);
* сегментация сообщения;
* создание `queue_msg_id`;
* резолв владеющего инстанса через registry в Runtime Redis (`operator_id` + `route_id` из результата Routing) — **protocol-aware dispatch**: `protocol=SMPP` → Operator SMPP Session Manager, `protocol=HTTP` → Operator HTTP Gateway;
* вызов выбранного шлюза;
* обработка результата submit (`submit_sm_resp` для SMPP, HTTP-ответ для HTTP);
* формирование `SUBMITTED`, `FAILED` или `SUBMISSION_OUTCOME_UNKNOWN`.

**Стек**

```text
Java 25
Micronaut
Kafka Client
gRPC Java
Protobuf
OpenTelemetry
```

**Причина выбора**

Delivery тесно взаимодействует с Java/Netty Operator SMPP Session Manager при `protocol=SMPP`, и с Go Operator HTTP Gateway при `protocol=HTTP` — оба через один и тот же registry, поэтому логика резолва инстанса не зависит от протокола. Одинаковая модель типов и gRPC-контрактов с SMPP-версией упрощает обработку SMPP sequence, late responses и ambiguous outcomes для SMPP-ветки.

**Состояние**

Stateless. Перед вызовом выбранного шлюза (SMPP или HTTP) дополнительно проверяет `execution.control` для `OPERATOR_ROUTE` — не полагается только на admission-проверку, выполненную Pipeline Engine на диспетчеризации, поскольку между диспетчеризацией и фактическим submit сообщение могло провести время в Scheduler на retry (см. HLD §8).

---

## 2.9. Delivery Reconciliation Service

**Назначение**

* разрешение `SUBMISSION_OUTCOME_UNKNOWN`;
* ожидание позднего `submit_sm_resp`;
* ожидание DLR;
* обработка reconciliation deadline;
* резолв владеющего инстанса (SMPP или HTTP) через тот же registry, что использует Delivery;
* опциональный `query_sm`;
* формирование `DELIVERY_UNRESOLVED`.

**Стек**

```text
Java 25
Micronaut
Kafka Client
gRPC Java
jOOQ
PostgreSQL
Protobuf
OpenTelemetry
```

**Политика `query_sm`**

* выключен по умолчанию;
* не применяется при надёжном DLR;
* отдельный bind предпочтителен;
* при общем bind имеет меньший приоритет, чем `submit_sm`.

---

# 3. Stateful processing

## 3.1. Scheduler

Scheduler остаётся одной логической capability, но разворачивается как три отдельных deployment unit с **разной архитектурой** — это не три одинаковых lane, как в первой версии, а два разных подхода к хранению состояния, выбранных по фактической потребности каждого.

```text
scheduler-critical-sweep
scheduler-standard
scheduler-background
```

### Critical Sweep — пересмотрено, больше не Kafka Streams

* stage timeout;
* stage retry;
* Billing-critical deadlines.

В первой версии это была Kafka Streams-группа, читающая все stage-топики целиком (≈160 000 событий/сек на пике) ради отслеживания дедлайнов. Пересмотрено: дедлайн уже лежит в Runtime Redis как часть Execution State (Pipeline Engine пишет его туда тем же атомарным вызовом, которым делает CAS-переход — HLD §7.2, §9.1). Держать для этого отдельный embedded state store и Kafka changelog избыточно.

**Назначение**

* раз в ~1 секунду опрашивать шардированный Redis sorted set (`deadlines:{bucket}`) на просроченные записи;
* по найденной просрочке — читать `attempt`/retry-policy из Execution State и публиковать retry, `TIMED_OUT`/`RETRY_EXHAUSTED` или DLQ (поведение не изменилось, изменился только триггер);
* принимать ручные команды `FORCE_TIMEOUT`/`FORCE_RETRY` из `scheduler.critical.commands`.

**Стек**

```text
Go 1.26
franz-go (только для publish retry/timeout/DLQ)
go-redis
Protobuf
OpenTelemetry
```

**Причина выбора**

Сервис больше не делает transactional stateful stream processing — он периодически читает Redis и иногда публикует в Kafka. Это тот же профиль нагрузки, что у остального control-plane (низкочастотный, I/O-bound), поэтому нет причины держать под него Java, Kafka Streams и RocksDB. Собственного state store и changelog у сервиса нет — восстанавливать после рестарта нечего, авторитетные данные (дедлайн, попытка) всё время лежат в Runtime Redis.

### Standard Lane

* PAUSED hold;
* controlled release;
* reconciliation deadlines.

### Background Lane

* DLR correlation retry;
* notification retry;
* фоновые задачи.

**Стек Standard и Background Lane** (без изменений — здесь Kafka Streams обоснован: у обоих есть реальная transactional stateful нагрузка, инициируемая явной командой, а не наблюдением за всем потоком):

```text
Java 25
Kafka Streams Processor API
RocksDB
exactly_once_v2
Protobuf
OpenTelemetry
```

Каждый из двух lane имеет собственные:

* application ID;
* command topic;
* changelog;
* RocksDB directory;
* consumer group;
* ресурсы Kubernetes.

---

## 3.2. Message State Resolver

**Назначение**

* обработка `stage.completed`;
* обработка `delivery.status`;
* проверка допустимых status transitions;
* запрет регрессии;
* дедупликация;
* публикация `message.lifecycle`.

**Стек**

```text
Java 25
Kafka Streams
RocksDB state store
exactly_once_v2
Protobuf
OpenTelemetry
```

**Состояние**

Authoritative state находится в compacted Kafka changelog. RocksDB является локальной восстановимой проекцией.

Message State Resolver и Scheduler используют общий internal Java-модуль:

```text
stateful-processing-runtime
```

В нём находятся:

* именование changelog;
* transactional topology;
* restore listener;
* rebalance hooks;
* fencing;
* state migration;
* recovery metrics;
* fault-injection utilities.

---

# 4. Control Plane

## 4.1. Execution Control Service

**Назначение**

* вычисление `ACTIVE / DEGRADED / PAUSED`;
* гистерезис;
* dwell time;
* композиция scope;
* admission rate;
* dispatch rate;
* controlled ramp-up;
* partner billing freeze;
* manual override.

Stage Availability Controller является модулем этого же сервиса, а не отдельным deployment unit на первой версии.

**Стек**

```text
Go 1.26
franz-go
Prometheus HTTP API client
PostgreSQL
Protobuf
OpenTelemetry
```

**Причина выбора**

Сервис низкочастотный, I/O-oriented и содержит понятный control loop. Go обеспечивает простой deployment и удобную работу с конкурентными watchers.

---

## 4.2. Configuration Service

**Назначение**

* CRUD конфигурации;
* валидация;
* создание immutable version;
* запись configuration + outbox в одной транзакции;
* аудит изменений.

**Стек**

```text
Go 1.26
net/http + chi
pgx
PostgreSQL
Protobuf
OpenTelemetry
```

### Config Event Publisher

Отдельный worker из того же Go-репозитория:

```text
PostgreSQL outbox → config.changes
```

### Config Cache Projector

Отдельный worker:

```text
config.changes → Configuration Redis
```

### Consent Cache Projector

Отдельный worker, симметричен Config Cache Projector, но целевой кластер другой — читает те же события `config.changes` с фильтром `entity_type='subscriber_consent'` (источник — PostgreSQL `policy.subscriber_consent`, `data_infrastructure_spec.md` §1.9c) и проецирует **в Runtime Redis** (`consent:category_blacklist:{msisdn}`, `consent:sender_blacklist:{msisdn}`), а не в Configuration Redis — потому что Policy читает эти данные на каждое сообщение (hot path), а не только при bootstrap. При полной потере этой части Runtime Redis выполняет принудительный full resync из PostgreSQL, а не пассивное восстановление.

Все три компонента имеют общий доменный модуль конфигурации, но разные deployment unit.

---

# 5. DLR-сервисы

## 5.1. DLR Correlation Writer

**Назначение**

* чтение `operator.submit.accepted`;
* batch insert correlation;
* запись `operator_id + smsc_message_id → message_id`;
* управление временными партициями PostgreSQL.

**Стек**

```text
Go 1.26
franz-go
pgx
PostgreSQL COPY / batch
Protobuf
OpenTelemetry
```

---

## 5.2. DLR Manager

**Назначение**

* чтение raw operator DLR;
* нормализация операторских статусов;
* поиск correlation;
* публикация `delivery.status`;
* отправка unresolved correlation в Scheduler Background.

**Стек**

```text
Go 1.26
franz-go
pgx
Protobuf
OpenTelemetry
```

DLR Manager и Correlation Writer находятся в одном Go workspace, но запускаются отдельно.

---

# 6. Billing platform services

## 6.1. Billing Outbox Publisher

```text
Billing Redis Stream → billing.ledger
```

**Стек**

```text
Java 25
Lettuce
Kafka Client
Protobuf
OpenTelemetry
```

## 6.2. Billing Ledger Writer

```text
billing.ledger → PostgreSQL double-entry ledger
```

**Стек**

```text
Java 25
Kafka Client
jOOQ
PostgreSQL
Protobuf
OpenTelemetry
```

## 6.3. Billing Reconciliation

**Назначение**

* сравнение Redis balance и PostgreSQL ledger;
* freeze аккаунта;
* detect/classify/alert;
* fenced recovery;
* compensating entries.

**Стек**

```text
Java 25
Micronaut
Lettuce
jOOQ
PostgreSQL
Kafka Client
OpenTelemetry
```

Billing Service и эти три workers размещаются в одном Java multi-module repository, но имеют разные deployment unit и разные credentials.

---

# 7. Projection services

## 7.1. Lifecycle Writer

**Назначение**

* чтение `incoming.messages`;
* чтение `message.lifecycle`;
* чтение DLQ-топиков;
* запись operational read model;
* запись lifecycle history;
* запись DLQ record.

`stage.completed` сознательно не читается — детальная пер-стадийная история идёт только в ClickHouse через Analytics Writer (HLD §18). Это примерно вдвое снижает объём событий, который должен обработать Lifecycle Writer, и убирает риск случайно продублировать в PostgreSQL то, что уже есть в ClickHouse.

**Стек**

```text
Go 1.26
franz-go
pgx
PostgreSQL COPY / batch
Protobuf
OpenTelemetry
```

## 7.2. Analytics Writer

**Назначение**

* запись потока событий в ClickHouse;
* batch insert;
* подготовка аналитических таблиц;
* materialized views и агрегаты.

**Стек**

```text
Go 1.26
franz-go
clickhouse-go
Protobuf
OpenTelemetry
```

Lifecycle и Analytics Writers используют общий пакет декодирования событий, но разные consumer groups и deployment unit.

---

# 8. Partner и Management Plane

## 8.1. Partner Notification Service

**Назначение**

* чтение `message.lifecycle`;
* REST callbacks;
* WebSocket;
* маршрутизация SMPP `deliver_sm`;
* retry через Scheduler Background;
* notification TTL.

**Стек**

```text
Go 1.26
franz-go
grpc-go
net/http
Protobuf
OpenTelemetry
```

SMPP PDU самостоятельно не формирует. Он отправляет типизированную gRPC-команду Partner SMPP Gateway.

**Retry и исчерпание TTL**

При неудачной попытке доставки сервис публикует задачу `NOTIFICATION_RETRY` в `scheduler.background.commands`; Background Lane возвращает её в `notification.retry`, откуда сервис читает её снова — второго, самостоятельного механизма retry внутри сервиса нет. После исчерпания notification TTL push-попытки прекращаются без побочных эффектов: `message.lifecycle` уже содержит терминальный статус, и партнёр может получить его pull-запросом через Partner API в любой момент (см. HLD §19).

---

## 8.2. Partner API

**Назначение**

* статус сообщения;
* поиск сообщений;
* детализация;
* отчёты;
* partner-side `query_sm` через SMPP Gateway;
* загрузка доступной конфигурации.

**Стек**

```text
Go 1.26
net/http + chi
pgx
clickhouse-go
JWT / Keycloak validation
OpenTelemetry
```

PostgreSQL используется для оперативных запросов, ClickHouse — для отчётности.

---

## 8.3. Backoffice API

**Назначение**

* административный CRUD;
* поиск сообщений;
* управление партнёрами и операторами;
* управление pipeline;
* execution control;
* billing operations;
* просмотр DLQ;
* reconciliation cases;
* отчётность.

**Стек**

```text
Go 1.26
net/http + chi
pgx
clickhouse-go
Keycloak OIDC
OpenTelemetry
```

---

## 8.4. Replay Service

**Назначение**

* безопасный replay DLQ;
* проверка TTL;
* проверка `stage_execution_id`;
* проверка Billing side effect;
* проверка Delivery ambiguity;
* аудит операции.

**Стек**

```text
Go 1.26
franz-go
pgx
Protobuf
OpenTelemetry
```

Replay Service запускается отдельно от Backoffice API и использует отдельные Kafka ACL. Backoffice только создаёт и подтверждает replay-команду.

Источник данных для replay — не Kafka DLQ-топики напрямую, а PostgreSQL: Lifecycle Writer уже сохраняет туда полный DLQ record (исходная stage-команда + причина), и Replay Service читает его оттуда через `pgx`. Туда же пишется журнал каждого replay-действия (кто, когда, `stage_execution_id`, результат проверок) — см. HLD §20.

---

## 8.5. Backoffice UI

**Стек**

```text
Vue 3
TypeScript
Vite
Pinia
Vue Router
Naive UI
TanStack Query
OpenAPI-generated client
```

UI не обращается к PostgreSQL или ClickHouse напрямую.

---

# 9. Общие платформенные стандарты

## 9.1. Контракты

Основной формат внутренних событий:

```text
Protocol Buffers
```

Используется единый репозиторий:

```text
platform-contracts
```

Он содержит:

* Kafka event schemas;
* gRPC contracts;
* enum status/reason;
* compatibility tests;
* generated clients для Rust, Java и Go.

Для каждой схемы обязательны:

* `schema_version`;
* backward compatibility;
* неизвестные поля должны игнорироваться;
* удаление существующих полей запрещено;
* breaking change создаёт новую major-схему или новый topic.

## 9.2. Kafka clients

* Rust: `rust-rdkafka`;
* Java: официальный Kafka Client и Kafka Streams;
* Go: `franz-go`.

Запрещается использовать разные Kafka abstraction frameworks внутри одного языка без архитектурного обоснования.

## 9.3. Internal RPC

```text
gRPC + Protobuf + mTLS
```

RPC делится на две категории.

**Instance-addressed** — вызов конкретного экземпляра stateful-компонента в обход обычной балансировки, инстанс резолвится через registry в Runtime Redis:

* Delivery → Operator SMPP Session Manager (`protocol=SMPP`) либо Operator HTTP Gateway (`protocol=HTTP`);
* Delivery Reconciliation → Operator SMPP Session Manager (`query_sm`, опционально);
* Notification → конкретный Partner SMPP Gateway.

**Обычные внутренние вызовы** — между stateless control-plane сервисами, стандартная балансировка Kubernetes, без instance addressing:

* Backoffice API → Configuration Service (CRUD конфигурации);
* Backoffice API → Execution Control Service (manual override);
* Backoffice API → Replay Service (создание/подтверждение replay);
* Backoffice API → Scheduler Critical Sweep (`FORCE_TIMEOUT`/`FORCE_RETRY` через `scheduler.critical.commands`, публикуется как Kafka-сообщение, а не gRPC — включено сюда для полноты списка ручных операций);
* Billing Reconciliation → Execution Control Service (freeze/unfreeze партнёра).

Обычный бизнес-процесс между стадиями проходит через Kafka, а не через RPC любой категории.

## 9.4. Доступ к БД

На hot path не используются тяжёлые ORM.

* Rust: SQLx при необходимости;
* Java: jOOQ;
* Go: pgx;
* ClickHouse: native batch driver.

SQL-схемы управляются миграциями, а не автоматически генерируются ORM.

---

# 10. Организация исходного кода

Не рекомендуется создавать отдельный Git-репозиторий для каждого worker.

Предлагаемая структура:

```text
platform-contracts
data-plane-rust
smpp-platform-java
stateful-processing-java
billing-platform-java
platform-services-go
backoffice-ui
```

### `data-plane-rust`

* Partner REST Receiver;
* Pipeline Engine;
* Destination Resolution Service;
* Policy;
* Routing;
* Rust Stage SDK.

### `smpp-platform-java`

* Partner SMPP Gateway;
* Operator SMPP Session Manager;
* Delivery;
* Delivery Reconciliation;
* SMPP core;
* SMPP simulator/testkit.

### `stateful-processing-java`

* Scheduler Standard;
* Scheduler Background;
* Message State Resolver;
* Stateful Processing Runtime.

Scheduler Critical Sweep сюда не входит — он на Go и живёт в `platform-services-go`, поскольку у него больше нет transactional stateful обработки (§3.1).

### `billing-platform-java`

* Billing Service;
* Billing Outbox Publisher;
* Ledger Writer;
* Billing Reconciliation.

### `platform-services-go`

* Execution Control;
* Configuration Service и workers;
* Consent Cache Projector;
* Scheduler Critical Sweep;
* Operator HTTP Gateway;
* DLR services;
* Lifecycle Writer;
* Analytics Writer;
* Notification;
* Partner API;
* Backoffice API;
* Replay Service.

Каждый компонент собирается в отдельный container image, даже когда находится в общем репозитории.

**Совместное развёртывание низкочастотных сервисов.** Execution Control, Configuration Service (+ 2 воркера), DLR Correlation Writer, Billing Outbox Publisher, Billing Ledger Writer, Partner API, Backoffice API и Replay Service — каждый по отдельности не требует выделенного throughput (не завязаны на 20 000 TPS основного потока). Раздавать каждому свой независимый минимум в 2-3 инстанса ради отказоустойчивости избыточно — на реализации их стоит размещать на общем пуле из 3-4 VM (разные процессы/поды на одних и тех же хостах), где отказоустойчивость обеспечивается пулом целиком, а не N раздельными минимумами. DLR Manager, Lifecycle Writer, Analytics Writer и Notification Service — реальный throughput, отдельное масштабирование для них обосновано и от общего пула их стоит держать отдельно. Количественная оценка экономии — `capacity_model.md` §5.

---

# 11. Обязательные quality gates для AI-generated кода

Использование ИИ повышает скорость разработки, но требует более строгой автоматической проверки.

Для каждого merge обязательны:

* unit tests;
* integration tests с Kafka, Redis и PostgreSQL;
* contract tests;
* race/concurrency tests;
* static analysis;
* dependency scanning;
* fuzzing протокольных декодеров;
* fault-injection для stateful processors;
* нагрузочный smoke test;
* отсутствие критического кода без code review.

Особенно критичны:

```text
SMPP codec
Billing Redis Functions
Kafka transactions
Runtime Redis CAS
Scheduler recovery
Message State Resolver transitions
DLQ replay
```

Для этих компонентов ИИ может писать реализацию и тесты, но итоговый контракт и инварианты должны утверждаться человеком.

---

# 12. Итоговая матрица

| Категория                              | Основной стек        |
| -------------------------------------- | -------------------- |
| Высоконагруженный stateless processing | Rust                 |
| SMPP и постоянные TCP-сессии           | Java + Netty         |
| Stateful Kafka processing              | Java + Kafka Streams |
| Billing и финансовый ledger            | Java                 |
| Control Plane и CRUD                   | Go                   |
| Batch writers и проекции               | Go                   |
| Внутренние контракты                   | Protobuf             |
| Синхронные internal calls              | gRPC                 |
| Event transport                        | Kafka                |
| Backoffice frontend                    | Vue 3 + TypeScript   |

Главный принцип спецификации:

> **Rust используется там, где он даёт измеримое преимущество в hot path; Java — там, где важнее зрелая модель протокольного или транзакционного состояния; Go — там, где сервис должен оставаться простым, прозрачным и дешёвым в сопровождении.**

[1]: https://tokio.rs/tokio/tutorial?utm_source=chatgpt.com "Tutorial | Tokio - An asynchronous Rust runtime"
[2]: https://openjdk.org/projects/jdk/25/?utm_source=chatgpt.com "JDK 25"
[3]: https://go.dev/doc/devel/release?utm_source=chatgpt.com "Release History"
[4]: https://netty.io/?utm_source=chatgpt.com "Netty: Home"
