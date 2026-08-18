# Ops Visibility Service

**Основание:** `luminous-hugging-charm.md` (12-фазный план закрытия API-пробелов платформы, `/Users/Alisher/.claude/plans/luminous-hugging-charm.md`), Фаза 8 — Ops/инфра-видимость. При ~34 микросервисах и ~15 из них потребляющих Kafka сейчас нет ни одного места, где видно consumer-group lag по всей платформе разом, ни одного места, где видно `/readyz` всех сервисов разом — оператору приходится обходить каждый сервис/группу вручную. Этот сервис закрывает именно это: не новую систему алертинга, не дашборд с историей — короткоживущий агрегированный снапшот двух вещей, которые раньше нужно было собирать руками.

**Статус:** реально компилируется и тестируется — `go build ./... && go vet ./... && go test ./... -race`, **38/38 тестов проходят**, включая реальный round-trip против **локального Kafka** (`infra/docker/docker-compose.yml`, `localhost:9094`) и **локального Redis** (`brew services`, `localhost:6379`) — не только против моков.

```bash
# Kafka — через docker-compose (infra/docker/docker-compose.yml), EXTERNAL listener на localhost:9094
cd infra/docker && docker compose up -d kafka

# Redis — локальный brew-инстанс на дефолтном порту, без пароля
brew services start redis

cd services/ops-visibility-service
go build ./... && go vet ./... && go test ./... -race
```

Если Kafka на `localhost:9094` недостижим — `internal/kafkalag`'s integration-тесты (`TestFetchAllAgainstRealKafkaReportsRealLag`, `TestPingAgainstRealKafka`) явно `t.Skip`, не подделывают round-trip. То же для Redis на `localhost:6379` в `internal/store`.

## Что опрашивается и как

### Kafka consumer-group lag (`internal/kafkalag`)

Использует `github.com/twmb/franz-go/pkg/kadm` (v1.18.0 — версия admin-подпакета, зависящая от `github.com/twmb/franz-go` v1.21.0; сам `franz-go` в этом сервисе закреплён на v1.21.5, той же версии, что уже использует `execution-control-service`/`config-cache-projector` — Go module resolution берёт максимум из двух требований, конфликта нет).

**Список опрашиваемых групп нигде не хардкодится.** `kadm.Client.Lag(ctx)`, вызванный БЕЗ имён групп, сам делает то, что нужно: `DescribeGroups` внутри `Lag` при пустом списке групп сначала выполняет `ListGroups` и описывает вообще все classic-группы, зарегистрированные на кластере (см. doc comment `DescribeGroups`: *"describes either all classic groups specified, or all classic groups in the cluster if none are specified"*, и комментарий внутри `Lag`: *"If the input set of groups is empty, DescribeGroups returns all groups. We add to `set` here so that the Lag function itself can calculate lag for all groups."* — `~/go/pkg/mod/github.com/twmb/franz-go/pkg/kadm@v1.18.0/groups.go`). Новая потребляющая группа, появившаяся позже, просто появится в следующем цикле опроса — этот сервис не нужно трогать.

`internal/kafkalag.Client.FetchAll` вызывает `kadm.Lag(ctx)` и трансформирует `kadm.DescribedGroupLags` (чистой функцией `BuildSnapshot`, вынесенной отдельно ради юнит-тестируемости без брокера — тот же приём, что `processRecords` в `config-cache-projector/internal/kafkaio/consumer.go`) в `kafkalag.Snapshot`. Ошибка ВСЕГО цикла (например, ни один брокер не отвечает) попадает в `Snapshot.Error`; ошибка ОДНОЙ группы (например, у неё отвалился координатор, пока остальные опросились нормально) — в `ConsumerGroupLag.Error` этой конкретной группы, не роняя весь снапшот.

### `/readyz` каждого сервиса (`internal/readyz`, `internal/services`)

Список из ~34 имён сервисов (`internal/services.List`) — **захардкожен и должен вручную поддерживаться в синхроне с `k8s/generate_manifests.py::SERVICES`** (тот же класс дублирования, что кодбейз уже сознательно принимает в других местах — например, `credential_ref_to_env_var`, независимо продублированный в Rust/Python/Go/Java, см. `services/credential-issuer-service/README.md`). Список снят на 2026-08-13, 34 имени, `internal/services/services_test.go` явно проверяет это число и падает, если оно изменится — сигнал, что список требует ручной сверки с `generate_manifests.py`. Этот файл этой фазой **не редактируется** (за него отвечает отдельная задача, которая заодно онбордит и `ops-visibility-service`, и `incident-service` в манифесты).

Гибкость без редеплоя — два env var:

* `EXTRA_SERVICES` — список через запятую, ДОБАВЛЯЕТСЯ к `internal/services.List`.
* `SERVICES_OVERRIDE` — список через запятую, полностью ЗАМЕНЯЕТ `internal/services.List` (побеждает над `EXTRA_SERVICES`, если заданы оба — расширять заведомо иной список не имеет смысла).

Каждый сервис опрашивается по адресу `http://<имя-сервиса>.mpp.svc:9090/readyz` — `HEALTH_PORT = 9090` (`k8s/generate_manifests.py:30`), и это без исключений: `_container()` в `generate_manifests.py` безусловно добавляет health-порт и `readinessProbe`/`livenessProbe` на `/healthz`/`/readyz` каждому `Service` независимо от `workload_class`, включая `backoffice-ui` (`workload_class="frontend"`) — проверено чтением исходника, не предположение.

`internal/readyz.Poller.PollAll` — bounded-concurrency fan-out: горутина на цель + буферизованный канал-семафор (`READYZ_POLL_CONCURRENCY`, по умолчанию 8) ограничивает число одновременных запросов, и у каждого запроса свой `http.Client{Timeout: READYZ_REQUEST_TIMEOUT}` (по умолчанию 3с) — один зависший/недоступный сервис не может застопорить обход остальных дольше собственного таймаута (проверено `TestPollAllSlowServiceTimesOutWithoutBlockingOthers` и `TestPollAllBoundsConcurrency` против `httptest.Server`).

## Интервалы опроса и TTL — обоснование выбора

* **`KAFKA_LAG_POLL_INTERVAL` / `READYZ_POLL_INTERVAL` — 20с по умолчанию** (задача оставляла диапазон 15-30с на усмотрение реализации). Оба цикла используют один и тот же интервал по умолчанию намеренно — это даёт оператору один согласованный "снимок времени" в `GET /snapshot` (оба поля обновляются примерно синхронно), а не два независимо дрейфующих таймера. Разные значения поддерживаются отдельными env var, если понадобится развести (например, Kafka `DescribeGroups`/`ListOffsets` под нагрузкой дороже, чем HTTP GET на `/readyz`).
* **`SNAPSHOT_TTL` — 3 минуты по умолчанию** (≈9 циклов при 20с интервале). Достаточно долго, чтобы пережить пропуск пары циклов подряд подряд (временный сетевой блип, GC-пауза) без того, чтобы `GET /snapshot`/Redis начали "врать" про актуальность — но достаточно коротко, чтобы если сам `ops-visibility-service` упал или завис насовсем, устаревшие данные видимо исчезли (`*_available=false` в `/snapshot`, ключ пуст в Redis) в течение единиц минут, а не отдавались бы неограниченно долго как будто всё ещё актуальные.
* **`KAFKA_LAG_POLL_TIMEOUT` — 15с** — верхняя граница на один цикл опроса Kafka (отдельно от интервала: если интервал сконфигурирован очень маленьким, это не даёт одному циклу растянуться дольше разумного).
* **Redis, не Postgres, без истории** — прямое требование плана для MVP этой фазы: *"Без новой Postgres-схемы для MVP — короткий TTL в Redis, не история. Таблица трендов (`ops.health_snapshots`) — явный отдельно оцениваемый follow-up, не тащим сейчас."* Каждый `Set` с TTL полностью ЗАМЕНЯЕТ предыдущее значение под тем же ключом (не накапливает, не мёрджит) — в Redis в любой момент живёт только последний цикл опроса каждой категории, что и проверяет `TestWriteKafkaLagOverwritesPreviousSnapshotAtomically`.

## Redis — ключевая схема (контракт для будущего чтения из `backoffice-api`)

Это самое важное, что нужно задокументировать точно — Redis здесь пишет **этот** сервис, а читать (кроме `GET /snapshot` самого этого сервиса) будет отдельная будущая работа над `backoffice-api`, не входящая в эту фазу.

```
ops:kafka-lag:snapshot     STRING (JSON kafkalag.Snapshot)   TTL = SNAPSHOT_TTL (по умолчанию 3 мин)
ops:readyz:snapshot        STRING (JSON readyz.Snapshot)     TTL = SNAPSHOT_TTL (по умолчанию 3 мин)
```

Два ключа, не по одному на группу/сервис. При ожидаемой кардинальности (~15 consumer groups, ~34 сервиса) один JSON-блоб на категорию за цикл опроса даёт:

* **атомарность снапшота целиком** — один `GET` возвращает все группы/сервисы одного и того же цикла опроса, не смесь из разных циклов;
* **не нужен отдельный индекс имён** — подход "ключ на элемент" (как у `config-cache-projector`, где `config:current:{entity_type}:{entity_id}` пишется по одному, событие за событием) потребовал бы ещё и `SET` с текущими именами групп/сервисов, а этот `SET` сам не истекает по TTL синхронно с элементами и требовал бы отдельной чистки — здесь это не нужно: пропавшая consumer group или сервис, выведенный из `SERVICES_OVERRIDE`, просто не попадёт в следующий записанный снапшот.

`internal/store.Client` — единственный код, пишущий/читающий эти ключи (`WriteKafkaLag`/`ReadKafkaLag`/`WriteReadyz`/`ReadReadyz`). Оба используют Redis-инстанс `REDIS_RUNTIME_*` (см. ниже) — та же категория "оперативный, короткоживущий, некритичный к потере" Redis, что уже используют `scheduler-critical-sweep`/`consent-cache-projector`/`partner-notification-service` (`k8s/generate_manifests.py::SECRET_DEPENDENCIES`), не `redis-configuration` (bootstrap-only, отдельная семантика) и не `redis-billing` (денежный путь). Это решение зафиксировано здесь как явный judgment call для будущего онбординга в `generate_manifests.py` — этот сервис не в `SECRET_DEPENDENCIES` сейчас (файл не редактировался этой фазой), но когда его туда будут заводить, он должен получить `["redis-runtime"]`.

### JSON-форма `ops:kafka-lag:snapshot` (`kafkalag.Snapshot`)

```json
{
  "generated_at": "2026-08-13T12:00:00Z",
  "bootstrap_servers": ["kafka-bootstrap.mpp.svc:9092"],
  "groups": [
    {
      "group": "config-cache-projector",
      "state": "Stable",
      "total_lag": 42,
      "partitions": [
        {"topic": "config.changes", "partition": 0, "commit_offset": 100, "end_offset": 142, "lag": 42}
      ]
    }
  ],
  "error": ""
}
```

`error` на верхнем уровне — ошибка ВСЕГО цикла (например, ни один брокер не достижим), непустая строка только если весь опрос не удался. У каждой группы в `groups[]` тоже есть опциональное `error` (опущено через `omitempty`, если пусто) — ошибка конкретно этой группы (например, `coordinator not available`), не мешающая остальным группам в том же снапшоте. Партиция тоже может нести `error` (например, `UNKNOWN_TOPIC_OR_PARTITION`, если топик исчез, а коммиты по нему остались).

### JSON-форма `ops:readyz:snapshot` (`readyz.Snapshot`)

```json
{
  "generated_at": "2026-08-13T12:00:05Z",
  "services": [
    {"service": "iam-service", "ready": true, "http_status": 200, "latency_ms": 12},
    {"service": "some-down-service", "ready": false, "latency_ms": 3001, "error": "context deadline exceeded (Client.Timeout exceeded while awaiting headers)"}
  ]
}
```

`services[]` отсортирован по имени сервиса (детерминированный порядок независимо от порядка завершения горутин опроса — иначе JSON-диффы между циклами шумели бы порядком полей без изменения данных). `ready = true` тогда и только тогда, когда HTTP-статус ответа `/readyz` — ровно 200; любой другой статус, таймаут, отказ соединения — `ready = false` с заполненным `error` в случае сетевой ошибки.

## `GET /snapshot` — HTTP-эндпоинт этого сервиса (`:9090`, тот же mux, что `/healthz`/`/readyz`/`/metrics`)

Отдаёт оба Redis-ключа одним ответом, независимо curl'abelен без доступа к Redis:

```json
{
  "kafka_lag": { "...": "kafkalag.Snapshot, см. выше, или null" },
  "kafka_lag_available": true,
  "readyz": { "...": "readyz.Snapshot, см. выше, или null" },
  "readyz_available": true
}
```

`kafka_lag_available`/`readyz_available` — явные булевы флаги, не просто вывод из `null` — отсутствие данных (сервис только что стартовал, TTL истёк потому что цикл опроса завис/сам сервис недавно упал) должно быть видно однозначно. Ошибка чтения ОДНОГО из двух ключей из Redis не роняет весь ответ (`500`) — второй ключ отдаётся, если он читается нормально (`TestSnapshotHandlerPartialFailureStillReturns200`); эндпоинт всегда отвечает `200` с `Content-Type: application/json`, если сам HTTP-сервер жив.

## Собственное здоровье (`internal/health`, порт 9090)

Тот же паттерн `SetDependencyChecks`, что `iam-service`/`credential-issuer-service`/`config-cache-projector`: `/readyz` реально пингует обе зависимости (`kafkalag.Client.Ping` → `kgo.Client.Ping`, `store.Client.Ping` → Redis `PING`) с 2с таймаутом на запрос, не отдаёт статический флаг, выставленный один раз при старте.

## Переменные окружения

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-bootstrap.mpp.svc:9092` | Список брокеров через запятую — та же конвенция, что у всех Kafka-сервисов платформы |
| `REDIS_RUNTIME_HOST` / `REDIS_RUNTIME_PORT` / `REDIS_RUNTIME_PASSWORD` | `localhost` / `6379` / `""` | Redis для хранения снапшотов |
| `REDIS_RUNTIME_DB` | `0` | Номер логической БД Redis (тесты используют `15`, чтобы не задевать прод-подобные данные на `0`) |
| `KAFKA_LAG_POLL_INTERVAL` | `20s` | Интервал опроса Kafka lag |
| `READYZ_POLL_INTERVAL` | `20s` | Интервал опроса readyz-грида |
| `KAFKA_LAG_POLL_TIMEOUT` | `15s` | Верхняя граница на один цикл опроса Kafka |
| `READYZ_REQUEST_TIMEOUT` | `3s` | Таймаут одного HTTP-запроса `/readyz` |
| `READYZ_POLL_CONCURRENCY` | `8` | Максимум одновременных `/readyz`-запросов |
| `SNAPSHOT_TTL` | `3m` | TTL обоих Redis-ключей |
| `EXTRA_SERVICES` | `""` | Список сервисов через запятую, добавляется к `internal/services.List` |
| `SERVICES_OVERRIDE` | `""` | Список сервисов через запятую, полностью заменяет `internal/services.List` (побеждает над `EXTRA_SERVICES`) |

## Реально проверено, не только компиляция

* **`internal/kafkalag`** — 5 чистых тестов `BuildSnapshot` (пустой список групп, top-level ошибка цикла, корректный расчёт `total_lag`/per-partition полей из руками собранного `kadm.DescribedGroupLags`, ошибка одной группы не портит остальные, сортировка по имени) + 2 реальных против `localhost:9094`: `TestFetchAllAgainstRealKafkaReportsRealLag` создаёт топик, продьюсит 10 записей, коммитит offset вручную через `kadm.CommitOffsets` (группа регистрируется в `__consumer_offsets` и становится видна `DescribeGroups`/`Lag` в состоянии `Empty` — не нужно гонять полный consumer-group protocol с `JoinGroup`/`SyncGroup` ради теста), затем проверяет, что `FetchAll` увидел ровно эту группу с ожидаемым lag; `TestPingAgainstRealKafka` — реальный `Ping`.
* **`internal/readyz`** — 6 тестов против `httptest.Server`: здоровый сервис → `ready=true`; не-200 → `ready=false`; полностью недостижимый адрес (`http://127.0.0.1:1`) → `ready=false` с непустой `Error`, без паники/зависания; зависший на 5с сервис с `READYZ_REQUEST_TIMEOUT=200мс` не блокирует опрос быстрого соседа (весь `PollAll` укладывается в секунды, не в 5с+); реальная проверка ограничения конкуренции — семафор с лимитом 2, счётчик текущих одновременных запросов через `atomic`, максимум пронаблюдённых одновременных запросов не превышает лимит; детерминированная сортировка результатов.
* **`internal/httpapi`** — фейковый `SnapshotStore`: оба ключа доступны, оба ключа отсутствуют (`*_available=false`, не 500), ошибка чтения одного ключа не роняет ответ со вторым.
* **`internal/store`** — реальный локальный Redis (`localhost:6379`, DB 15): `Ping`, чтение отсутствующего ключа возвращает `(nil, nil)` не ошибку, round-trip записи/чтения для обеих категорий, **реальное истечение по TTL** (запись с TTL=300мс, сон 500мс, повторное чтение — ключ действительно пропал, не просто "считается устаревшим" на уровне приложения), повторная запись полностью заменяет предыдущий снапшот (нет накопления/истории).
* **`internal/services`** — список без дубликатов/пустых записей, точное число (34, с падением теста как сигналом на ручную сверку с `generate_manifests.py`, если оно изменится), явно не содержит `ops-visibility-service`/`incident-service`.
* **`cmd/ops-visibility-service`** — чистые хелперы: `env`/`envDuration`/`envInt` с фоллбэками на невалидные значения, `splitCSV`, `resolveServiceList` (дефолт / `EXTRA_SERVICES` добавляет / `SERVICES_OVERRIDE` полностью заменяет и побеждает), `buildTargets` строит URL по платформенной конвенции.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено в реальном k8s-кластере** — та же оговорка, что у остальных сервисов сессии на момент их первого среза.
* **k8s-онбординг (`k8s/generate_manifests.py`, включая `SECRET_DEPENDENCIES["ops-visibility-service"] = ["redis-runtime"]`) — отдельная задача**, намеренно не эта фаза (см. "Redis — ключевая схема" выше про сам judgment call выбора `redis-runtime`). До онбординга у этого сервиса нет реального Deployment/Service/probes в кластере.
* **Нет истории/трендов** — прямо по плану: `ops.health_snapshots` (Postgres-таблица трендов) явно вынесена как отдельно оцениваемый follow-up, не MVP этой фазы. TTL — единственный механизм "давности" данных здесь.
* **Нет интеграции с `backoffice-api`/`backoffice-ui`** — читающая сторона (`GET /v1/ops/...` на `backoffice-api`, раздел "Ops Health" в `backoffice-ui`, право `ops:read`) — отдельная работа, не входящая в эту фазу. `GET /snapshot` на этом сервисе существует именно затем, чтобы у той будущей работы было что проксировать/читать без прямого доступа к Redis.
* **Нет share-групп (`kadm.DescribeShareGroups`) и streams-групп отдельным путём** — `kadm.Client.Lag` работает с classic consumer groups (то, что реально использует вся платформа — `kgo.ConsumerGroup`); Kafka Streams-сервисы (`scheduler-standard-lane`, `scheduler-background-lane`, `message-state-resolver`) используют тот же протокол consumer group под капотом, так что их lag виден тем же путём — отдельная share-group ветка API не нужна для текущего состава потребителей платформы.
* **`ListGroupsByType`/фильтрация по типу группы не используется** — `Lag(ctx)` без аргументов уже покрывает "все classic-группы", более узкая фильтрация не требовалась ни одним потребителем этого сервиса на этом шаге.
