# A2P MPP — Disaster Recovery Runbook

**Основание:** `BACKOFFICE_ROADMAP.md` P1 «Capacity/DR» — «нет проверенного backup/restore, RPO/RTO, DR-топологии, chaos-прогонов, production runbook». Терраформ-часть (`171bb8e`) уже включила нативные managed-бэкапы PostgreSQL/ClickHouse — этот документ добавляет то, чего не хватало: реально выполненный restore-прогон (не описание того, что «стоило бы сделать»), RPO/RTO с числами, и честный список того, что ещё не проверено, потому что реального облачного окружения для этого нет.

**Что здесь НЕ сделано и почему:** ни один прогон в этом документе не выполнялся против Yandex Cloud — у этой сессии нет доступа к реальному облачному окружению. Всё, что помечено «локально, реально протестировано» — выполнено против `infra/docker/docker-compose.yml`. Всё, что помечено «облако, не проверено» — это то, что Terraform теперь умеет включить (`171bb8e`), но включённая настройка ≠ отрепетированное восстановление. Чтобы не повторить ту же ошибку, которую этот документ должен исправить (спецификация без прогона), эти два класса утверждений разделены явно в каждом разделе.

---

## 1. Что бэкапится автоматически уже сегодня (из `171bb8e`)

| Хранилище | Механизм | Retention | Окно | Управляется |
|---|---|---|---|---|
| PostgreSQL (`yandex_mdb_postgresql_cluster`) | Managed full backup + непрерывный WAL-архив (нативно, `terraform providers schema -json` подтверждено на `yandex-cloud/yandex` 0.218.0) | 14 суток | 03:00 UTC | Terraform, `infra/terraform/postgresql.tf` |
| ClickHouse (`yandex_mdb_clickhouse_cluster_v2`) | Managed full backup (нативно, top-level атрибут, не блок) | 7 суток | 04:00 UTC | Terraform, `infra/terraform/clickhouse.tf` |
| Redis (`yandex_mdb_redis_cluster`) | **Нет Terraform-контроля** — в установленной версии провайдера у ресурса нет `backup_retain_period_days`/`backup_window_start`. По документации Yandex Managed Redis сам снимает ежедневные бэкапы при `persistence_mode=ON`, но с фиксированной, не настраиваемой отсюда политикой хранения. **Не подтверждено реальным прогоном** — считать честным открытым гэпом, не решённым вопросом. | — | — | Не Terraform; поведение управляемого сервиса, непроверенное |

Это всё, что existed до сегодняшнего дня. Ничего из этого раздела не тестировалось как restore — только как «включённая настройка», см. `171bb8e`'s коммит-сообщение (`terraform providers schema -json`, не предположение о поведении).

## 2. Что реально протестировано в этом заходе (локально, docker-compose)

### 2.1 Почему не трогали живой `docker-postgres-1`/`docker-redis-billing-1`

На момент этого прогона (`2026-09-14`, `docker ps`) в этом shared-репозитории на локальной машине был поднят полный стек из 32+ сервисов, часть — «Up 5 hours», часть — «Up 4 days», активно пишущих в `docker-postgres-1` и `docker-redis-billing-1` (backoffice-api, iam-service, billing-service и т.д.). Останавливать/пересоздавать эти контейнеры означало бы реальный риск потерять или испортить данные, на которые может полагаться другая параллельная сессия в этом же репозитории — прямо запрещено инструкцией к этой задаче.

Вместо этого: **живые данные читались, но не изменялись** (`pg_dump`/`docker cp` — обе операции read-only относительно живого контейнера, `BGSAVE` — штатная операция, которую Redis и так делает сам), а само восстановление проверялось на **отдельном одноразовом контейнере с отдельным томом**, никак не связанном с живым стендом. После прогона оба одноразовых контейнера удалены (`docker rm -f`), scratch-файлы дампа удалены.

### 2.2 PostgreSQL — реальный restore-прогон

**Источник:** `docker-postgres-1` (postgres:17, host-порт 5433, `mpp`/`mpp_local_dev`/`mpp`, см. `infra/docker/docker-compose.yml`), живой стенд, 667 МБ, 13 схем, PostgreSQL 17.10.

Реальные команды и вывод:

```bash
$ export PGPASSWORD=mpp_local_dev
$ time pg_dump -h localhost -p 5433 -U mpp -d mpp -Fc -f mpp_dev_backup.dump
pg_dump ... 6.61s user 0.37s system 81% cpu 8.613 total
$ ls -lh mpp_dev_backup.dump
-rw-r--r--  1 Alisher  wheel   100M mpp_dev_backup.dump
```

Одноразовый restore-таргет — отдельный контейнер, отдельный том, порт 5544 (не 5433):

```bash
$ docker run -d --name mpp-dr-drill-restore \
    -e POSTGRES_USER=mpp -e POSTGRES_PASSWORD=mpp_local_dev -e POSTGRES_DB=mpp \
    -p 5544:5432 postgres:17
$ pg_isready -h localhost -p 5544 -U mpp -d mpp
localhost:5544 - accepting connections

$ time pg_restore -h localhost -p 5544 -U mpp -d mpp --no-owner --no-privileges mpp_dev_backup.dump
pg_restore ... 0.74s user 0.43s system 6% cpu 17.038 total
```

**Верификация — не «вроде похоже», а построчное сравнение источник vs восстановленное:**

| Проверка | Источник (5433) | Восстановлено (5544) | Совпадает |
|---|---|---|---|
| Таблиц по схемам (13 схем) | `backoffice=1, billing=2, config=2, control=1, credentials=2, dlr=26, iam=8, incident=2, messaging=4, policy=2, reconciliation=2, routing=2, support=1` | идентично | ✅ |
| `policy.policy_template` count | 1 | 1 | ✅ |
| `config.config_versions` count | 67 | 67 | ✅ |
| `messaging.message_read_model` count | 1 780 486 | 1 780 486 | ✅ |
| `iam.partner_portal_users` (конкретные строки, созданы этой же сессией сегодня) | `id=2 click_uz_portal 2026-09-14 05:54:02.659422+00`, `id=3 click_uz_portal2 2026-09-14 05:59:09.813653+00` | побайтово идентично, включая микросекунды timestamp | ✅ |
| Sequence `iam.partner_portal_users_id_seq.last_value` | 3 | 3 | ✅ |
| `pg_constraint` count | 211 | 211 | ✅ |
| `pg_indexes` count (нестандартные схемы) | 124 | 124 | ✅ |

Единственное честное расхождение: `pg_database_size` — 667 МБ источник vs 528 МБ восстановленное. Это ожидаемо (свежий restore не несёт физический bloat/свободные страницы старого кластера) и не является потерей данных — все счётчики строк, constraints и indexes выше совпали точно.

**Замеренное RTO для локального дампа 667 МБ: ~26 секунд** (dump 8.6с + restore 17с, реальные `time`-замеры выше). Это не production-число — реальная PostgreSQL, source of truth (`data_infrastructure_spec.md` §1) будет на порядки больше 667 МБ, и `pg_dump`/`pg_restore` логического дампа — не то, чем реально пользуются для облачного restore (там — снэпшот управляемого сервиса, другой механизм, другое время, см. §4).

Cleanup: `docker rm -f mpp-dr-drill-restore`, файл дампа удалён из scratch.

### 2.3 Redis — второй прогон (redis-billing, не redis-runtime/redis-configuration)

**Почему redis-billing, а не два других инстанса:** первым делом проверена реальная persistence-конфигурация всех трёх живых Redis (не предположена):

```bash
$ docker exec docker-redis-runtime-1 redis-cli -a *** CONFIG GET save        # -> "" (пусто)
$ docker exec docker-redis-runtime-1 redis-cli -a *** CONFIG GET appendonly  # -> no
$ docker exec docker-redis-configuration-1 redis-cli -a *** CONFIG GET save        # -> "" (пусто)
$ docker exec docker-redis-configuration-1 redis-cli -a *** CONFIG GET appendonly  # -> no
$ docker exec docker-redis-billing-1 redis-cli -a *** CONFIG GET save        # -> "3600 1 300 100 60 10000" (дефолт)
$ docker exec docker-redis-billing-1 redis-cli -a *** CONFIG GET appendonly  # -> yes
```

Это подтверждает и объясняет комментарии `infra/docker/docker-compose.yml`: `redis-runtime`/`redis-configuration` реально запущены с `--save ""` и без AOF — **нулевая персистентность, что угодно теряется при рестарте контейнера**, намеренно (TTL'd эфемерное состояние / rebuildable-кэш, см. §3). `redis-billing` — единственный из трёх с реальной durability (AOF, `appendonly yes` из docker-compose, плюс дефолтная RDB-политика провайдера образа, никем не отключённая). Поэтому restore-drill имеет смысл только для `redis-billing` — тестировать восстановление хранилища, которое само спроектировано терять всё при рестарте, было бы бессмысленно.

**Реальный restore-прогон redis-billing:**

```bash
$ docker exec docker-redis-billing-1 redis-cli -a *** DBSIZE
6
$ docker exec docker-redis-billing-1 redis-cli -a *** TYPE billing:account:click_uz            # hash
$ docker exec docker-redis-billing-1 redis-cli -a *** TYPE billing:account:click_uz:charges     # set
$ docker exec docker-redis-billing-1 redis-cli -a *** TYPE billing:outbox:{0,1,2,3}             # stream x4

# Снимок значений ДО восстановления:
HGETALL billing:account:click_uz        -> state=ACTIVE balance=-4097516814 epoch=0
SCARD billing:account:click_uz:charges  -> 1168479
md5(sort(SMEMBERS ...))                 -> 6cbca7fd24b14a7fccddaed71c4763d8
XLEN billing:outbox:{0,1,2,3}           -> 291731 292450 292685 291613

$ docker exec docker-redis-billing-1 redis-cli -a *** BGSAVE
Background saving started
$ docker cp docker-redis-billing-1:/data/. ./redis-billing-data/    # read-only относительно живого контейнера

$ docker run -d --name mpp-dr-drill-redis \
    -v $(pwd)/redis-billing-data:/data -p 6399:6379 \
    redis:7-alpine redis-server --requirepass *** --appendonly yes
# лог: "Done loading RDB, keys loaded: 6" -> "DB loaded from append only file: 2.373 seconds" -> "Ready to accept connections"
```

**Верификация — точное совпадение по всем метрикам:**

| Метрика | До (источник) | После (восстановлено) |
|---|---|---|
| `DBSIZE` | 6 | 6 |
| `HGETALL billing:account:click_uz` | `state=ACTIVE balance=-4097516814 epoch=0` | идентично |
| `SCARD billing:account:click_uz:charges` | 1 168 479 | 1 168 479 |
| `md5(sort(SMEMBERS ...))` | `6cbca7fd24b14a7fccddaed71c4763d8` | `6cbca7fd24b14a7fccddaed71c4763d8` |
| `XLEN billing:outbox:{0,1,2,3}` | 291731 / 292450 / 292685 / 291613 | идентично, все 4 |

**Замеренное RTO: ~10 секунд** (`docker cp` практически мгновенный для ~150 МБ AOF+RDB, контейнер принимает `PING` через ~1с, полная загрузка AOF — 2.4с по логу выше).

**Честная находка, отдельно от самого теста:** в `infra/docker/docker-compose.yml` у сервиса `redis-billing` **нет смонтированного volume** — `/data` живёт только в writable layer контейнера. AOF/RDB защищают от потери данных при *рестарте процесса внутри контейнера* (`docker restart`), но НЕ при удалении/пересоздании самого контейнера (`docker rm`, `docker compose down` без `-v` всё равно удаляет анонимный layer вместе с контейнером) — ровно то же наблюдение, что уже сделано в docker-compose про Kafka (`KAFKA_LOG_DIRS`, комментарий про `/tmp/kafka-logs` внутри writable layer). Это открытый локальный гэп: `redis-billing` в этом docker-compose однозначно нуждается в durability (баланс/CAS-состояние, см. её же комментарий в файле), но сегодня одна `docker compose down` без `-v` при пересоздании контейнера (не только volume) всё равно теряет `/data`, если Docker не сохраняет writable layer явно (в общем случае — не гарантировано). Не исправлено в рамках этой задачи (задача — тест восстановления, не переконфигурация compose), зафиксировано как отдельный follow-up.

Cleanup: `docker rm -f mpp-dr-drill-redis`, scratch-файлы удалены.

## 3. `redis-runtime` / `redis-configuration` — почему не тестировались как backup/restore

Оба намеренно эфемерны (`--save ""`, без AOF, подтверждено CONFIG GET выше) — это не гэп, а сознательный дизайн (см. комментарии в `infra/docker/docker-compose.yml`):

* `redis-configuration` — rebuildable-кэш проекций config-версий; `config-cache-projector` (`services/config-cache-projector/README.md`) перечитывает `config.changes` из Kafka и переписывает `config:current:*`/`config:version:*` заново — то есть «восстановление» этого хранилища на практике = «дать consumer'у перепройти лог», не backup/restore в классическом смысле.
* `redis-runtime` — TTL'd исполнительное состояние (`exec:{message_id}`, CAS) пайплайна. Есть TTL-backstop (~25ч, по комментарию `delivery-reconciliation-service` в docker-compose), но **не проверено в рамках этой задачи**, полностью ли это состояние воспроизводимо реплеем Kafka-лога после потери Redis, или backstop — это просто «истечёт и перестанет мешать», а не «восстановится в корректном виде». Открытый вопрос, не факт — не утверждается ни то, ни другое без проверки.

## 4. RPO / RTO по хранилищам

| Хранилище | RPO (локально, измерено) | RPO (облако, из конфигурации `171bb8e`, НЕ отрепетировано) | RTO (локально, измерено) | RTO (облако) |
|---|---|---|---|---|
| **PostgreSQL** | Зависит от того, когда в последний раз вручную снят `pg_dump` — в локальном dev-стенде НЕТ расписания автоматического дампа, RPO = «сколько угодно, пока кто-то не снимет дамп вручную». Это тоже честный гэп локального стенда, не только облака. | Управляемый сервис держит непрерывный WAL-архив поверх суточного full backup (`171bb8e`) → секунды-минуты (по документации Yandex, не измерено нами) | **~26с** на 667 МБ (реально измерено, §2.2) | Не измерено — зависит от размера кластера в production, управляемый restore-механизм отличается от `pg_dump`/`pg_restore` (снэпшот, не логический дамп) |
| **ClickHouse** | Не тестировался в этом заходе (не хватило времени/приоритет ниже PostgreSQL — analytics/diagnostic хранилище, не source of truth, `171bb8e`) | 7-суточное окно full backup, аналитика допускает более мягкий RPO (обоснование в `171bb8e`) | Не измерено | Не измерено |
| **Redis-billing** | **~1с** (AOF `appendonly yes`, дефолтный `everysec` fsync — стандартное поведение Redis, не переопределено явно нигде в стеке) при рестарте процесса; **полная потеря** при удалении контейнера (нет volume, см. §2.3) | Управляемый сервис заявляет ежедневные бэкапы при `persistence_mode=ON`, но retention не настраивается через Terraform и не подтверждён реальным прогоном (`171bb8e`) | **~10с** на 6 ключей/~150 МБ AOF+RDB (реально измерено, §2.3) | Не измерено |
| **Redis-runtime / Redis-configuration** | Намеренно 0 — эфемерны по дизайну (§3) | Тот же управляемый механизм, что и redis-billing (если вообще применяется к этим двум инстансам в production-топологии — не проверено, нужно свериться с `infra/terraform/redis.tf`, что это те же или отдельные кластеры) | N/A (не backup/restore, а re-projection/TTL, см. §3) | N/A |

## 5. Как реально запустить restore локально (воспроизводимая процедура)

### PostgreSQL

```bash
# 1. Снять дамп (read-only относительно источника)
PGPASSWORD=mpp_local_dev pg_dump -h localhost -p 5433 -U mpp -d mpp -Fc -f backup.dump

# 2. НЕ восстанавливать поверх живого стенда, если на нём есть данные, которыми
#    кто-то пользуется — поднять отдельный контейнер на отдельном порту/томе:
docker run -d --name pg-restore-drill \
  -e POSTGRES_USER=mpp -e POSTGRES_PASSWORD=mpp_local_dev -e POSTGRES_DB=mpp \
  -p 5544:5432 postgres:17

# 3. Восстановить
PGPASSWORD=mpp_local_dev pg_restore -h localhost -p 5544 -U mpp -d mpp \
  --no-owner --no-privileges backup.dump

# 4. Проверить (минимум — счётчики строк ключевых таблиц + конкретная известная строка)
```

### Redis (AOF-инстансы, т.е. redis-billing)

```bash
docker exec <redis-container> redis-cli -a <pass> BGSAVE
docker cp <redis-container>:/data/. ./redis-data-copy/
docker run -d --name redis-restore-drill -v $(pwd)/redis-data-copy:/data -p 6399:6379 \
  redis:7-alpine redis-server --requirepass <pass> --appendonly yes
docker exec redis-restore-drill redis-cli -a <pass> DBSIZE   # сверить с источником
```

## 6. Что нужно ДОПОЛНИТЕЛЬНО для реального облачного DR-прогона (не сделано здесь, честно)

Наличие `backup_retain_period_days`/`backup_window_start` в Terraform (`171bb8e`) означает только, что управляемый сервис **включён** снимать бэкапы — это НЕ равно «restore отрепетирован». Ни один пункт ниже не проверялся, потому что у этой сессии нет реального Yandex Cloud окружения:

1. **Реальный restore из managed-снэпшота Yandex MDB для PostgreSQL** — через `yandex_mdb_postgresql_cluster` API/CLI (`yc managed-postgresql cluster restore` или аналог), не `pg_dump`/`pg_restore`. Другой механизм, другое время, другие права доступа (нужен отдельный least-privilege restore-identity, ещё не заведён — ср. с уже открытым пунктом «Schema rollout discipline» в `BACKOFFICE_ROADMAP.md` про отдельную migration identity).
2. **Point-in-time recovery (PITR)** через WAL-архив, а не только restore на момент последнего full backup — заявлено в комментарии `171bb8e`, но конкретная команда/процедура/фактическая гранулярность RPO не проверены.
3. **ClickHouse restore** из managed-бэкапа — вообще не выполнялся, ни разу, ни локально, ни в облаке.
4. **Redis managed backup** — само существование ежедневных бэкапов при `persistence_mode=ON` взято из документации Yandex, не подтверждено даже наблюдением (нет реального кластера). Реальный прогон restore для Redis в облаке не выполнялся.
5. **DR-топология между зонами/регионами** — вообще не в скоупе этого документа; текущий Terraform не описывает multi-region failover ни для одного хранилища.
6. **RTO под production-объёмом данных**, а не 667 МБ/150 МБ dev-стенда — числа в §4 не масштабируются линейно, для production нужен отдельный замер.
7. **Восстановление после потери целого availability zone**, не только «пересоздать контейнер/базу с тем же содержимым».

## 7. Chaos / failure-testing checklist — на будущий заход (НЕ выполнено, только список)

Ничего из этого не запускалось. Список — то, что стоит проверить, когда появится реальный staging/production-подобный стенд, не выдуманные «результаты» несуществующих экспериментов:

- [ ] Убить `docker-postgres-1` (`docker kill`, не `stop`) под нагрузкой — проверить, что зависимые сервисы (billing, config-event-publisher и т.д.) деградируют предсказуемо (retry/backoff), а не теряют сообщения молча.
- [ ] Убить один из трёх Redis-инстансов под нагрузкой — отдельно для каждого (runtime/configuration/billing), т.к. у них разный дизайн durability (§3) — ожидаемое поведение должно различаться и это стоит подтвердить, а не предположить.
- [ ] Убить Kafka-брокер (в проде — один из RF=3 реплик) во время активного produce/consume — проверить фактическое ISR-поведение, не только конфигурацию `min.insync.replicas`.
- [ ] Сетевой партишн между Pipeline Engine и Redis Runtime — проверить, что CAS-логика (`execution_state.rs`) не создаёт split-brain при переподключении.
- [ ] Полное исчерпание диска на PostgreSQL-хосте — проверить деградацию (read-only режим БД) и алертинг, не просто падение процесса.
- [ ] Восстановление PostgreSQL из бэкапа под активным трафиком (не offline drill, как в этом документе) — с реальными rolling restart зависимых сервисов.
- [ ] Потеря `redis-billing` без бэкапа (симуляция «забыли настроить durability») — подтвердить фактический денежный/consistency-эффект на billing-ledger reconciliation (`billing.reconciliation_audit`, V021), не только теоретический.
- [ ] Failover оператора SMPP-сессии (`operator-smpp-session-manager`) во время активной доставки — проверить, что сообщения не теряются и не дублируются на границе failover.
- [ ] Chaos на уровне k8s (pod eviction, node drain) — неприменимо, пока `BACKOFFICE_ROADMAP.md` P0#1 (кластер не собирается в цельную систему) не закрыт; ставить это до P0#1 бессмысленно.

---

**Итог для `BACKOFFICE_ROADMAP.md` P1 «Capacity/DR»:** backup/restore для PostgreSQL и Redis (durable-инстанс) реально протестированы локально с измеренным RTO и построчной верификацией данных (не просто «команда не упала»). RPO/RTO для остальных комбинаций (ClickHouse, облако целиком, DR-топология, chaos) остаются нулём — честно задокументированным, не молчаливым.
