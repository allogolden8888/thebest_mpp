# Partner REST Receiver

**Основание:** `development_plan.md` Фаза 2.1, шестой сервис "ходового скелета" (Главный агент) — единственная точка входа сообщений в систему для REST-партнёров (`hld.md` §2, `services_specifictaion.md` §2.1). Первый сервис в этой серии, публикующий `incoming.messages` (не потребляющий `stage.*`), и первый с настоящим внешним HTTP API поверх бизнес-логики (не только `/healthz`).

**Статус:** реально компилируется и тестируется — `cargo build && cargo test`, **125/125 тестов проходят**, компилирует настоящие `platform-contracts/{common,events}/*.proto` (тот же паттерн двух package с cross-package ссылками, что у `pipeline-engine`).

**Исправлено (найдено при реализации `dlr-manager`, полный разбор — его README, "Реальная находка (систематическая...)"):** `main.rs` раньше читал единственную `REDIS_RUNTIME_URL`, которую k8s никогда не установит — реальный секрет инжектится дискретными `REDIS_RUNTIME_HOST`/`PORT`/`PASSWORD` (`envFrom: secretRef`). `redis_url::build_redis_runtime_url()` теперь собирает connection string из них, `REDIS_RUNTIME_URL` оставлена как явный override для локальной разработки/тестов. 3 новых теста.

```bash
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/partner-rest-receiver
cargo build
cargo test
```

## P0: production admission из `execution.control` (2026-09-11)

Production main больше не использует `AlwaysAdmit`. Каждая реплика независимо
читает **все** партиции compacted-топика `execution.control` с начала и строит
локальный потокобезопасный snapshot. До полного initial replay и появления
GLOBAL sentinel `/readyz` остаётся 503, а ingress отклоняет запросы — свежий
под не может открыть трафик на пустом/недочитанном состоянии. После bootstrap
при потере Kafka действует fail-static: последнее подтверждённое состояние
сохраняется, consumer переподключается в фоне.

На REST ingress применимы GLOBAL и PARTNER scopes: `PAUSED`/rate=0 отклоняют
запрос с `Retry-After`, частичный `admission_rate` реально семплирует поток,
effective rate — минимум GLOBAL и PARTNER. STAGE/PARTNER_STAGE/OPERATOR_ROUTE
на этой точке ещё не известны и остаются ответственностью downstream consumers.
Kafka key сверяется с protobuf scope/id; неизвестные enum, некорректный rate,
битый payload и запись без key не могут тихо открыть трафик. Tombstone удаляет
scope. Истёкший PARTNER override больше не блокирует поток; истёкший GLOBAL без
свежего автоматического состояния переводит gate/readiness в fail-closed.

`AlwaysAdmit` оставлен только под `#[cfg(test)]` для unit-тестов HTTP-порядка
проверок и отсутствует в production binary path. Проверено полным `cargo test`:
125 passed во всём сервисе, включая startup/empty snapshot, GLOBAL/partner pause, tombstone,
partial rate, malformed payload и expiry.

## P0: self-service PARTNER config применяется без рестарта (2026-09-11)

Статический `PARTNER_CONFIG_PATH` теперь только bootstrap fallback и сам по
себе не открывает ingress. Каждая реплика вручную назначает себе все партиции
compacted `config.changes`, реплеит их с начала и после EOF всех партиций
атомарно устанавливает полный PARTNER snapshot. До этого `/readyz`=503, а
auth path не видит bootstrap-приложения — свежий pod не принимает трафик на
устаревшем example-файле. После bootstrap application/status/IP allowlist/
rate/credential_ref меняются live; при потере Kafka сохраняется последняя
валидная версия.

Config Service version используется для защиты от out-of-order delivery;
archive хранится как versioned tombstone, поэтому поздний старый `active` не
воскресит партнёра. Kafka key, entity_id и payload.partner_id сверяются;
невалидная версия логируется и не заменяет последний snapshot, но не блокирует
более новые записи партиции. Проверено тестами active/archive/revival,
key/payload mismatch, readiness и fail-closed bootstrap.

Открытый межсервисный риск: Config Event Publisher всё ещё ключует compacted
topic только по `entity_id`, а не `(entity_type, entity_id)`. Совпавшие ID
партнёра и другой конфигурационной сущности могут вытеснить друг друга при
compaction; исправление key contract требует coordinated rollout и tombstone
старых ключей, поэтому не маскируется внутри одного consumer.

## Порты

* `9090` — `/healthz`/`/readyz`/`/metrics`, конвенция всех 32 сервисов.
* `8080` — бизнес REST API, `POST /v1/messages`.

## REST-контракт — решение этого среза, не найдено в документах дословно

`service_io_contracts.md` §1.1 описывает данные текстом ("партнёрские креды, msisdn/sender, тело, application_id"), но не фиксирует конкретные заголовки/JSON-поля ни в одном документе. Здесь — конкретный, обоснованный выбор:

```
POST /v1/messages
X-Partner-Id: click_uz
X-Application-Id: click_uz_main
X-Api-Key: <секрет>
Content-Type: application/json

{"msisdn": "998901331835", "sender_id": "Click", "body": "Your OTP is 123456"}
```

Ответы: `202 {"message_id","trace_id"}` (успех, после подтверждённой публикации в Kafka — `service_io_contracts.md` §1.1: "ACK после подтверждённой публикации в Kafka"), `400` (валидация), `401` (аутентификация/партнёр неактивен), `403` (IP/канал не разрешён), `429` (rate limit), `503` (`Retry-After` — admission control или ошибка публикации).

## Что реализовано по service_internal_methods.md §1.1

| Метод | Где | Примечание |
|---|---|---|
| `validate_request_schema` | `request.rs::validate_request_schema` | Чистая функция, JSON-парсинг + msisdn-валидация (E.164 без `+`, 9-15 цифр) |
| `authenticate_partner` | `http.rs::authorize_and_admit` + `auth.rs::EnvAuthVerifier` | См. "Аутентификация" ниже |
| `check_ip_and_application` | `ip_allowlist.rs` | Ручной IPv4 CIDR-парсер (`partner.schema.json` — только IPv4), + проверка `allowed_channels` |
| `check_admission` | `admission.rs::SnapshotAdmissionGate` + full compacted-topic snapshot | Fail-closed до bootstrap, fail-static после него; GLOBAL/PARTNER pause и rate применяются на ingress |
| `check_rate_limit` | `rate_limit.rs::RateLimiter` | Локальный token bucket на `(partner_id, application_id)`, реальная логика (refill/capacity/независимые bucket'ы), не заглушка |
| `sync_rate_limit_counters` | `redis_sync.rs` | Реальный `redis` крейт, таймер ~1с в `main.rs`, не интеграционно проверено (см. ниже) |
| `generate_message_id`/`generate_trace_id` | `build_incoming.rs` | UUID v4 |
| `build_incoming_message` | `build_incoming.rs` | Включает реальный расчёт `segment_count`/`encoding` — см. "Segment count" ниже |
| `publish_incoming` | `kafka_io.rs::publish_incoming` | `rdkafka` FutureProducer, реальный API, не live-проверен |
| `build_ack_response` | `http.rs::error_response_parts` + Axum-обработчик | Статус-коды задокументированы построчно выше |

## Segment count — почему этот сервис, не Pipeline Engine, его считает

`platform-contracts/common/types.proto`'s комментарий у `SmsPayload.segment_count` — "посчитано один раз Pipeline Engine при кэшировании контекста" — на первый взгляд подразумевает, что Pipeline Engine вычисляет это поле. Но реальный код `pipeline-engine/src/kafka_io.rs::handle_incoming` (уже написан и протестирован раньше в этой сессии) читает `sms.segment_count` прямо с `IncomingMessage`, не вычисляет — то есть комментарий описывает, что Pipeline Engine лишь **кэширует** уже готовое значение в Runtime Redis (`service_internal_methods.md` §0), не пересчитывает его. Значит именно на приёме, здесь, поле обязано быть заполнено правильно.

`segmentation.rs` — настоящая, не упрощённая до заглушки логика подсчёта (GSM 03.38 / 3GPP TS 23.038, тот стандарт, которым пользуется буквально любой SMS-шлюз):
* GSM-7 (basic 128-символьный алфавит + расширенная escape-таблица `|^€{}[]~\`, 2 септета на символ): 160 септетов в одиночном сегменте, 153 при конкатенации.
* UCS-2 (как только встречается хоть один символ вне GSM-7, например кириллица): 70 code unit в одиночном сегменте, 67 при конкатенации; используется `encode_utf16().count()`, не `chars().count()` — корректно учитывает суррогатные пары для символов вне BMP (часть эмодзи).

13 тестов покрывают границы (160/161, 306/307, escape-символ, кириллица, суррогатная пара) — не просто happy path.

## Аутентификация — `VaultAuthVerifier` (luminous-hugging-charm.md Ф1), `EnvAuthVerifier` — bootstrap/break-glass

`partner.schema.json` хранит только `credential_ref` (ссылку на Vault path), не сам секрет ("конфиг не хранит credential в открытом виде"). До Фазы 1 реального Vault-клиента не было вообще — `EnvAuthVerifier` читал ожидаемый ключ из переменной окружения, детерминированно построенной из `credential_ref` (`vault://partners/click_uz/main/api_key` → `PARTNER_CRED_VAULT___PARTNERS_CLICK_UZ_MAIN_API_KEY`), инжектированной статически на деплое (`k8s/generate_manifests.py SECRET_DEPENDENCIES` → `envFrom`).

**`vault_auth.rs::VaultAuthVerifier` — теперь дефолт** (`AUTH_VERIFIER_MODE=vault`, дефолтное значение). Читает секрет напрямую из Vault по `credential_ref` (KV v2, тот же путь/деривация, что `credential-issuer-service` пишет при `RotateCredential` — см. `services/credential-issuer-service/README.md` за полным разбором HTTP-контракта; `partner-smpp-gateway`'s `VaultAuthenticator` — третья независимая реализация того же протокола, Java), с in-process TTL-кешем (по умолчанию 30с — реальный сетевой read на КАЖДЫЙ входящий REST-запрос добавил бы латентность на самый горячий путь платформы; ротация credential становится эффективной без передеплоя в пределах этого TTL, не мгновенно). **Fail-closed**: холодный кеш + недоступный Vault → отказ, не default-open (тот же принцип, что `iam-service`'s `CheckPermission` — сервис, аутентифицирующий входящий трафик, не может по умолчанию проваливаться в "открыто" на недоступности своей зависимости); TTL истёк, но Vault временно недоступен, а раньше УЖЕ был успешный read → протухший кеш обслуживает запрос, не рвёт живого партнёра из-за transient-проблемы инфраструктуры.

`AUTH_VERIFIER_MODE=env` — явный bootstrap/break-glass откат на старый `EnvAuthVerifier` (план прямо требует не удалять старый статический путь). Оба разделяют `AuthVerifier` trait, ставший `async fn verify` (был синхронным — секрет теперь требует реального сетевого I/O, `EnvAuthVerifier`'s реализация тривиально `async` без реального ожидания).

Сравнение plaintext-значений в обоих верификаторах — через ручной constant-time compare (защита от самого дешёвого класса timing-атаки — побайтового `==`, останавливающегося на первом несовпадении; не претендует на полную защиту от timing side-channel в общем случае, сеть/GC/JIT добавляют куда больший шум).

## Проверено кодревью (2026-07-27, PART 2): ingress был мёртв как задеплоенный — исправлено

Кодревью нашло (HIGH #4), что как задеплоено, `EnvAuthVerifier` НИКОГДА не мог успешно верифицировать реального партнёра: `k8s/generate_manifests.py`'s `SECRET_DEPENDENCIES["partner-rest-receiver"]` не содержал ни одной записи под `PARTNER_CRED_*`, `infra/secrets/generate_external_secrets.py` не имел ни одного Vault-пути для этой категории — рендеренный манифест не инжектировал эти переменные вообще, и КАЖДАЯ попытка партнёрской аутентификации в описанном деплое падала в deny-ветку (401), при том что сам код fails safe/closed (безопасное направление), но буквальная входная точка всей платформы не могла принять ни одного реального сообщения. Расследование при исправлении вскрыло смежный, ещё более базовый пробел: `PARTNER_CONFIG_PATH` тоже нигде не выставлялся — под откатывался на relative dev-путь, которого в реальном образе нет, что означало бы падение при старте ещё до того, как вопрос авторизации вообще возникал бы.

Исправлено на уровне `k8s/`/`infra/secrets/` (не в этом Rust-коде — сам `auth.rs`/`EnvAuthVerifier` не менялся, был корректен, просто ничего не получал):
* `k8s/generate_manifests.py`: новый `PARTNER_CONFIG_CONFIGMAP_NAME` ConfigMap (`00-partner-config.yaml`), реально монтирующий `config_schemas/examples/partner.valid.json` в `/etc/mpp/partner-config/` + `PARTNER_CONFIG_PATH` env var, для обоих реальных потребителей этого файла (`partner-rest-receiver`, `partner-notification-service`).
* `partner-rest-receiver` получил новый секрет-ключ `partner-credentials` в `SECRET_DEPENDENCIES`.
* `infra/secrets/generate_external_secrets.py`: новый `build_partner_credentials_external_secret()` — по одной Vault KV v2 записи на `credential_ref` каждого application'а в фикстуре (`vault_kv_path_and_property`), с именем env var, построенным `credential_ref_to_env_var` — Python-портом ТОЙ ЖЕ функции из `auth.rs`, сверенным с тем же тестовым вектором (`k8s/test_partner_credentials_wiring.py::test_credential_ref_to_env_var_matches_rust_test_vector`).
* На момент этого старого прохода dynamic `config.changes` ещё не был реализован; пробел закрыт отдельным P0-срезом 2026-09-11 выше. Статический mount сохранён только как bootstrap fallback, не как production source of truth.

`python3 k8s/generate_manifests.py && python3 k8s/test_partner_credentials_wiring.py` — 5/5, плюс `kubeconform -strict` на обновлённый рендер (98→82 native-valid объекта, см. `k8s/README.md`).

**Отдельно, HIGH #1 — `sender_id` без валидации длины/символов (resource amplification).** Раньше проверялся только на непустоту, форвардился as-is в Kafka без ограничения — ~2MB `sender_id` от авторизованного партнёра превращал бы каждый запрос в ~2MB Kafka-сообщение. `request.rs`: `MAX_SENDER_ID_CHARS=21` (реальная граница SMPP `source_addr`, не придуманная) + `is_valid_sender_id` (печатаемый ASCII, тот же диапазон, что легитимные буквенно-цифровые/числовые sender ID).

**Отдельно, HIGH #3 — rate limiting пропускался для запросов с неверным ключом (brute-force вектор).** `partner_id`/`application_id` не секретны (обычные заголовки) — `check_rate_limit` в документированном порядке шёл ПОСЛЕ `authenticate_partner`, поэтому неограниченное число попыток подбора ключа против известной пары не встречало throttling вообще. `rate_limit.rs`: новый независимый `auth_attempt_buckets` (ёмкость = `rate_limit_tps * 4`, headroom, чтобы легитимный трафик в пределах своего лимита никогда не задевал эту проверку — см. `legitimate_traffic_at_configured_tps_never_sees_auth_rate_limited`), проверяется в `authorize_and_admit` ДО сравнения ключа. Не единый bucket с legitimate-трафиком: атакующий, тратящий свой bucket на угадывание, не влияет на бюджет реального партнёра по той же паре.

**Отдельно, HIGH #2 — отсутствие идемпотентности на partner-инициированный retry.** `generate_message_id` всегда генерировал свежий `UUID v4`, единственный downstream dedup-ключ (Billing `charge_id = stage_execution_id`) выводится из ЭТОГО message_id — retry партнёра после разрыва соединения (успешная публикация + 202, ACK не дошёл или таймаут клиента) трактовался как совершенно новое сообщение: реальный дубль SMS/списания. Исправлено новым `idempotency.rs`: опциональный заголовок `X-Idempotency-Key` (партнёр сам генерирует, стабильный на повтор одного логического запроса — тот же паттерн, что Stripe и большинство платёжных REST API) + атомарный `SET NX EX` в Runtime Redis (тот же Redis, что уже используется для `sync_rate_limit_counters`), TTL = `DEFAULT_MESSAGE_TTL` (24ч). Первая попытка с ключом публикует как обычно; повтор с тем же ключом получает **тот же** `message_id`/`trace_id` из Redis и **не публикует повторно в Kafka**. Redis-недоступность — best-effort, не блокирует ingress (публикует как если бы ключ не был передан — та же safe-degradation философия, что и remote-IP/rate-limit код этого сервиса). Ключ полностью опциональный — партнёры, ещё не использующие его, ведут себя ровно как раньше (не решает проблему для них, но и не меняет их контракт). `MAX_IDEMPOTENCY_KEY_CHARS=128` — та же resource-amplification защита, что у нового `sender_id` лимита. Реальные round-trip тесты против локального Redis (`idempotency::tests::second_claim_with_same_key_reuses_first_ids_not_a_fresh_claim` и др.) — не мок.

## Закрыто позже (второй раунд кодревью): X-Forwarded-For defense-in-depth, timeout/body-size/concurrency

**MEDIUM #5 — X-Forwarded-For доверял левому (клиентскому) адресу без defense-in-depth.** `extract_remote_ip` (`http.rs`) раньше брал ПЕРВЫЙ адрес из заголовка — безопасно только пока ingress-nginx *перезаписывает* заголовок целиком (`use-forwarded-headers: false`, дефолт Helm-чарта, но нигде явно не закреплённый в Terraform); если это когда-нибудь сменится на дозапись (например ради CDN/WAF перед одним из четырёх внешних сервисов), левый адрес станет тем, что заявил о себе сам клиент. Теперь берётся ПОСЛЕДНИЙ (правый) адрес — тот, что дописала бы наша собственная инфраструктура (единственный доверенный hop) непосредственно перед тем, как запрос попал сюда. В текущем режиме "перезапись" поведение не меняется (единственная запись что слева, что справа); `extract_remote_ip_trusts_rightmost_hop_not_client_claimed_leftmost` — прямая регрессия на конкретный сценарий из находки.

**MEDIUM #6 — не было ни таймаута на Kafka-путь, ни ограничения на body-size, ни лимита на одновременные запросы.** Kafka publish (5с producer-queue wait + 5с delivery timeout) awaited'ился в handler'е без верхней границы — деградировавший/недоступный Kafka мог держать запрос неограниченно. Три независимых защиты в `http.rs`:
* `DefaultBodyLimit::max(64КиБ)` — axum's дефолт 2МБ был именно тем, что делало HIGH #1 (`sender_id` amplification) эксплуатируемым на всю глубину; 64КиБ — щедрый запас над реалистичным размером запроса (`body` ≤1600 символов), но на два порядка меньше дефолта.
* `tokio::time::timeout(15с, kafka_io::publish_incoming(...))` — верхняя граница поверх librdkafka's собственных внутренних таймаутов (которые применяются per-attempt, не гарантируют суммарный возврат за 10с при внутренних ретраях).
* `AppState::concurrency_limit` (`tokio::sync::Semaphore`, `try_acquire_owned`, не `.acquire().await`) — 1024 одновременных in-flight запроса на реплику; сверх лимита — немедленный 503, не неограниченная очередь (которая свела бы защиту на нет).

## Реальная находка (найдена только реальным прогоном платформы через локальный docker-compose): никто не писал `msgctx`

`msgctx:{message_id}` в Runtime Redis (`data_infrastructure_spec.md` §284) — единственный источник полного содержимого сообщения (`body`/`sender`/`msisdn`/`encoding`) для downstream-стадий (Policy, Delivery, Billing и т.д.); Kafka между стадиями несёт только `message_id`/`stage_execution_id`, не сам текст. Каждый читатель (`policy-service::RedisMessageContextStore`, `delivery-service::MessageContextStore` и другие) был реализован и задокументирован — но нигде в репозитории не было ни одной ЗАПИСИ в этот ключ. Статичным чтением кода это было не видно (каждый сервис по отдельности выглядел корректным); проявилось только при первом реальном сообщении, прошедшем через `pipeline-engine` → `policy-service` в docker-compose: `не удалось получить MessageContext`.

Исправлено здесь — `msgctx.rs`: пишет `msgctx:{message_id}` (те же имена полей, что ожидает `policy-service::RedisMessageContextStore::fetch` — `msisdn`/`sender`/`body`, плюс `encoding`/`partner_id`/`segment_count`/`channel` для остальных читателей) ДО Kafka publish, TTL = `DEFAULT_MESSAGE_TTL` (24ч, тот же, что уже используется для `message_ttl` в `IncomingMessage`). Best-effort, как `idempotency::claim` — недоступность Redis логируется, не блокирует ingress (сообщение уже прошло валидацию, публикация в Kafka не должна зависеть от этой побочной записи). `write_then_hgetall_round_trips_fields_readers_expect` — реальный round-trip против локального Redis, поля сверены с настоящим читателем, не придуманы заново.

## Тесты — что доказано

125 тестов, по модулям (счётчик вырос за счёт execution-control и PARTNER config reload):
* `segmentation.rs` (12) — GSM-7/UCS-2 определение и подсчёт сегментов на границах.
* `request.rs` (16) — валидация схемы: отсутствующие заголовки, невалидный JSON, невалидный msisdn, лимит длины тела на границе, + новое: `sender_id` длина/символы (граница 21, control char, non-ASCII, дефис/пробел, numeric — 7 тестов).
* `auth.rs` (5) — построение имени переменной окружения, верный/неверный ключ, отсутствующая переменная (не паникует).
* `ip_allowlist.rs` (7) — CIDR-парсинг и матчинг, включая `/32`, невалидные записи (не паникуют), проверка канала.
* `rate_limit.rs` (7) — token bucket: исчерпание, рефилл со временем, независимость bucket'ов по паре, `drain_consumed` (для Redis-синхронизации), защита от переполнения capacity долгим простоем, + новое: `auth_attempt_buckets` независимость от message bucket, legitimate-трафик никогда не задевает auth-attempt bucket.
* `build_incoming.rs` (4) — построение `IncomingMessage`, `message_ttl`, кодировка по телу.
* `idempotency.rs` (5, 2 — **реально против локального Redis**) — encode/decode round-trip, scoping по паре, повторный claim с тем же ключом переиспользует ids, независимость разных ключей.
* `redis_url.rs` (3), `admission.rs` (8), `config_reload.rs` (3), `health.rs` (5, включая execution-control/config/Vault readiness), `partner_config.rs` (5) — versioned live snapshot, archive tombstone и загрузка реального `config_schemas/examples/partner.valid.json`.
* `vault_auth.rs` (20) — `credential_ref` парсинг (границы: несколько сегментов пути, отсутствие `vault://`, отсутствие `/`, пустой property после trailing slash), реальный round-trip чтения/кеша/TTL против `vault server -dev` (успешный read, cache-hit не зовёт сеть повторно, TTL истёк — зовёт снова, отсутствующий path/property — fail closed, недоступный Vault на холодном/протухшем кеше), Kubernetes-login flow против фейкового HTTP-сервера (кеширование токена, релогин после истечения, отсутствующий JWT-файл, отклонённый login).
* `http.rs` (13) — `authorize_and_admit`: полный порядок проверок из `service_internal_methods.md` §1.1 (auth-attempt rate limit → auth → IP/канал → admission → rate limit), включая **неизвестный партнёр отклоняется тем же кодом ошибки, что неверный API-ключ** (`AuthFailed`, не отдельным "партнёр не найден") — не даёт внешнему атакующему через код ошибки определить, существует ли `partner_id`; + новое: brute-force против известной пары в итоге throttle'ится, легитимный трафик по своему tps никогда не видит `AuthRateLimited`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **Live Kafka integration обоих full-mirror consumer ещё не прогонялась.** Unit-тесты доказывают decode/key/version/rate/scope/expiry и HTTP/readiness paths, но нужен production-like E2E: config application change + GLOBAL PAUSED/ACTIVE без рестарта.
* **`sync_rate_limit_counters` — реальный `redis` API, ни разу не запущен против живого Runtime Redis.** Тот же класс оговорки, что у `RedisMessageContextStore` в policy-service. (`VaultAuthVerifier`, в отличие от этого пункта, реально протестирован против живого локального Vault — см. "Аутентификация" выше.)
* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступен Docker daemon в этом окружении (см. `services/destination-resolution-service/README.md`).
* **Compacted key `config.changes` не namespaced по entity_type.** Publisher использует только `entity_id`; коллизия между типами может удалить PARTNER snapshot при compaction. Нужен отдельный coordinated contract rollout.
* **`sync_rate_limit_counters` открывает новое TCP-соединение на каждый цикл синхронизации (~1с)**, не переиспользует мультиплексированное соединение — тот же класс компромисса, что уже задокументирован как отложенный в `billing-service/README.md` ("нет пулинга соединений").
* **Rate limiting — только token bucket, без синхронизации лимита между репликами до первого цикла sync** (~1с окно, в течение которого несколько реплик могут независимо пропустить сообщения сверх номинального лимита партнёра) — задокументированное, не скрытое ограничение той же архитектуры, что описана в `service_io_contracts.md` §1.1 для этого паттерна.
* `Application.display_name`/`AuthConfig.auth_type` — поля честно смоделированы по `partner.schema.json`, но не используются текущей REST-логикой (multi-auth-type не реализован); `Partner.version` хранится в payload, а ordering live reload намеренно использует отдельный `ConfigChangeEvent.version`.
