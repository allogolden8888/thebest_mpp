# Partner REST Receiver

**Основание:** `development_plan.md` Фаза 2.1, шестой сервис "ходового скелета" (Главный агент) — единственная точка входа сообщений в систему для REST-партнёров (`hld.md` §2, `services_specifictaion.md` §2.1). Первый сервис в этой серии, публикующий `incoming.messages` (не потребляющий `stage.*`), и первый с настоящим внешним HTTP API поверх бизнес-логики (не только `/healthz`).

**Статус:** реально компилируется и тестируется — `cargo build && cargo test`, **62/62 тестов проходят** (было 59 — 3 добавлены по итогам находки ниже), компилирует настоящие `platform-contracts/{common,events}/*.proto` (тот же паттерн двух package с cross-package ссылками, что у `pipeline-engine`).

**Исправлено (найдено при реализации `dlr-manager`, полный разбор — его README, "Реальная находка (систематическая...)"):** `main.rs` раньше читал единственную `REDIS_RUNTIME_URL`, которую k8s никогда не установит — реальный секрет инжектится дискретными `REDIS_RUNTIME_HOST`/`PORT`/`PASSWORD` (`envFrom: secretRef`). `redis_url::build_redis_runtime_url()` теперь собирает connection string из них, `REDIS_RUNTIME_URL` оставлена как явный override для локальной разработки/тестов. 3 новых теста.

```bash
export PKG_CONFIG_PATH="/opt/homebrew/opt/librdkafka/lib/pkgconfig:$PKG_CONFIG_PATH"
cd services/partner-rest-receiver
cargo build
cargo test
```

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
| `check_admission` | `admission.rs::AlwaysAdmit` | Fail-open заглушка — см. "Что НЕ реализовано" |
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

## Аутентификация — реальный выбор вместо изобретённого решения

`partner.schema.json` хранит только `credential_ref` (ссылку на Vault path), не сам секрет ("конфиг не хранит credential в открытом виде") — реального Vault-клиента в этом срезе нет. `auth.rs::EnvAuthVerifier` читает ожидаемый ключ из переменной окружения, детерминированно построенной из `credential_ref` (`vault://partners/click_uz/main/api_key` → `PARTNER_CRED_VAULT___PARTNERS_CLICK_UZ_MAIN_API_KEY`) — не выдумано с нуля: это ровно тот способ, каким уже реально устроены 5 платформенных секретов в этом репозитории (`k8s/generate_manifests.py SECRET_DEPENDENCIES` → `envFrom` → переменные окружения). Сравнение — через ручной constant-time compare (защита от самого дешёвого класса timing-атаки — побайтового `==`, останавливающегося на первом несовпадении; не претендует на полную защиту от timing side-channel в общем случае, сеть/GC/JIT добавляют куда больший шум).

## Тесты — что доказано

59 тестов, по модулям:
* `segmentation.rs` (13) — GSM-7/UCS-2 определение и подсчёт сегментов на границах.
* `request.rs` (9) — валидация схемы: отсутствующие заголовки, невалидный JSON, невалидный msisdn, лимит длины тела на границе.
* `auth.rs` (5) — построение имени переменной окружения, верный/неверный ключ, отсутствующая переменная (не паникует).
* `ip_allowlist.rs` (7) — CIDR-парсинг и матчинг, включая `/32`, невалидные записи (не паникуют), проверка канала.
* `rate_limit.rs` (5) — token bucket: исчерпание, рефилл со временем, независимость bucket'ов по паре, `drain_consumed` (для Redis-синхронизации), защита от переполнения capacity долгим простоем.
* `build_incoming.rs` (4) — построение `IncomingMessage`, `message_ttl`, кодировка по телу.
* `admission.rs` (1), `health.rs` (2), `partner_config.rs` (3) — снапшот-загрузка на реальном `config_schemas/examples/partner.valid.json` (та же кросс-артефактная сверка, что у остальных сервисов).
* `http.rs` (10) — `authorize_and_admit`: полный порядок 4 проверок из `service_internal_methods.md` §1.1 (auth → IP/канал → admission → rate limit), включая **неизвестный партнёр отклоняется тем же кодом ошибки, что неверный API-ключ** (`AuthFailed`, не отдельным "партнёр не найден") — не даёт внешнему атакующему через код ошибки определить, существует ли `partner_id`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`check_admission` — `AlwaysAdmit`, пустой снапшот `execution.control`, fail-open по всем scope.** Тот же паттерн, что уже задокументирован в `routing-service/src/routing.rs` (`ControlState` для отсутствующих записей) — ни один сервис в этой сессии не реализует реальное потребление `execution.control` (Execution Control Service, владелец Субагент 1, сам ещё не публикует его для всех scope — см. `CODE_REVIEW.md`). Путь обработки `AdmissionDecision::Reject` (429/503 + `Retry-After`) уже построен и протестирован — включение реального consumer'а не потребует трогать `http.rs`.
* **`sync_rate_limit_counters`/`EnvAuthVerifier` — реальный `redis` API, ни разу не запущены против живого Runtime Redis.** Тот же класс оговорки, что у `RedisMessageContextStore` в policy-service.
* **`docker build` не выполнялся** — недоступен Docker daemon в этом окружении (см. `services/destination-resolution-service/README.md`).
* **Партнёрский снапшот грузится один раз из статического файла**, не из `config.changes` (entity_type=PARTNER) — тот же паттерн упрощения, что у Routing/Policy (Фаза 2.2, один тестовый партнёр).
* **`sync_rate_limit_counters` открывает новое TCP-соединение на каждый цикл синхронизации (~1с)**, не переиспользует мультиплексированное соединение — тот же класс компромисса, что уже задокументирован как отложенный в `billing-service/README.md` ("нет пулинга соединений").
* **Rate limiting — только token bucket, без синхронизации лимита между репликами до первого цикла sync** (~1с окно, в течение которого несколько реплик могут независимо пропустить сообщения сверх номинального лимита партнёра) — задокументированное, не скрытое ограничение той же архитектуры, что описана в `service_io_contracts.md` §1.1 для этого паттерна.
* `Application.display_name`/`AuthConfig.auth_type`/`Partner.version` — поля честно смоделированы по `partner.schema.json`, но не используются логикой этого среза (multi-auth-type/hot-reload не реализованы) — помечены `#[allow(dead_code)]` с объяснением в коде, не удалены.
