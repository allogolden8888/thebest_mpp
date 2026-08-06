# Delivery Service

**Основание:** `development_plan.md` Фаза 2.1, седьмой сервис "ходового скелета" (Главный агент) — готовит и отправляет операторский submit (`service_internal_methods.md` §1.8). Второй Java-сервис серии (после billing-service) и **первый сервис во всей сессии, реально генерирующий и компилирующий gRPC-код** (`protoc-gen-grpc-java`) — Delivery вызывает `OperatorSubmitService.Submit` как gRPC-клиент (`service_io_contracts.md` §1.8).

**Статус:** реально компилируется и тестируется — `mvn test`, **30/30 тестов проходят** (было 26 — 4 добавлены по итогам находки о секретах ниже), компилирует настоящие `platform-contracts/common/*.proto` + `platform-contracts/grpc/operator_gateway.proto` (`protobuf-maven-plugin` с `pluginId=grpc-java`, `pluginArtifact=io.grpc:protoc-gen-grpc-java`).

**Исправлено (найдено при реализации `dlr-manager`, полный разбор — его README, "Реальная находка (систематическая...)"):** `Main.java` раньше читал единственную `REDIS_RUNTIME_URL`, которую k8s никогда не установит — реальный секрет инжектится дискретными `REDIS_RUNTIME_HOST`/`PORT`/`PASSWORD` (`envFrom: secretRef`). `RedisUrl.buildRuntimeUrl()` теперь собирает connection string из них (тот же паттерн, что `billing-service/RedisUrl.java`), `REDIS_RUNTIME_URL` оставлена как явный override. 4 новых теста.

```bash
cd services/delivery-service
mvn test
```

## Реальная находка окружения: тот же TLS-инспектирующий прокси, другой JDK

Тот же корневой сертификат прокси (`Unitel Root Certification authority`), что уже был найден и исправлен для billing-service (см. его README), пришлось **переимпортировать** — `mvn` в этом сервисе резолвился через другую установку JDK (Homebrew `openjdk@26`, отдельный `cacerts` от того, что использовался для billing-service), и `PKIX path building failed` повторился один в один. Тот же `keytool -importcert` фикс, другой путь до `cacerts`. Задокументировано здесь явно — это не разовая случайность конкретного JDK, а свойство сети, актуальное для любого нового JDK/Maven-инстанса в этом окружении.

## Реальная находка компилятора: `DeliveryExtension` не нёс `resolved_operator_id`

`resolve_gateway_instance` должен резолвить `operator_route:{operator_id}:{route_id}` в Runtime Redis (`data_infrastructure_spec.md` §287), и `OperatorSubmitService.SubmitRequest.operator_id` (`grpc/operator_gateway.proto`) тоже нужен явно — но `DeliveryExtension` (`stage_contract.proto`, до этого сервиса) нёс только `route_id`/`protocol`/`route_version`. Ни Policy/Billing/Routing не столкнулись с этим, потому что все они получают `resolved_operator_id` в своём собственном extension — только Delivery оказался этой стадией без него, хотя Pipeline Engine уже накапливал значение в `ExecutionState` с этапа DestinationResolution. Исправлено **в контракте**, не обходным путём в этом сервисе: добавлено поле 4 `resolved_operator_id` в `DeliveryExtension` (`platform-contracts/common/stage_contract.proto`), `pipeline-engine/src/build_stage_execute.rs` обновлён, чтобы его прокидывать — pipeline-engine прошёл на 19/19 (было 18), новый тест `delivery_command_carries_resolved_operator_id_accumulated_since_destination_resolution`. Оба изменения — отдельный коммит перед этим сервисом.

## Что реализовано по service_internal_methods.md §1.8

| Метод | Где | Примечание |
|---|---|---|
| `handle_delivery_execute` | `KafkaIo.processRecord` | Оркестрация всех методов ниже в задокументированном порядке |
| `fetch_message_context` | `MessageContextStore` | `msgctx:{message_id}` HASH, реальный Lettuce-клиент, не live-проверено |
| `check_control_state` | `ControlSnapshot` | Fail-open заглушка, тот же паттерн, что `AlwaysAdmit` в partner-rest-receiver — см. "Что НЕ реализовано" |
| `segment_message` | `SegmentMessage` | Реальная посимвольная сегментация GSM-7/UCS-2 — см. ниже |
| `generate_queue_msg_id` | `KafkaIo.processRecord` | UUID v4 |
| `resolve_gateway_instance` | `GatewayRegistry` | `operator_route:{operator_id}:{route_id}` HASH, реальный Lettuce-клиент, не live-проверено |
| `call_submit` | `OperatorSubmitClient` | Реальный сгенерированный gRPC blocking stub, `ManagedChannel` кэшируется по endpoint — см. ниже |
| `interpret_submit_result` | `DeliveryService.interpretSubmitResult` | Чистая функция, все 4 `SubmitOutcomeStatus` покрыты тестами |
| `publish_stage_completed` | `KafkaIo.processRecord` | Offset-per-partition паттерн из billing-service, применён с самого начала |

## `segment_message` — вторая, независимая реализация сегментации в этой сессии

`partner-rest-receiver/src/segmentation.rs` уже реализовал GSM-7/UCS-2 подсчёт на приёме (для `msgctx.segment_count`, кэшированного значения). `SegmentMessage.java` здесь — не переиспользование того кода (разные языки, разные сервисы), а независимый **порт того же алгоритма** (GSM 03.38 / 3GPP TS 23.038, тот же basic+extended alphabet, те же лимиты 160/153 GSM-7 и 70/67 UCS-2), потому что этому сервису нужны не просто числа, а реальные байты сегментов для `MessageSegment.content`. Известное, задокументированное упрощение: для GSM-7 `content` — UTF-8 текстовые байты, не 7-bit-packed септеты (реальная упаковка в PDU — забота Operator SMPP Session Manager, не реализованного ни в одном репозитории на момент написания); для UCS-2 `content` — настоящие корректные UTF-16BE байты, стандартный wire-формат для SMPP `data_coding=8`, никакой дальнейшей трансформации не требуется. Fail-safe: если `msgctx.encoding` говорит "GSM7", а тело реально содержит не-GSM7 символ (рассинхрон между тем, что решил partner-rest-receiver, и тем, что хранится) — откат на UCS-2 для этого вызова, не паника и не искажённая отправка (`mismatchedEncodingHintFallsBackToUcs2NotCorruption`).

## gRPC — первый реально сгенерированный код в этой сессии

`pom.xml` подключает `protoc-gen-grpc-java` через `protobuf-maven-plugin`'s `pluginArtifact` (`io.grpc:protoc-gen-grpc-java:1.68.1:exe:${grpc.plugin.classifier}`) — тот же os-classifier паттерн, что Maven использует для нативных бинарников, `grpc.plugin.classifier` дефолтится на `osx-aarch_64` (локальная разработка) (тот же принцип, что `protobuf.protocExecutable` в billing-service). **Реальная находка (найдена только при первом настоящем `docker build`, Dockerfile раньше жёстко прописывал `linux-x86_64`):** на Apple Silicon хосте Docker Desktop по умолчанию собирает нативный `arm64`-контейнер (без эмуляции) — скачанный `linux-x86_64` grpc-плагин внутри такого контейнера падал через Rosetta (`rosetta error: failed to open elf`). Исправлено — Dockerfile теперь определяет классификатор динамически через `uname -m` внутри самого `RUN` (`linux-aarch_64`/`linux-x86_64`), совпадает с реальной архитектурой контейнера независимо от того, где собирается образ (Apple Silicon локально или x86_64 в проде/CI), без явного `--platform` флага снаружи. `OperatorSubmitClient` использует `newBlockingStub(...).withDeadlineAfter(...)` — реальный bounded-timeout вызов, тот же принцип, что bounded `producer.send(...).get(timeout)` в billing-service (найдено кодревью как отсутствующее там до фикса — здесь применено сразу, не задним числом). Сервер контракта (`OperatorSubmitService`, реализуют Operator SMPP Session Manager / Operator HTTP Gateway) не реализован ни в одном репозитории на момент написания (владелец — Субагент 1) — вызов компилируется против настоящего сгенерированного стаба и типобезопасен, но live не проверен.

## Тесты — что доказано

26 тестов:
* `SegmentMessage` (11) — границы GSM-7/UCS-2 (160/161, 306/307, escape-символ, кириллица, 70/71-code-unit границы UCS-2, fail-safe откат на несовпадение encoding-подсказки).
* `DeliveryService` (8) — все 4 `SubmitOutcomeStatus` → `Outcome` маппинги (включая `UNRECOGNIZED`/`UNSPECIFIED` → `SUBMISSION_OUTCOME_UNKNOWN`, не крашится на неизвестном enum-значении), gRPC-транспортный сбой тоже маппится в `SUBMISSION_OUTCOME_UNKNOWN`, не `FAILED` (нельзя утверждать, что оператор бы отклонил — обрыв мог случиться уже после реального accept), **регрессия на `resolved_operator_id`** (`buildSubmitRequestCarriesOperatorIdFromDeliveryExtensionNotRouteId`), `retryable` флаг верно завязан на `outcome`.
* `KafkaIo.OffsetTracker` (4) — тот же паттерн проверки, что в billing-service (см. его README/`KafkaIoTest.java`), применённый здесь с первого коммита, не после отдельной находки.
* `HealthServer` (3) — реальный HTTP round-trip на эфемерном порту.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`check_control_state` — `ControlSnapshot`, fail-open по всем scope.** Тот же паттерн, что уже задокументирован в routing-service (`ControlState`) и partner-rest-receiver (`AlwaysAdmit`) — ни один сервис в этом срезе не реализует реальное потребление `execution.control`.
* **`MessageContextStore`/`GatewayRegistry`/`call_submit` — реальные клиенты (Lettuce/gRPC), ни разу не запущены против живого Runtime Redis или Operator-шлюза.** `OperatorSubmitService`, в частности, не имеет ни одной реальной серверной реализации в этом репозитории на момент написания — весь путь `call_submit` компилируется и типобезопасен, но не может быть проверен end-to-end без сервисов, которые ещё не существуют.
* ~~`MessageContextStore`/`GatewayRegistry` открывают новое TCP-соединение на каждый вызов~~ — **исправлено** (MEDIUM находка кодревью): `MessageContextStore`, `GatewayRegistry` и `SubmitIdempotencyStore` все открывали `client.connect()` внутри try-with-resources на каждый вызов вместо переиспользования — при целевых 20 000 TPS это соединение-на-сообщение, а не пул. Все три теперь держат один `StatefulRedisConnection` на весь жизненный цикл сервиса (Lettuce документированно потокобезопасен для конкурентного использования одного соединения из многих потоков — `SubmitIdempotencyStoreTest.concurrentClaimsOnSameStageExecutionIdOnlyOneWinner` уже проверяет это поведенчески, 20 параллельных потоков на одном соединении). Тот же класс компромисса, что задокументирован в billing-service, только там он остался — здесь закрыт (в отличие от `OperatorSubmitClient`, где `ManagedChannel` кэшировался по endpoint с самого начала — gRPC-канал по своей природе рассчитан на переиспользование).
* ~~`docker build` не выполнялся~~ — **реально собран и провалидирован** (после исправления находки про grpc.plugin.classifier выше).
* **Partner Notification Service / DLR-путь не участвуют здесь** — этот сервис только про submit, не про DLR-обработку (отдельные сервисы: `dlr-correlation-writer`, `dlr-manager`, оба на очереди у Главного агента).
* **Нет ретрая/backoff на gRPC-уровне** — `SUBMISSION_OUTCOME_UNKNOWN`/timeout публикуется как есть в `stage.completed`, решение о повторной попытке принимает Scheduler (владелец — Субагент 1), не этот сервис.
