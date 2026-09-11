# Backoffice — полный план

Дополняет `BACKOFFICE_API_CATALOG.md` (там — контракты для дизайна). Здесь — что строим, в каком порядке, и почему именно так, с реальным статусом данных под каждой темой (не предположения).

## Production Readiness Review (2026-09-10) — платформа целиком, не только backoffice-экраны

Внешний аудит production-готовности всей платформы (не backoffice UI конкретно — K8s/Terraform/секьюрити/self-service data-plane). Вердикт: **no-go для продакшена, ~2-3/10**. Архитектурный фундамент оценён как продуманный глубже обычного MVP, но есть системные разрывы между кодом, self-service, data plane и инфраструктурой. Зафиксировано здесь как отдельный backlog — требует отдельного планового захода (инфра/K8s/Terraform — не CRUD-экраны), не смешивается со скоупом остального этого файла.

### P0 — блокирует запуск

1. **Production-деплой не собирается в цельную систему.** 41 сервис в репозитории, 36 в K8s-каталоге, 32 в registry — списки поддерживаются вручную и разъехались (`k8s/generate_manifests.py:107`, `infra/terraform/registry.tf:15`); в K8s нет `partner-self-service-api`/`billing-self-service-api`/`compliance-api`/`partner-portal-ui`/`template-management-service`, в registry дополнительно нет IAM/credentials/incidents/ops-сервисов. Ещё серьёзнее: у всех node pool `NoSchedule` taint, но pod-темплейты не задают `tolerations`/`nodeSelector` — на таком кластере прикладные поды вообще не заскедулятся (`infra/terraform/k8s-cluster.tf:88`, `k8s/generate_manifests.py:434`).
2. **Секреты и обязательная конфигурация не подключены.** `backoffice-api`/`partner-api` требуют JWT-ключи при старте, `operator-http-gateway` — обязательный webhook token, но генератор секретов создаёт только DB/Redis/ClickHouse/статические partner credentials (`k8s/generate_manifests.py:361`, `backoffice-api/cmd/backoffice-api/main.go:57`, `operator-http-gateway/cmd/operator-http-gateway/main.go:107`). `partner-smpp-gateway` читает `PARTNER_CONFIG_PATH`, но K8s монтирует конфиг только двум другим сервисам — под фактически неработоспособен (`k8s/generate_manifests.py:33`, `partner-smpp-gateway/.../Main.java:63`).
3. **NetworkPolicy блокирует межсервисный трафик.** Default-deny ingress+egress включён, но egress-правила для gRPC не строятся вообще (только ingress со стороны callee) — тест проверяет только ingress и даёт ложную уверенность (`k8s/network_policies.py:71,135`, `k8s/test_network_policy_reachability.py:46`). Аналогично Prometheus в namespace `monitoring`, а scrape-разрешение ищет его по `podSelector` внутри `mpp` — мониторинг будет заблокирован.
4. **Self-service не управляет реальным data plane.** Partner self-service пишет новые версии `PARTNER`-конфига в Configuration Service, но REST-gateway/SMPP-gateway/notification-service грузят партнёров из статического файла при старте (`partner-rest-receiver/src/main.rs:89`, `partner-notification-service/internal/config/partner.go:1`). Партнёр может создать application/sender/webhook через UI и получить успех, но live-трафик этого не увидит без ручного рестарта — главный функциональный разрыв self-service.
5. **Identity-контур не production-класса.** Partner portal предлагает вставить JWT вручную и хранит его в `localStorage` (`partner-portal-ui/src/views/LoginView.vue:1`, `stores/auth.ts:8`); partner-self-service-api доверяет роли из JWT, не проверяя `partner_portal_role_assignments` в IAM (`partner-self-service-api/internal/auth/jwt.go:18`). В backoffice деактивация сотрудника блокирует новый логин, но `CheckPermission` не проверяет `staff_accounts.active` — уже выпущенный 8-часовой токен продолжит давать права (`iam-service/internal/store/store.go:86`).
6. **Не все защитные механизмы реально работают — частично закрыто 2026-09-11.** Исправлены исходные три находки: Partner REST теперь применяет GLOBAL/PARTNER `execution.control`, Partner SMPP Gateway периодически и с epoch-fencing продлевает живые bind, Delivery Reconciliation получает отдельный авторитетный `resolved_operator_id`. Остаток P0: Partner SMPP ingress, Pipeline Engine, Routing и Delivery ещё не имеют требуемого сквозного/fail-safe execution-control; у SMPP не завершён cross-pod takeover, у reconciliation — `resolve_gateway_instance` для `query_sm`.
7. **Нет production release pipeline — CI-тестовое покрытие расширено 2026-09-11, сам release pipeline всё ещё не сделан.** Было: CI тестировал только 5 из 41 сервиса. Стало: `.github/workflows/ci.yml` собирает и тестирует 41 из 43 сервисов (все, у кого есть `go.mod`/`Cargo.toml`/`pom.xml`) — 24 новых Go-job'а, 2 новых Rust-job'а, 10 новых Java-job'ов, каждый в том же паттерне (checkout → setup языка → `cd services/<name>` → build → test), что и 5 уже существовавших job'ов; подробности и полный список в разделе «Журнал работ» ниже. `backoffice-ui`/`partner-portal-ui` (Vue/vitest) сознательно не включены — в файле ещё нет ни одного Node/npm job'а, который можно было бы скопировать один-в-один, это отдельный открытый пробел. По-прежнему нет сборки/сканирования/подписи/публикации образов, deploy/smoke/canary/rollback; манифесты используют placeholder registry и `:latest` (`k8s/generate_manifests.py:380`).

### P1 — до стабильной эксплуатации

- **Observability**: многие `/metrics` отдают только `*_up 1`, OpenTelemetry на `noopExporter`, нет alert rules/дашбордов/централизованных логов/реального OTel Collector (уже отложено в `development_plan.md:206`).
- **Security**: Vault standalone/file storage без HA/auto-unseal, TLS выключен (`infra/terraform/vault.tf:5`); Kafka — plaintext listener без SASL/ACL, хотя HLD требует ACL (`infra/kafka/generate_kafka_topics.py:201`); один Postgres-юзер на все сервисы, ClickHouse — `admin` (`infra/terraform/postgresql.tf:40`); ни один Dockerfile не задаёт `USER`, нет pod security context.
- **Конкурентные изменения**: self-service делает read-modify-write всего partner-документа без ETag/expected version/idempotency key (`partner-self-service-api/internal/httpapi/partnerconfig.go:84`) — параллельные изменения могут тихо затереть друг друга.
- **Autoscaling**: KEDA считает topic как `stage.<service-name>`, но, например, Config Cache Projector потребляет `config.changes` (`k8s/generate_manifests.py:559`, `config-cache-projector/internal/kafkaio/consumer.go:16`) — большинство autoscaling-сигналов смотрит не туда.
- **Capacity/DR**: модель до 20k TPS расчётная, не измеренная (`capacity_model.md:4`); локальный результат на 300 TPS нестабилен и выше целевого p99 (`PLATFORM_STATE_FOR_REVIEW.md:9`); нет проверенного backup/restore, RPO/RTO, DR-топологии, chaos-прогонов, production runbook.

### P2 — функциональная полнота

Уже отражено в разделах ниже этого файла: provisioning нового оператора без ручного изменения compose/K8s, модерация шаблонов/sender ID, TPS/statistics dashboards, PDU-журналы, MT session disconnect, requests/approval workflow, полноценные partner message search/status/DLR.

### Рекомендованный порядок (аудита)

1. K8s scheduling + NetworkPolicy + секреты + единый каталог сервисов.
2. Один E2E-сценарий: партнёр/application через портал → реальное сообщение принимается немедленно → обрабатывается → DLR/callback без рестарта.
3. Полноценный IdP/OIDC PKCE, MFA, отзыв сессий, единая IAM-проверка staff/partner.
4. CI для всех сервисов, immutable image digest, security scan/SBOM/signing, staging deploy, smoke/rollback.
5. Реальные метрики/трейсинг/логи, SLO, alerting.
6. Load/failover/chaos/backup-restore испытания — только затем возвращаться к P2 экранам.

Минимальный go-live gate: чистый кластер разворачивается автоматически, все поды Ready, self-service реально меняет data plane, отзыв пользователя/ключа действует за измеримый SLA, релиз откатывается, backup восстанавливается, целевой TPS и failure modes подтверждены на production-подобном стенде.

**Статус на 2026-09-11**: реализация начата. Инфраструктурный и первый data-plane срезы ниже уже сделаны и проверены локально, но общий вердикт остаётся **no-go**: исправлены сборка deployment-каталога, scheduling, часть сетевой связности, KEDA, startup wiring обязательных секретов, немедленный отзыв прав сотрудника и три конкретных P0-дефекта ingress/session/reconciliation. Не закрыты полный secret lifecycle/identity, динамическое применение self-service конфигурации, execution-control во всех требуемых точках, release pipeline, DR и production-like E2E.

### Журнал работ по production readiness — 2026-09-10/11

#### Что уже сделал другой агент и что было принято

К моменту проверки в `main`/`origin/main` была принята серия `0530a81`/`cad31f2`/`0a2f721`/`10923e7`; последний commit смешал несколько параллельных направлений:

- добавлен `chat-service`, миграция `support.chat_messages`, backoffice API/UI для чата и сгенерированные gRPC-клиенты; backoffice-часть собрана, но partner API/UI и сквозной E2E на момент проверки ещё не завершены;
- добавлен `pdu-log-writer`, который читает `operator.pdu.log` и сохраняет PDU в ClickHouse; writer-тесты проходят, но backoffice query/UI находились в незавершённом working tree и не считаются принятыми;
- `config-event-publisher` научен публиковать новые типы конфигурации, а policy-service — применять/архивировать placeholder-конфиг без panic;
- IAM `CheckPermission` теперь проверяет `iam.staff_accounts.active`, поэтому деактивация сотрудника немедленно отзывает доступ даже для уже выданного JWT;
- одновременно принят первый инфраструктурный пакет: полный каталог образов/сервисов, scheduling и базовые NetworkPolicy.

В рабочем дереве также обнаружен параллельный незакоммиченный WIP другого агента: partner chat API/UI и PDU-log browse в backoffice. Эти файлы намеренно не менялись и ниже не отмечены как готовые, пока нет отдельной приёмки и E2E.

#### Что сделано в инфраструктурном и data-plane проходе

1. **Каталог deployment и registry сведён в одну фактическую систему.** Генератор рендерит все 43 сервиса, для которых в репозитории есть runnable Dockerfile; Terraform registry содержит те же 43 имени. Добавлены отсутствовавшие self-service/admin/chat/PDU-компоненты и внутренние `ClusterIP Service` для HTTP/gRPC.
2. **Поды теперь могут планироваться на созданные Terraform node pools.** В workload-шаблоны добавлены согласованные `nodeSelector` и `tolerations` для tainted пулов; отдельные тесты ловят drift каталога, registry и scheduling.
3. **Исправлены service discovery endpoints.** Kafka использует реальный Strimzi bootstrap `mpp-kafka-kafka-bootstrap.mpp.svc:9092`, Prometheus — namespace `monitoring`.
4. **NetworkPolicy стала двусторонней.** Для прямых вызовов генерируются и ingress callee, и egress caller; разрешены DNS, точные Strimzi broker labels, Prometheus scrape, Vault из namespace `mpp`, backoffice proxy и оба partner-portal proxy. Генератор создаёт 41 policy, а reachability-тесты проверяют разрешённые и запрещённые направления.
5. **KEDA привязана к реальным consumer subscriptions.** У каждого масштабируемого сервиса теперь явный список `(topic, consumerGroup)`, включая multi-topic consumers; producer-only `billing-outbox-publisher` больше не получает фиктивный Kafka scaler. Тест сверяет mapping с рендером и списком реально provisioned topics.
6. **Закрыт пропущенный Kafka topic.** В production profile добавлен `stage.delivery-reconciliation.dlq`, который уже читает `lifecycle-writer`, но который раньше не создавался при выключенном auto-create. Итого: 33 topic + Kafka CR.
7. **Partner config подключён всем четырём фактическим потребителям.** Один ConfigMap и `PARTNER_CONFIG_PATH` теперь монтируются в `billing-service`, `partner-rest-receiver`, `partner-notification-service` и `partner-smpp-gateway`; регрессионный тест не позволит снова потерять потребителя.
8. **Обязательные startup-секреты доведены до pod env.** Terraform принимает PEM/token только через sensitive variables и записывает три раздельных Vault KV entry: backoffice RSA keypair, partner IdP verification key и operator webhook token. External Secrets создаёт соответствующие Kubernetes Secrets, а deployment-каталог подключает их ровно к `backoffice-api`, partner/self-service/compliance API и `operator-http-gateway`. Backoffice и partner IdP намеренно остаются разными trust domain.
9. **Усилен infrastructure CI gate.** Workflow закрепляет версию kubeconform, генерирует production Kafka topology до KEDA-тестов, запускает весь K8s/ExternalSecret pytest-набор и валидирует Kubernetes + Strimzi/KEDA/Istio/ESO/PodMonitor/cert-manager по строгим CRD-схемам без `ignore-missing`/skip. Это закрывает silent pass манифеста неизвестного CI типу, но пока не заменяет отсутствующий build/sign/deploy pipeline всех образов.
10. **Partner REST ingress больше не обходит Execution Control.** Production path вместо `AlwaysAdmit` независимо реплеит все партиции compacted `execution.control`, остаётся fail-closed до полного EOF и GLOBAL sentinel, применяет GLOBAL/PARTNER pause и rate, проверяет Kafka key/protobuf и учитывает expiry manual override. После bootstrap при потере Kafka действует fail-static; `/readyz` отражает готовность snapshot. `AlwaysAdmit` остался только в test binary.
11. **Живые Partner SMPP bind больше не исчезают из Runtime Redis по TTL.** Добавлен periodic fixed-delay heartbeat текущих каналов. Heartbeat/unregister fenced по `gateway_instance_id + session_epoch`, stale callback не трогает новый bind, а текущая локальная сессия восстанавливает потерянную Redis-запись после краткого отказа. Одновременно исправлен production Redis URI: пароль из ExternalSecret теперь реально используется и URL-экранируется. Cross-pod duplicate-bind takeover остаётся отдельным открытым протоколом.
12. **Delivery Reconciliation больше не подменяет `operator_id` идентификатором очереди.** В `DeliveryReconciliationExtension` добавлено аддитивное `resolved_operator_id`; Pipeline Engine переносит авторитетный результат Destination Resolution и не строит команду без него. Consumer читает только это поле. Legacy-команда сначала публикуется полноценным `DlqRecord` в `stage.delivery-reconciliation.dlq`, и только после Kafka ack коммитит входной offset; transient failure перематывает партицию. Rollout требует остановки старого consumer, потому что producer-first rolling update позволил бы старому коду продолжить порчу case'ов.
13. **Первое реальное self-service → data-plane применение без рестарта.** Partner REST Receiver теперь держит versioned full-mirror PARTNER snapshot из `config.changes`: каждая реплика реплеит все партиции, fail-closed до EOF, затем live применяет application/status/IP/rate/credential_ref. Archive хранится как versioned tombstone и не может быть отменён поздним старым active-event; malformed/key-mismatch событие не заменяет последнюю валидную версию. Статический ConfigMap оставлен только bootstrap fallback и не открывает ingress. Это закрывает REST-ветку, но SMPP Gateway и Notification Service пока всё ещё статичны.

#### Чем проверено

- K8s regression suite: **27 passed**;
- External Secrets contract suite: **1 passed**;
- строгий `kubeconform` для Kubernetes 1.34 и CRD-схем, включая ExternalSecret/ClusterSecretStore: **229/229 valid, 0 invalid, 0 errors, 0 skipped**;
- полный набор workload + Kafka + Istio + ingress + observability CRD: **267/267 valid, 0 invalid, 0 errors, 0 skipped**;
- Terraform: форматирование без diff, `terraform validate` — **Success**;
- IAM: `go test ./...` и `go test -race ./...` — успешно;
- `pdu-log-writer` и `config-event-publisher`: все Go-тесты — успешно;
- policy-service: **82 passed**, в том числе config reload/archive и placeholders;
- backoffice UI production build — успешно; Vitest router-suite периодически зависает/падает на cleanup (`global.removeEventListener`) и остаётся отдельной задачей стабилизации тестов.
- Partner REST Receiver после PARTNER config reload: `cargo test` — **125 passed**;
- Partner SMPP Gateway heartbeat/registry/Redis/TCP subset — **24 passed**; полный suite содержит ещё 4 environment-dependent `VaultClientTest`, которым нужен dev Vault на `127.0.0.1:8200`;
- Pipeline Engine: `cargo test` — **55 passed**; Delivery Reconciliation без DB-dependent store suite — **35 passed**; protobuf descriptor compile — успешно. Полный reconciliation suite выявил drift локальной БД: 9 store-тестов не видят unique index из V032, что отдельно подтверждает необходимость versioned migration runner/pre-deploy gate.

Это локальная верификация генераторов и компонентов. Реального apply на чистый кластер, ожидания всех Pod Ready и smoke/E2E через production-like зависимости ещё не было — go-live gate не пройден.

#### Что всё ещё блокирует production (оставшийся P0)

- **Secrets/config lifecycle:** Kubernetes/Vault wiring для обязательных JWT/webhook startup values теперь есть, но сами production-значения ещё должны быть безопасно переданы в Terraform/bootstrap, проверены на соответствие issuer'у и отротированы; статический public key нужно заменить на JWKS/`kid` rotation, общий webhook token — на per-operator credential lookup. Текущий `partner.valid.json` — пример, а не production source of truth; в нём нет ни одного `SMPP_BIND`, поэтому один только mount не делает partner SMPP готовым.
- **Внешний egress при default-deny (частично закрыто, см. `k8s/README.md` "Внешний egress при default-deny"):**
  - ✅ **Managed PostgreSQL/Redis (runtime/configuration/billing)/ClickHouse** — закрыто. `k8s/external_hosts.py` читает резолвленные FQDN/IP (per-environment `k8s/external-hosts.json`, приоритет над committed placeholder-фикстурой `k8s/external-hosts.example.json`), `k8s/network_policies.py` генерирует egress `ipBlock` строго на эти IP + точный `podSelector` из `SECRET_DEPENDENCIES`, `k8s/service_entries.py` — Istio `ServiceEntry` (`MESH_EXTERNAL`) на те же хосты. Открытый хвост: `infra/terraform/export_external_hosts.sh` (производитель реального `external-hosts.json` после `terraform apply`) **ещё не написан** — до этого момента прод-прогон молча (WARNING в stderr, не ошибка) падает на fixture-адреса RFC 5737, что для реального деплоя неприемлемо.
  - ⚠️ **SMSC-эндпоинты операторов и partner/operator webhook callback URL — честно НЕ закрыто, только частичная промежуточная мера.** Оба принципиально динамические (SMSC host/port нигде не смоделирован в конфиге и меняется через `route_table`/`config.changes` в runtime; webhook URL — произвольный HTTPS-адрес, зарегистрированный партнёром/оператором в runtime-конфиге) — ни то, ни другое не известно на этапе генерации k8s-манифестов, поэтому статический `ipBlock`/`ServiceEntry` на конкретный хост невозможен в принципе на этом слое. Сделано: egress сужен до конвенциональных SMPP-портов 2775/2776 (SMSC) и порта 443 (webhook) к публичному интернету с явным исключением RFC1918/link-local/loopback диапазонов (`network_policies.py`: `SMPP_CONVENTIONAL_PORTS`, `SCOPED_PUBLIC_HTTPS_CLIENTS`, `PRIVATE_RANGES_EXCEPT`) — это НЕ egress-контроль per-destination, произвольный внешний хост на разрешённом порту проходит; полноценное закрытие требует egress-gateway с runtime-сверяемым hostname allow-list (отдельная задача, не сделана). Широкое `0.0.0.0/0` без ограничения порта намеренно не добавлялось ни для одного случая.
- **Self-service → data plane (частично закрыто):** Partner REST теперь live применяет PARTNER application/status/IP/rate/credential_ref из `config.changes`; Partner SMPP Gateway и Notification Service всё ещё читают статический файл. Нужен тот же full-mirror/versioned reload и измеренный propagation SLA. Дополнительно Kafka compaction key сегодня равен только `entity_id`, не `(entity_type, entity_id)`: коллизия разных типов может вытеснить partner config и требует coordinated key-contract rollout.
- **Identity:** нет полноценного OIDC/PKCE + MFA, issuer/audience contract и partner IAM/session revocation; ручной JWT в `localStorage` остаётся неприемлемым для production.
- **Data-plane correctness:** три исходные находки закрыты (REST admission, SMPP heartbeat, reconciliation operator ID), но execution-control ещё не сквозной: Partner SMPP ingress не реализует `check_admission`, Pipeline Engine не проверяет control перед dispatch, Routing и Delivery используют пустые fail-open snapshots. У SMPP нет cross-pod duplicate-bind takeover, а Delivery Reconciliation всё ещё не реализует `resolve_gateway_instance` для `query_sm`. Legacy reconciliation backlog требует обогащения авторитетным operator ID перед replay.
- **Schema rollout discipline:** локальная БД оказалась без уже существующей V032, из-за чего `ON CONFLICT(message_id)` не работает. Нужен обязательный versioned migration runner с таблицей применённых версий, lock, checksum и pre-deploy проверкой, а не ручной запуск SQL-файлов.
- **Release/operations:** нет pipeline всех сервисов с immutable digest, scan/SBOM/signing, staging deploy, smoke/canary/rollback; не подтверждены alerts/SLO, backup-restore, RPO/RTO, load/failover/chaos.

Следующий минимальный срез: довести контролируемый external egress и production secret bootstrap/rotation, затем закрыть live config reload и оставшиеся execution-control consumers в рамках одного сквозного сценария «создание application в partner portal → немедленный live REST/SMPP auth → pipeline → DLR/callback». После этого — identity, migration/release pipeline и DR.

#### Довершён managed-DB external egress (ServiceEntry wiring) — 2026-09-11

Предыдущий проход по этому пункту оставил `k8s/external_hosts.py`/`k8s/service_entries.py` не подключёнными никуда (dead code — `generate_manifests.py`/CI их не вызывали) и незакоммиченным diff в `k8s/network_policies.py`/`k8s/test_network_policy_reachability.py`. Довершено в этом проходе:

- `service_entries.py` подключён в `.github/workflows/ci.yml` ("Generate manifests" step, рядом с `generate_manifests.py`/`network_policies.py`) — `ServiceEntry` теперь реально рендерится и валидируется в CI, не только при ручном локальном запуске.
- `ServiceEntry.apiVersion` исправлен `v1beta1` → `v1` — согласован с уже выбранным `networking.istio.io/v1` для `DestinationRule` (`infra/istio/peer-authentication-strict.yaml`), кластер на Istio 1.24.1, где `v1` GA с 1.20.
- `k8s/external-hosts.json` (реальный, per-environment, производится `terraform apply` + DNS-резолвом) добавлен в корневой `.gitignore` — раньше не был проигнорирован, хотя комментарий в `external_hosts.py` уже утверждал обратное.
- Тесты: `k8s/test_network_policy_reachability.py` — 6 новых тестов на managed-DB egress/ServiceEntry/scoped-public-HTTPS/SMSC-egress, включая явную проверку, что несвязанный внешний хост (`8.8.8.8`) не проходит через сгенерированное правило. Полный `k8s` pytest-набор: **33 passed** (было 27 — рост за счёт этих 6). `kubeconform` с CRD-схемами (тот же вызов, что в CI): **280/280 valid, 0 invalid, 0 errors, 0 skipped** — прогнано локально именно сейчас, включает 5 новых `ServiceEntry` (v1, не v1beta1) поверх ранее записанных 267/267; часть разницы (267→280 это +13, не +5) — накопленные изменения других параллельных сессий в рендере с момента прошлой записи этого числа, не только это изменение.
- **Честно осталось открытым, без исключений:**
  1. `infra/terraform/export_external_hosts.sh` — файл, на который ссылается `external_hosts.py` как на производителя реального `external-hosts.json`, **не существует**. Пока не написан — прод-прогон будет молча (WARNING, не ошибка) использовать fixture-адреса RFC 5737, что не годится для реального деплоя.
  2. SMSC-эндпоинты операторов и partner/operator webhook callback URL остаются **не закрытыми по-настоящему** — см. пункт P1 "Внешний egress" выше. Сделанная мера (порты 2775/2776 и 443 к произвольному публичному хосту, кроме приватных диапазонов) — сознательно суженный, но всё ещё "любой внешний хост на этом порту", не per-destination контроль.

#### Расширено CI-покрытие сервисов (P0 #7) — 2026-09-11

Было: `.github/workflows/ci.yml` собирал и тестировал только 5 из 41 сервиса (`destination-resolution-service`, `policy-service`, `billing-service`, `routing-service`, `pipeline-engine`); все остальные, включая `chat-service`/`pdu-log-writer`/`iam-service`/`backoffice-api`/`credential-issuer-service` и т.д., не имели никакого CI. Добавлено 36 новых job'ов — build+test для каждого сервиса `services/*`, у которого есть реальный код (`go.mod`/`Cargo.toml`/`pom.xml`), строго тем же паттерном, что и 5 существовавших (checkout → setup языка → `cd services/<name>` → build → test, без caching — его и старые job'ы не используют).

**Покрыто (41 из 43):**
- уже было (5): `destination-resolution-service`, `policy-service`, `billing-service`, `routing-service`, `pipeline-engine`;
- новые Go (24, `actions/setup-go@v5`, `go build ./...` + `go test ./...`): `analytics-writer`, `backoffice-api`, `billing-self-service-api`, `chat-service`, `compliance-api`, `config-cache-projector`, `config-event-publisher`, `configuration-service`, `consent-cache-projector`, `credential-issuer-service`, `dlr-correlation-writer`, `dlr-manager`, `execution-control-service`, `iam-service`, `incident-service`, `lifecycle-writer`, `operator-http-gateway`, `ops-visibility-service`, `partner-api`, `partner-notification-service`, `partner-self-service-api`, `pdu-log-writer`, `replay-service`, `scheduler-critical-sweep`;
- новые Rust (2, `dtolnay/rust-toolchain@stable`, `cargo build` + `cargo test`): `partner-rest-receiver`, `template-management-service`;
- новые Java (10, `actions/setup-java@v4` (temurin 25), `mvn test`): `billing-ledger-writer`, `billing-outbox-publisher`, `billing-reconciliation`, `delivery-reconciliation-service`, `delivery-service`, `message-state-resolver`, `operator-smpp-session-manager`, `partner-smpp-gateway`, `scheduler-background-lane`, `scheduler-standard-lane`.

**Сознательно исключено (2 из 43), с причиной:**
- `backoffice-ui`, `partner-portal-ui` — фронтенд (Vue/vitest, только `package.json`, без `go.mod`/`Cargo.toml`/`pom.xml`). В `ci.yml` на момент правки не было ни одного Node/npm job'а — ни одной существующей структуры, которую можно было бы скопировать один-в-один; изобретать новый паттерн вместо копирования существующего было вне рамок этой правки. Это реальный, не скрытый пробел: обе UI по-прежнему не тестируются в CI.
- Ни одного сервиса «только Dockerfile без исходников» в `services/` не обнаружено — проверены все 43 директории, у каждой есть либо `go.mod`, либо `Cargo.toml`, либо `pom.xml`, либо (у двух UI) `package.json`.

**Верификация:** `.github/workflows/ci.yml` синтаксически проверен `actionlint` (0 замечаний) и `yaml.safe_load` (47 job'ов разобраны). Локально прогнаны build+test-команды ровно как их будет выполнять новый CI-job (тот же working directory, те же команды) для всех 36 новых сервисов, не только для выборки: все 24 Go (`go build ./...` + `go test ./...`), оба новых Rust (`cargo build` + `cargo test`: `template-management-service` — 47 passed, `partner-rest-receiver` — 119 passed) и все 10 новых Java (`mvn test`: 17/13/30/44/83/30/74/71/12/40 тестов соответственно) — везде `BUILD SUCCESS`/`ok`.

**Важная деталь верификации, не баг сервиса:** при первом локальном прогоне `partner-api` (`TestLifecycleHistoryReturnsOrderedEvents`) и `delivery-reconciliation-service` (9 тестов в `ReconciliationStoreTest`) упали с ошибкой Postgres — но это оказался артефакт локальной dev-базы `mpp` на `localhost:5432` (общей для параллельных сессий этого репозитория), в которой на момент прогона не было партиции `message_lifecycle_history` под текущую дату и не была накатана `V032__reconciliation_early_evidence.sql` (уникальный индекс `reconciliation_cases_message_uniq`). На чистом раннере (как реальный GitHub Actions — ни один из 5 существовавших job'ов не поднимает Postgres как `services:`) оба теста корректно **пропускаются** (`assumeTrue`/`t.Skipf` на недоступном подключении) — проверено явно, подключение к заведомо недоступному адресу даёт `BUILD SUCCESS`/`ok` с skip вместо ошибки. Реальных багов в коде сервисов не найдено; повторный локальный прогон `partner-api` без изменений тоже прошёл (транзиентная гонка за общую dev-БД между параллельными сессиями, не воспроизводится стабильно).

---

## Текущее состояние (уже живое, задеплоено, работает)

| Область | Экран | Статус |
|---|---|---|
| Сообщения | Messages | Работает, но `current_status`/таймлайн застревают — чиню отдельно (см. "Известные баги" ниже) |
| DLQ / Replay | DLQ | Готово |
| Reconciliation | Reconciliation | Готово (billing-специфичный, см. ниже) |
| Config versions/diff | Configuration | Готово |
| Execution Control | Execution Control | Готово |
| Scheduler force-command | — | Готово (backend), нет отдельного пункта меню сейчас |
| Access Control (роли/юзеры) | Users & Roles | Готово |
| Audit Log | Audit Log | Готово |
| Credentials rotation | — | Готово (backend), нет отдельного экрана в меню |
| Incidents | Incidents | Готово |
| Ops Health | Ops Health | Готово (сегодня почини́л DNS-баг) |
| Reports | Reports | Backend готов, упирается в отсутствующий ClickHouse локально |

---

## Новый скоуп — по темам из вашего списка

### 1. Billing (админская видимость)

**Данные, которые реально есть:**
- `billing.billing_ledger` — charge_id, account_id, partner_id, amount, currency, entry_type (charge/compensating), source_charge_id, created_at. Это ПОЛНАЯ история списаний, готова к browse прямо сейчас.
- `billing.reconciliation_audit` — уже проксирован (`GET /v1/reconciliation`).
- Тариф — `billing_tariff.schema.json`: `price_per_segment` по category (SERVICE/TRANSACTION/ADVERTISING/UNTEMPLATED/BLOCKED), `default_category`, `recurring_charges`. **Тариф сегодня per-partner** (TariffCache в billing-service читает per-partner конфиг из configuration-service), **НЕ per-operator** — если нужен тариф per-operator, это архитектурное расширение схемы, не просто новый эндпоинт.

**Что строим:**
1. `GET /v1/billing/ledger` — browse `billing_ledger` (тот же паттерн, что DlqBrowse), фильтры `partner_id`/`entry_type`/дата. **Backend: 1 день**, тот же шаблон, что Messages/DLQ.
2. Tariff management — `GET/POST /v1/config/versions?entity_type=billing_tariff` уже покрывает это ЧЕРЕЗ существующий generic config-version механизм (раздел 5 каталога) — нужен только UI-экран поверх уже существующего API, backend не нужен вообще.
3. Per-operator тариф — если реально нужен, отдельная задача: новое поле в тарифной схеме + изменения в TariffCache resolution logic в billing-service. Не делаю, пока не подтверждено, что нужно.

### 2. Операторы (SMPP routes)

**Данные, которые реально есть:**
- `operator_route:{operator_id}:{route_id}` в Runtime Redis (не Postgres!) — owning_instance_id, endpoint, heartbeat, route_epoch, protocol. Живое состояние, пишет/читает `operator-smpp-session-manager`.
- Статический route table (`route_table.valid.json`) — конфиг per-operator (primary/reserve endpoints, failover) — уже версионируется через тот же generic config-version механизм, что policy_template.

**Что строим:**
1. ✅ `GET /v1/operators/routes` — read-only снимок текущих `operator_route:*` ключей из Redis (кто сейчас держит какой route, когда последний heartbeat), + экран `OperatorRoutesView.vue`. SCAN обязателен (нет источника "список всех operator_id" у backoffice-api), тот же cursor-loop паттерн, что уже в `consent-cache-projector`. Живьём проверено — виден реальный `beeline_uz` SMPP route, который сейчас держит `operator-smpp-session-manager`.
2. Route config (primary/reserve/failover) — ✅ то же самое, что банворды выше: `entity_type=route_table` через уже рабочий `ConfigView.vue`, ничего нового строить не нужно.
3. Новый оператор "с нуля" (новый SMPP-коннекшен, креды, лимиты) — сегодня это ручное добавление сервиса в `docker-compose.yml`/k8s-манифест + route_table конфиг. Полноценный self-service "добавить оператора через UI" — отдельная, немаленькая задача (нужен provisioning-слой, которого нет), не оцениваю как "1 день", честно отдельная фаза.

### 3. Spam / Banwords

**Данные, которые реально есть:**
- `policy_ruleset.banwords.words[]` + normalization-настройки (nfkc/homoglyph_fold/strip_separators) — часть того же `policy_ruleset` config-version объекта, что уже редактируется через `/v1/config/versions?entity_type=policy_ruleset`.

**✅ Уже функционально возможно сегодня, без единой новой строки кода.** `ConfigView.vue` (экран "Configuration", дефолтный маршрут `/`) — это уже полностью универсальный редактор `entity_type`/`entity_id`: список версий, создание, архивирование, `validate`, `diff` — `entity_type` там свободное текстовое поле, не хардкод. Указав `entity_type=policy_ruleset` и нужный `entity_id`, banwords/antispam/time_of_day/sender_validation редактируются прямо сейчас через сырой JSON-textarea. Специализированная форма (тег-инпуты для банвордов, чекбоксы для нормализации, как на референс-дизайне) — это UX-полировка поверх уже рабочей функциональности, не закрытие пробела. Не делаю отдельным заходом, если явно не попросите — приоритетнее закрыть экраны, которых вообще нет.

### 4. Blacklist

**✅ Готово: проксировано через backoffice-api + экран.** `GET/POST /v1/compliance/consent` — плоский HTTP-прокси в `compliance-api` (`internal/httpapi/compliance.go`), форвардит `Authorization`. GET без gate прав (то же решение, что уже приняло само `compliance-api`), POST — `compliance:write`. Экран `BlacklistView.vue`: карточка поиска по msisdn (открыта всем) + карточка ручной блокировки/разблокировки (за `RequirePermission`). Поиск ТОЛЬКО по msisdn — обратный поиск "все заблокированные" не поддержан нигде (msisdn не индексируется reverse в Redis) — честное ограничение, не баг.

По пути нашлись и починены два реальных, ранее не пойманных бага, без которых запись структурно не доходила до Redis (обнаружено живой curl-проверкой, не гипотезой):
1. `services/configuration-service/internal/validate/schemas/subscriber_consent.schema.json` (вручную поддерживаемая копия `config_schemas/`, задокументированный drift risk) не был синхронизирован после того, как канонический файл получил поле `status` — каждый `POST /v1/compliance/consent` падал на валидации.
2. `consent-cache-projector` (Kafka `config.changes` → Runtime Redis) существует и задеплоен в k8s, но полностью отсутствовал в локальном `docker-compose.yml` — тот же класс находки, что уже чинился раньше ("docker-compose: restore 13 local self-service services"). Добавлен.

### 5. Category management per operator

Категории (SERVICE/TRANSACTION/ADVERTISING/etc) — общеплатформенные, не per-operator сегодня (используются в `policy_template.category`, `billing_tariff.price_per_segment`, оба per-partner). "Per-operator категории" как отдельная концепция не существует нигде в текущей архитектуре — прежде чем проектировать экран, нужно решить: это то же самое, что per-partner категории (тогда уже есть, просто UI), или реально новая сущность (тогда — архитектурное решение, не эндпоинт).

### 6. Модерация шаблонов (партнёр → админ)

**Полностью новая фича, ничего не существует.** Партнёр сегодня НЕ МОЖЕТ предложить паттерн — `template-management-service`'s `/v1/templates` пишет напрямую, без approval-стадии. Нужно:
- Новая колонка/статус в `policy.policy_template`: `pending_review` (сейчас только `active`/`archived`).
- Партнёрский эндпоинт: `POST /v1/self-service/templates/propose` (partner-self-service-api) — создаёт запись со статусом `pending_review`.
- Админский эндпоинт: `GET /v1/templates?status=pending_review` (уже работает, статус не гейтится сегодня) + `POST /v1/templates/{id}/approve|reject` (новый, не существует).
- UI с обеих сторон: партнёрская "мои заявки" + админская "очередь модерации".

**Оценка: 3-4 дня** (миграция + 2 новых эндпоинта + 2 экрана). Самая содержательная новая фича из всего списка — реальная новая бизнес-логика, не просто CRUD-обёртка над уже существующими данными.

### 7. Roles & Users

Уже полностью готово (`/v1/iam/*`, экран Access Control). Если нужно — что именно не хватает? (например: bulk-назначение ролей, self-service запрос роли с одобрением — тоже approval workflow, тот же паттерн, что #6).

### 8. Dashboards

Нет единого агрегирующего эндпоинта сегодня — "Home" пришлось бы собирать из уже существующих кусков (Ops Health snapshot + последние N сообщений + открытые инциденты + DLQ count). Два варианта:
- (а) Фронтенд сам делает 3-4 параллельных запроса к уже существующим эндпоинтам, собирает в один экран — **backend не нужен, 1 день фронтенда**.
- (б) Отдельный `GET /v1/dashboard/summary`, агрегирующий на бэкенде — чуть быстрее для клиента, чуть больше кода. Рекомендую (а) — не плодить агрегирующий эндпоинт без реальной нужды.

### 9. A2P Admin — доп. спецификация (экраны 21-41 дизайна)

`MPP Backoffice.dc.html` содержит отдельный референс (экраны 21-41, "A2P Admin — доп. спецификация"), стилизованный под реальную SMS-агрегаторскую admin-панель — не то же самое, что экраны 1-20 выше, разобран отдельно и впервые в `BACKOFFICE_DESIGN_SPEC.md` ЧАСТЬ 2b (полный экран-за-экраном разбор с API-контрактами, тем же ✅/🔨/❌ форматом).

Коротко по категориям:
- **Уже закрыто, пересекается с экранами 1-20** (Senders/Patterns/Partners = Экраны 12/7/12, Roles = Экран 16 другим UI-паттерном, Blacklist numbers = Экран 13 c тем же архитектурным барьером на browse-список): Senders, Patterns, Partners, Roles, Blacklist numbers.
- ~~Маленький новый backend~~ ✅ Partner Users (экран 35) готово — новые RPC в IamService + `/v1/iam/partner-portal-assignments` + экран.
- **Решено пользователем и реализовано**: ✅ CTN (экран 23) — реальная сущность для офлайн-биллинга. ✅ Categories (экран 32) — полная замена фиксированного словаря. Spam Patterns (экран 37) = Banwords (Экран 10), отдельно не строим.
- **Решено пользователем, строится/спланировано**: Admin users (экран 33) — LDAP не нужен, полноценный локальный логин/пароль — см. раздел "Admin users" ниже. TPS-дашборд (экран 29) — подтверждён "сразу полная версия", фазовый план ниже. ~~Chat (экран 27)~~ ✅ готово, обе стороны — см. раздел "Chat" ниже. ~~A2P/DLR per-PDU лог (экраны 38-40)~~ ✅ готово — см. раздел ниже. P2A (входящие MO, вторая половина экранов 38-40) — архитектурно отсутствует, фазовый план (дизайн, без кода) ниже.
- ~~Guides CMS (экран 41)~~ ✅ готово — админская сторона (`GuidesView.vue`, `entity_type=CONFIG_ENTITY_TYPE_GUIDE`, 0 backend). Партнёрская видимость гайдов — открытый, не решённый вопрос.
- ~~Regex Patterns (экран 36) → "Pattern Placeholders"~~ ✅ готово, включая изменение движка матчинга — не только CRUD-реестр (`PatternPlaceholdersView.vue`, `entity_type=CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER`), но и реальный `%{name}`-синтаксис в `policy-service/src/template_matching.rs` с извлечением значения + hot-reload через `config.changes` (`config_reload.rs`). Открытый вопрос: прокидывание извлечённого значения дальше по пайплайну (`StageCompletedEvent` и далее) — не решено, требует отдельного подтверждения потребителя. См. `BACKOFFICE_DESIGN_SPEC.md`, Экран 36.
- **Реальная новая фича, ещё не оценивалась пользователем**: MT Sessions Disconnect (экран 30, новый write-путь), Requests (экран 25, тот же backend, что уже оценённая Модерация шаблонов — 3-4 дня), Statistics (экран 31, тот же ClickHouse-источник, что Reports/TPS).

### ~~Chat (экран 27)~~ ✅ готово — обе стороны, живой раунд-трип проверен

Реальная новая фича с нуля: до этого захода в платформе не было ни топика, ни таблицы для сообщений админ↔партнёр. Новый выделенный `chat-service` (маленький Go control-plane сервис, тот же класс, что `iam-service`/`incident-service`) — единственный писатель/читатель новой таблицы `support.chat_messages` (`migrations/V034__chat_messages.sql`, один тред на `partner_id`); `backoffice-api` и `partner-self-service-api` — тонкие gRPC-прокси поверх `platform-contracts/grpc/chat.proto`, ни один не читает/пишет таблицу напрямую (полный разбор архитектурного решения — `services/chat-service/README.md`, единственный найденный в платформе случай двух независимых писателей в одну таблицу в реальном времени).

Обе стороны экрана: `backoffice-ui`'s `ChatView.vue` (список партнёров + unread-бейджи слева, тред справа, право `chat:write`) и `partner-portal-ui`'s `ChatView.vue` (только свой тред, без пикера партнёра — `partner_id` всегда из JWT, открыт любой партнёрской роли). Polling через TanStack Query `refetchInterval` — первое использование этого механизма в обоих UI (в платформе нигде нет websocket-инфраструктуры). `read_at` — побочный эффект чтения треда противоположной стороной, единственный источник счётчика "N новых" в сайдбаре админки.

**Граница объёма**: без read-receipts UI-полировки, без typing indicators, без file attachments. `since`-курсор полностью реализован на бэкенде, но не используется ни одним из двух фронтендов — каждый poll перезапрашивает тред целиком (мало данных на тред, инкрементальный merge курсора поверх TanStack Query не окупается на этом масштабе).

Живьём проверено на реальном `docker compose` стенде (все 5 сервисов — `chat-service`, `backoffice-api`, `partner-self-service-api`, `backoffice-ui`, `partner-portal-ui` — пересобраны и перезапущены), реальными JWT из `infra/docker/dev-secrets/`: админ шлёт сообщение → партнёр видит его через `GET /v1/self-service/chat/messages`; партнёр шлёт сообщение → админ видит его через `GET /v1/chat/{partner_id}/messages`; повторный `GET` с `since=<курсор предыдущего ответа>` возвращает только новое сообщение, не всю историю треда.

### ~~Admin users (экран 33)~~ ✅ готово — локальный логин/пароль

Полноценное управление аккаунтами прямо из бэкофиса: создать сотрудника с логином/паролем, LDAP/Keycloak — сильно позже. Новая таблица `iam.staff_accounts` (username/password_hash/display_name/active, `external_id`=username — нет Keycloak `sub`, брать неоткуда), новые RPC в `IamService` (`CreateStaffAccount`/`ListStaffAccounts`/`DeactivateStaffAccount`/`VerifyStaffCredentials`, bcrypt внутри `iam-service`, хеш никогда не пересекает границу процесса), новый `POST /v1/auth/login` в `backoffice-api` (единственный маршрут без JWT-мидлвари, подписывает токен новым RSA-keypair'ом, отдельным от общего dev-keypair остальных self-service API), новый `LoginView.vue` (форма вместо textarea) + `AdminUsersView.vue`. Живьём проверено на реальном стеке: создать сотрудника → залогиниться → выданный JWT реально проходит защищённые маршруты → деактивировать → повторный логин отклонён (401).

### ~~A2P/DLR per-PDU лог (экраны 38-40, первая половина)~~ ✅ готово

Raw per-PDU SMPP-трасса (`submit_sm` → `submit_sm_resp` → `deliver_sm` → `deliver_sm_resp`), которой раньше не было нигде в платформе (см. `BACKOFFICE_DESIGN_SPEC.md` Экраны 38-40, статус ❌ на момент разбора) — P2A ниже остаётся отдельным, ещё не начатым куском той же пары экранов.

Инструментация: `platform-contracts/events/operator_events.proto` — новый `OperatorPduLog` (`operator_id`/`protocol`/`direction`("A2P"/"DLR")/`pdu_type`/`sequence_number`/`message_id`/`stage_execution_id`/`smsc_message_id`/`segment_id`/`status`/`occurred_at`); `operator-smpp-session-manager`'s `OperatorSmppClientHandler`/`OperatorSubmitServer` эмитят одно событие на каждый реально пересечённый по проводу PDU в новый топик `operator.pdu.log` (24ч retention, тот же класс, что `operator.dlr`/`operator.submit.accepted` — диагностика, не бизнес-корреляция). Новый Go-сервис `pdu-log-writer` (тот же buffer/flush-паттерн, что `analytics-writer`, 1:1 портирован) батчит в `analytics.operator_pdu_log` (ClickHouse, `ReplacingMergeTree`, читатели обязаны `FINAL` — та же цена корректности, что `analytics.stage_events`).

Read-путь: новый `GET /v1/messages/{message_id}/pdu-log` в `backoffice-api` (`internal/httpapi/pdulog.go`) — DLR-направление PDU (`deliver_sm`/`_resp`) не несёт `message_id` (архитектурный барьер, тот же, что уже задокументирован у `OperatorDlr`), поэтому хендлер сперва резолвит `smsc_message_id` сообщения через `dlr.dlr_correlation` (переиспользует существующий `Postgres.OperatorEventsByMessage`, который уже кормит `/operator-events`), затем читает ClickHouse по `(message_id = ? OR smsc_message_id IN (...))`, объединяя A2P- и DLR-направления в одну хронологическую ленту. `MessagesView.vue` — новая карточка "Пер-PDU лог" в деталях сообщения (таблица direction/pdu_type/sequence_number/status/segment_id/smsc_message_id/occurred_at, цветные теги на direction/status).

**Найденный по пути пробел, оставленный как есть (не в скоупе этого захода)**: `deliver_sm_resp` — чистый ACK (`ESME_ROK`, без содержимого) — публикуется с пустым `smsc_message_id` (в отличие от `deliver_sm`, откуда он реально разбирается из receipt'а). Это не баг конкретно этой правки, а честное отражение того, что реальный SMPP `deliver_sm_resp` не несёт `smsc_message_id` на проводе — искусственно подставлять его значило бы придумывать данные, которых не было в PDU. Практическое следствие: для любого сообщения `pdu-log` показывает 3 из 4 PDU (без финального ACK); сама строка `DELIVER_SM_RESP` при этом реально существует в `analytics.operator_pdu_log` и видна в полнотабличных диагностических запросах, просто не коррелируется ни по одному сообщению конкретно.

Живьём проверено на реальном docker-compose стенде: пересобраны и передеплоены `operator-smpp-session-manager`/`pdu-log-writer`/`backoffice-api`/`backoffice-ui`; обнаружено и исправлено по пути — топик `operator.pdu.log` не был заведён ни в `infra/docker/create_topics.sh`, ни живьём в Kafka (создан вручную + добавлен в скрипт), `pdu-log-writer` отсутствовал в `infra/docker/docker-compose.yml` (добавлен, порт `9122:9090`, тот же паттерн Kafka+ClickHouse env, что `analytics-writer`), `services/pdu-log-writer/internal/proto/gen/mpp/platformcontracts/go.sum` отсутствовал (скопирован из `analytics-writer`'s идентичного `go.mod`, `docker build` иначе падал на `COPY`). Реальное сообщение проведено через `partner-rest-receiver` → пайплайн → `operator-smpp-session-manager` → ClickHouse: `SUBMIT_SM`/`SUBMIT_SM_RESP`/`DELIVER_SM` реально появились в `analytics.operator_pdu_log` с правильной корреляцией по `smsc_message_id`, `GET /v1/messages/{message_id}/pdu-log` вернул все три через `backoffice-api`, и `MessagesView.vue` реально отрендерил их в браузере (headless Chromium, скриншот, 0 console errors).

### Офлайн-биллинг / CDR-экспорт — план, не реализовано

См. `BACKOFFICE_DESIGN_SPEC.md` Экран 32 "Офлайн-биллинг / CDR-экспорт" — использует уже реализованные Categories+CTN. Кратко: добавить `category` в `billing.billing_ledger` (сегодня не хранится, только транзитно в Kafka), генерация CDR — событийным Kafka-consumer'ом (не cron/polling — озвученная пользователем проблема объёма), каждое событие с активным CTN-маппингом сразу дописывается в текущий CDR-файл, ротируемый по времени/размеру.

### TPS-дашборд (экран 29) — фазовый план, реализация не начата

Пользователь выбрал "сразу полную версию с настраиваемыми виджетами", уже зная, что это крупная фича. Почти все нужные данные сегодня физически не существуют (метрики `operator-smpp-session-manager` размечены только по `tier`, у `operator-http-gateway` `/metrics` — заглушка, Prometheus нигде не поднят, `analytics.stage_events` не хранит category/sender/operator/gateway, счётчиков трафика на сессию и хранения конфигурации виджетов нет вообще) — поэтому "полная версия" реализуется последовательно, не одним заходом:
1. **Инструментация**: разметить метрики по partner_id/sender_id/category, поднять реальные метрики в `operator-http-gateway`, добавить колонки в `analytics.stage_events`. Не поднимаем отдельный Prometheus — ClickHouse уже здесь.
2. **Счётчики трафика на сессию**: `HINCRBY` на `operator_route:*` (аутбаунд), аналогичный счётчик у `smpp:partner_session:*` (инбаунд SMPP), короткоживущий rolling-counter для REST (у которого сегодня нет понятия сессии).
3. **Backend-агрегация**: `GET /v1/dashboard/tps?group_by=...`, читает ClickHouse.
4. **Хранение виджетов + фронтенд**: новая таблица `backoffice.dashboard_widgets` (per-admin), `vue-grid-layout` (новая зависимость — grid-layout библиотеки в проекте сегодня нет).

### P2A (входящие MO, вторая половина экранов 38-40) — ДИЗАЙН, КОД НЕ ПИСАТЬ

**Явно только план по просьбе пользователя — ничего из этого раздела не реализовано, ниже нет ни одной строчки кода.** Задача — зафиксировать фазовый план в том же формате/детализации, что TPS-дашборд выше, прежде чем к этому будет отдельный заход.

Проверено чтением реального кода (не предположение) — P2A архитектурно отсутствует целиком, тот же вердикт, что уже зафиксирован в `BACKOFFICE_DESIGN_SPEC.md` Экран 6 "MO":
- `OperatorSmppClientHandler.channelRead0` (`services/operator-smpp-session-manager/.../client/OperatorSmppClientHandler.java`) обрабатывает **каждый** `deliver_sm` как DLR безусловно — `esm_class` уже декодируется кодеком (`ShortMessagePdu.esmClass()`, `codec/PduCodec.java`), но нигде дальше не читается. Реальный MO (esm_class без бита "SMSC Delivery Receipt", SMPP 3.4 §5.2.12) сегодня прошёл бы через `DeliveryReceiptParser`, который слепо ищет `id:`/`stat:` в теле — на настоящем тексте абонента дал бы пустые/мусорные значения и тихо потерялся бы в логах как "DLR без smsc_message_id".
- `operator-http-gateway`'s единственный входящий HTTP-путь — DLR-вебхук (`internal/webhook`); приёмника для MO там тоже нет.
- Ни один Kafka-топик, Postgres/ClickHouse-таблица или proto-сообщение не существуют для входящего от абонента направления — весь пайплайн (`incoming.messages` → `stage.*` → `operator.submit.accepted`) спроектирован строго под A2P (исходящее).

Фазы:
1. **Детекция на границе SMPP**: ветвление по `esm_class` прямо в `OperatorSmppClientHandler.channelRead0` — то же место, что уже разбирает `deliver_sm`, не новый компонент. DLR-ветка (`dlrSink`/`pduLogSink` с `direction=DLR`) не меняется; новая MO-ветка публикует сырое MO-событие вместо `dlrSink`. Раньше всего остального, потому что без этого шага реальный MO продолжит тихо портить DLR-метрики (ложные "DLR без smsc_message_id").
2. **Контракт + топик**: новое proto-сообщение `OperatorMoReceived` (`platform-contracts/events/operator_events.proto`, рядом с `OperatorPduLog`/`OperatorDlr` — та же секция, тот же файл) — `operator_id`/`protocol`/`source_addr` (msisdn абонента)/`destination_addr` (short number/alphaname, на который пришло)/`body`/`received_at`, плюс поля многосегментной конкатенации через UDH (`reference_number`/`total_segments`/`segment_number` — сегодня UDH не парсится вообще нигде на приёме, только неявно на исходящей стороне в `dispatchOne`). Новый топик `operator.mo.received` — тот же класс, что `operator.dlr` (`infra/kafka/generate_kafka_topics.py`).
3. **Маршрутизация к партнёру — обратное направление от `destination-resolution-service`**: тот сервис сегодня решает только исходящее (partner sender → оператор). Для MO нужен обратный lookup `destination_addr` (short number) → `partner_id`/`application_id`. Ближайший существующий кусок — `partner.schema.json`'s `senders[].sender_id`/`type: SHORT_NUMBER`, но он привязан к `partner_id` уровня партнёра, не к конкретному `application_id` — сегодня в конфиге в принципе нет поля "какое приложение партнёра владеет этим коротким номером/ключевым словом", это отдельное дополнение схемы, не только новый код. Новый consumer-сервис (например `mo-router`) читает `operator.mo.received`, резолвит партнёра/приложение через тот же кеш конфигурации, что уже использует `destination-resolution-service`, и пишет новую `messaging.mo_read_model` (тот же стиль строки, что `messaging.message_read_model`, но для входящего направления).
4. **Доставка партнёру**: переиспользовать существующую инфраструктуру вебхуков `partner-notification-service` и уже существующее поле `notification_callback_url` на каждом `application` в `partner.schema.json` — новый тип payload (`type: "mo"` рядом с уже существующими уведомлениями), не новый транспорт и не новый сервис доставки.
5. **Бесплатная корреляция с уже готовым per-PDU логом**: MO-PDU (`deliver_sm` с MO-семантикой) естественно ложится в уже существующую `analytics.operator_pdu_log` (см. раздел выше, эта работа уже сделана) — просто третье значение `direction` ("MO" рядом с уже существующими "A2P"/"DLR"), 0 изменений схемы ClickHouse. Даёт диагностическую видимость на уровне PDU ещё до того, как готовы полноценные MO-экраны.
6. **Backoffice-экраны**: список + карточка (от кого/на какой номер/текст/оператор/время/статус доставки партнёру) — контур уже намечен в `BACKOFFICE_DESIGN_SPEC.md` Экран 6 "MO"; строится после того, как данные реально появятся в `messaging.mo_read_model` (тот же принцип, что и у TPS-дашборда выше — экран не строится раньше данных, которые он показывает).

Оценка не меняется от уже зафиксированной в `BACKOFFICE_DESIGN_SPEC.md` — **~1-2 недели отдельной фазой, не между делом**: новый топик, новая таблица, новый consumer-сервис, изменение существующего SMPP-приёмника (риск затронуть живой DLR-путь, поэтому нужны тесты на обе ветки `esm_class` до мержа), расширение `partner.schema.json` под application-level владение коротким номером, и два новых экрана.

---

## Известные баги

**✅ Исправлено: `current_status`/лента истории застревали на первом статусе.** Два реальных, независимых бага в `lifecycle-writer`, оба воспроизведены живьём (не гипотеза) в логах реально работающего контейнера:
1. `messaging.create_lifecycle_history_partition` никогда не вызывался ни одним планировщиком — как только текущий час выходил за бутстрап-окно V015, `BatchInsertLifecycleHistory` падал КАЖДЫЙ tick, что блокировало commit офсетов для ВСЕХ топиков (`incoming.messages`/`message.lifecycle`/DLQ), не только history. Фикс — тот же паттерн, что уже был решён для `dlr-correlation-writer` (`EnsurePartition` перед каждым flush).
2. Реальная гонка `incoming.messages`/`message.lifecycle` (разные топики, порядок не гарантирован) — если update приходил раньше insert'а, `UPDATE ... WHERE lifecycle_version < $5` молча не находил строку и терял событие навсегда.

Перезапущен реальный контейнер `docker-lifecycle-writer-1` с фиксом — consumer lag подтверждённо вернулся к 0, сообщения в проде БД реально доходят до DELIVERED/FAILED. Подробности — коммит `856625d`.

---

## Приоритет (моя рекомендация, не финальное решение)

1. ~~Fix lifecycle-writer~~ ✅ готово.
2. ~~Spam/banwords UI~~ / ~~Route config UI~~ — **оказались уже функционально готовы**: `ConfigView.vue` — универсальный редактор любого `entity_type`, уже покрывает `policy_ruleset`/`route_table` сегодня (см. разделы 2/3 выше). Специализированные формы вместо сырого JSON — полировка, не пробел, отложено.
3. ~~Billing ledger browse UI~~ ✅ готово.
4. ~~Blacklist proxy~~ ✅ готово (по пути починены configuration-service schema drift и отсутствующий в compose consent-cache-projector — см. раздел 4 выше).
5. ~~Operator routes read-only view~~ ✅ готово.
6. **Template moderation workflow** — самая большая новая фича, делать осознанно отдельным заходом, не между делом.
7. **Dashboard** — в конце, после того как остальные экраны дадут данные, которые он агрегирует.

Скажите, что переставить местами — список открыт для правок, это не приказ сверху.
