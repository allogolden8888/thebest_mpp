# Инфраструктура — Фаза 1 (development_plan.md)

**Решения, принятые пользователем перед началом (не мои):**
1. Провайдер — региональный облачный (Yandex Cloud как конкретная реализация; при смене провайдера меняются `provider`-блок и resource-типы в `infra/terraform/*.tf`, структура — нет).
2. Kafka — self-hosted через Strimzi-оператор, не managed-сервис.
3. mTLS — service mesh (Istio), не app-level TLS.

**Статус:** `terraform validate`/`fmt` — чисто против реальной схемы `yandex-cloud/yandex` провайдера (не просто "похоже на HCL"); Kafka-топики сгенерированы из той же таблицы `SERVICES`, что и `k8s/`; Istio-манифесты и CI-пайплайн, объединяющий все проверки этой сессии, готовы.

```bash
brew install hashicorp/tap/terraform actionlint
cd infra/terraform && terraform init -backend=false && terraform validate && terraform fmt -check
cd ../kafka && python3 generate_kafka_topics.py
kubeconform -kubernetes-version 1.34.0 -ignore-missing-schemas -summary rendered/*.yaml
actionlint .github/workflows/ci.yml
```

## `infra/terraform/` — облачные ресурсы

| Файл | Что | Реально найденные расхождения со schema (не выдуманные, а через `terraform validate`) |
|---|---|---|
| `k8s-cluster.tf` | Managed k8s + 4 node group (rust/java/go/sticky-pool), taints по `mpp.io/workload-class` — совпадает с классами из `k8s/generate_manifests.py` | — |
| `postgresql.tf` | HA PostgreSQL 17, 3 хоста | `database{}` внутри кластера — deprecated провайдером, вынесено в отдельный `yandex_mdb_postgresql_database` |
| `redis.tf` | 3 физически изолированных кластера (Runtime/Configuration/Billing), разный `persistence_mode` | `persistence_mode` — top-level аргумент ресурса, не поле `config{}` (первая попытка сломалась именно на этом) |
| `clickhouse.tf` | Analytics-хранилище | `yandex_mdb_clickhouse_cluster` — deprecated, переписано на `_v2` (другая форма схемы: `hosts` — map nested-attributes, не повторяемые `host{}`-блоки; `maintenance_window{}` — обязателен) |
| `registry.tf` | Container registry + по репозиторию на каждый из 32 сервисов | Список сервисов синхронизирован вручную с `k8s/generate_manifests.py SERVICES` — см. "Известный технический долг" ниже |
| `istio.tf` | `helm_release` для `istio-base`/`istiod` | — |

Всё выше подтверждено `terraform validate` **против настоящей схемы провайдера** (`terraform providers schema -json`), не только визуально — три реальные ошибки схемы (redis `persistence_mode`, clickhouse `_v2`, clickhouse `maintenance_window`) были найдены и исправлены в процессе, не предугаданы заранее.

## `infra/kafka/` — топики и партиции

`generate_kafka_topics.py` — партиции не выдуманы, а вычислены: `partitions(topic) = ceil(max(replicas_for(consumer)) × 1.5)`, где `replicas_for` — та же функция из `k8s/generate_manifests.py`, что определяет число реплик каждого сервиса в кластере. Если `capacity_model.md` в следующей итерации изменит vCPU для какого-то сервиса, партиции пересчитаются автоматически при следующем прогоне, а не рассинхронизируются.

**Реальный найденный нюанс, не очевидный на уровне спецификации:** `delivery.status` сам по себе (потребитель — только Message State Resolver, 4 реплики) естественно получил бы 6 партиций, но Message State Resolver джойнит его со `stage.completed` (17 реплик Pipeline Engine → 26 партиций) — Kafka Streams требует **равного** числа партиций у co-partitioned топиков для join. `CO_PARTITIONED_GROUPS` в генераторе принудительно поднимает `delivery.status` до 26, с явным комментарием почему. Без этого шага реальный деплой сломался бы на первом же join в Message State Resolver — ошибка, которую JSON/YAML-валидация в принципе не может поймать, только знание семантики Kafka Streams.

Классификация топиков — не единая формула на все: `WORKLOAD` (формула выше), `CONTROL` (config.changes/execution.control — compacted, фиксированные 6 партиций, объём низкий), `DLQ` (фиксированные 3, retention 7 дней), `CHANGELOG` (наследует партиции родительского workload-топика — Kafka Streams-инвариант, не независимое число).

**Kafka CR** (5 брокеров, `default.replication.factor=3`, `min.insync.replicas=2`, `transaction.state.log.min.isr=2`) — последнее поле не случайно: Pipeline Engine и Message State Resolver используют Kafka-транзакции (`hld.md` §10.1, транзакционная гарантия changelog+событие+offset), exactly-once семантика требует этого явно, иначе транзакционная гарантия из HLD была бы не более чем текстом.

kubeconform не имеет офлайн-схемы для `kafka.strimzi.io` CRD (тот же паттерн, что раньше был с `keda.sh` в `k8s/README.md`) — 30/30 ресурсов **skipped**, не "passed", это разница явно видна в выводе `-summary` и не должна читаться как "всё провалидировано".

## `infra/istio/` — mTLS

`PeerAuthentication` (`mode: STRICT`, namespace `mpp`) + явный `DestinationRule` (`ISTIO_MUTUAL`, формально избыточен поверх auto-mTLS, оставлен для видимости в манифесте). `k8s/generate_manifests.py` обновлён — namespace `mpp` теперь несёт `istio-injection: enabled` (было: без меток вообще) — без этой метки PeerAuthentication STRICT просто не на что было бы опираться, sidecar не инжектился бы.

**Разделение слоёв, зафиксированное явно (см. `k8s/README.md`):** NetworkPolicy решает, кто может открыть TCP-соединение; PeerAuthentication STRICT — что это соединение обязано быть аутентифицировано mTLS. Ни один слой не заменяет другой.

## `.github/workflows/ci.yml` — CI

Шесть job — ровно шесть категорий ручной валидации, которые проводились по ходу этой сессии (protoc compile, PostgreSQL-миграции на реальном `postgres:17` service-контейнере, три Python-пакета с исполняемыми тестами, kubeconform на k8s-манифестах + reachability-тест, kubeconform на Kafka CRD, `terraform validate`/`fmt`). Ни один job не "придуман для вида" — каждый воспроизводит команду, которая уже была реально запущена руками в этом или предыдущих шагах LLD.

Провалидирован `actionlint` (реальный линтер GitHub Actions + shellcheck внутри `run:`-блоков) — нашёл и заставил исправить одну настоящую проблему: `find | xargs` без `-print0`/`-0` (SC2038, ломается на именах файлов с пробелами/спецсимволами) в protobuf-job.

## `infra/secrets/` и `infra/terraform/vault.tf`/`vault-secrets.tf` — Secrets (Фаза 1.7)

**Решение (не спрошено у пользователя явно, обосновано здесь):** Vault (self-hosted) + External Secrets Operator, не Yandex Lockbox напрямую. Причина — тот же принцип портируемости, что уже применён к Kafka (self-hosted Strimzi вместо managed-сервиса): провайдер выбран пользователем как "региональный, Yandex Cloud как конкретика", не как жёсткая привязка — Vault работает одинаково независимо от того, какой облачный провайдер окажется финальным, Yandex Lockbox — нет. ESO поддерживает Vault одним из первых backend-провайдеров.

**Как это закрывает реальный, а не гипотетический пробел:** до этого шага ни один из 32 сервисов в `k8s/generate_manifests.py` не имел вообще никакого способа получить DB DSN/Redis-пароль/ClickHouse-креды — `_container()` собирал только порты/ресурсы/probes, без единого `env`/`envFrom`. Это было тихим пропуском, не зафиксированным как "осознанно вне скоупа" нигде раньше.

**Что сделано:**
1. `k8s/generate_manifests.py` — новое поле `Service.secrets` + таблица `SECRET_DEPENDENCIES`, которая по каждому из 32 сервисов называет, к каким из 5 хранилищ (`postgresql`/`redis-runtime`/`redis-configuration`/`redis-billing`/`clickhouse`) он обращается — источник: упоминания конкретных хранилищ по каждому сервису в `services_specifictaion.md` (не выдумано; там, где документ не называет хранилище явно — DLR Manager, Config Event Publisher — секрет не назначен, а не додуман). `_container()` теперь добавляет `envFrom: [secretRef: ...]` для каждого назначенного секрета.
2. `infra/secrets/generate_external_secrets.py` — `ClusterSecretStore` (Vault backend, Kubernetes auth method — ESO аутентифицируется собственным ServiceAccount-токеном, не статичным секретом) + 5 `ExternalSecret`, каждый читает свой Vault KV v2 путь и материализует k8s `Secret` с именем из `SECRET_K8S_NAME` — **та же таблица**, что использует `generate_manifests.py` (импортируется, не дублируется).
3. `infra/terraform/vault.tf` — `helm_release` для Vault (standalone, file storage — см. ограничение ниже) и External Secrets Operator.
4. `infra/terraform/vault-secrets.tf` — **замыкает контур**: `vault_kv_secret_v2` пишет в Vault те же значения (`var.postgresql_app_password`, `var.redis_passwords`, `yandex_mdb_postgresql_cluster.mpp.host[0].fqdn` и т.д.), которыми реально были созданы managed-ресурсы в `postgresql.tf`/`redis.tf`/`clickhouse.tf` — не заново введённые вручную значения, которые могли бы разойтись с реальностью при следующей ротации. `terraform validate` подтвердил реальные имена аргументов (`vault_kubernetes_auth_backend_role.bound_service_account_namespaces`, `vault_kv_secret_v2.data_json` как JSON-строка через `jsonencode`, не map напрямую) через `terraform providers schema -json`, тем же методом, что раньше поймал ошибки в `redis.tf`/`clickhouse.tf`.

**Важное ограничение, не спрятанное:** Vault standalone + file storage — не production HA (нет Raft-кластера, нет auto-unseal). Init/unseal — операционный шаг, который Terraform принципиально не может сделать декларативно для этого режима (Vault стартует sealed, `vault_kv_secret_v2` не может писать, пока кто-то не разблокировал под) — задокументировано в `vault.tf` как ручной/скриптовый bootstrap, а не спрятано за предположением "terraform apply само разберётся".

## Известный технический долг этого шага

* **Список из 32 сервисов дублирован** между `k8s/generate_manifests.py SERVICES` и `infra/terraform/registry.tf locals.service_names` — при добавлении нового сервиса нужно обновить оба места. Правильное решение — экспортировать список из общего источника (например, Terraform читает JSON, сгенерированный тем же `generate_manifests.py`), не сделано в этой итерации ради скорости, зафиксировано как долг, не забыто молча.
* `terraform validate` подтверждает **синтаксическую и схемную** корректность, не то, что план реально применится с реальными cloud-креденшлами (`cloud_id`/`folder_id`/пароли/`vault_token` — переменные без default, обязаны прийти из окружения/CI secrets).
* Kubernetes-провайдер аутентификация в `istio.tf` использует `exec { command = "yc" }` (Yandex CLI) — рабочий паттерн для локального `terraform apply`, но в CI потребует либо установленного `yc`, либо переключения на service account key/IAM-токен напрямую.
* Vault standalone/file-storage (см. выше) — известно упрощено, не прод-топология.
* `SECRET_DEPENDENCIES` — по значительной части (control-plane/writer-сервисы) сопоставление сделано по прямым цитатам из `services_specifictaion.md`; по остальным (например какие именно из "hot-path сервисов" читают Configuration Redis) — разумное следствие архитектуры, не всегда подкреплено построчной цитатой. Стоит сверить перед реальным продом.
* **Kafka-листенеры без аутентификации.** `infra/kafka/generate_kafka_topics.py` настраивает `plain`/`tls` листенеры (шифрование), но ни SASL/SCRAM, ни mTLS-аутентификацию клиентов Kafka не включает — сейчас любой под внутри NetworkPolicy-разрешённого пути может писать в любой топик без проверки identity на уровне Kafka (Istio PeerAuthentication STRICT покрывает gRPC/HTTP между сервисами, но не Kafka-протокол сам по себе, если Kafka не терминирует TLS через mesh). Не закрыто этим шагом — SMPP bind-креды операторов (Фаза 5, per-operator данные) туда же.

## `infra/terraform/observability.tf` + `infra/observability/` — KEDA/Prometheus/Grafana (Фаза 1.10)

**Что это закрывает, конкретно:** `k8s/generate_manifests.py` генерирует `ScaledObject` для всех 16 `kafka_consumer=True` stateless-сервисов с самого первого прогона `k8s/` — но ни разу не было развёрнуто ничего, способного их прочитать. Все 16 лежали в кластере как валидный, но полностью нерабочий YAML. Аналогично `HEALTH_PORT=9090`/`/metrics` — конвенция существовала с первого k8s-шага (readiness/liveness), но ничего её не скрейпило.

**Что сделано:**
1. `helm_release "keda"` — оператор для уже существующих `ScaledObject`.
2. `helm_release "kube_prometheus_stack"` — Prometheus Operator + Prometheus + Grafana + Alertmanager одним чартом. Пароль Grafana admin — `random_password` + `kubernetes_secret` (не захардкожен, не в Terraform state как plaintext helm-value), продублирован в Vault тем же способом, что остальные креды (`vault_kv_secret_v2`) для восстановления доступа при потере Secret.
3. `infra/observability/pod-monitor.yaml` — **один** `PodMonitor` на весь namespace `mpp`, не 32 отдельных объекта: все сервисы уже унифицированы на один порт/путь (`HEALTH_PORT`/`/metrics`), PodMonitor селектит поды напрямую по label и не требует Service-объекта — большинство чистых Kafka-consumer сервисов вообще не получают Service в `generate_manifests.py`, заводить его только ради scrape было бы лишней сущностью.
4. **Реальная нестыковка, найденная и исправленная, а не предположенная:** `k8s/network_policies.py` уже содержал `allow-prometheus-scrape-ingress` с ожиданием `podSelector: {app: prometheus}` (написано на шаге NetworkPolicy, до того как Prometheus вообще был выбран для развёртывания). Дефолтные лейблы подов `kube-prometheus-stack` этому не обязаны соответствовать. Вместо угадывания внутренних дефолтов чарта — `prometheus.prometheusSpec.podMetadata.labels.app = "prometheus"` явно проставлен в helm values, подогнан под уже написанную и протестированную NetworkPolicy, а не наоборот.

**Осознанно не сделано на этом шаге:** alerting rules и Grafana-дашборды — `development_plan.md` Фаза 6 (после нагрузочного тестирования). Числа/пороги, придуманные до единого реального прогона под нагрузкой, были бы угаданы, не откалиброваны — тот же принцип, что уже применён к KEDA `lagThreshold` в `k8s/generate_manifests.py` (текущее значение `1000` — временное, ждёт калибровки в Фазе 6).

## `infra/terraform/ingress.tf` + `infra/ingress/` — Ingress/cert-manager (Фаза 1.9, последний пункт Фазы 1)

**Область:** только 4 из 6 внешних сервисов идут через Ingress — Partner REST Receiver, Partner API, Backoffice API, Backoffice UI (все `workload_class != sticky-statefulset`, HTTP, `ClusterIP`). Partner SMPP Gateway — raw TCP (SMPP-протокол, не HTTP, L7 Ingress-роутинг не применим), уже на собственном `LoadBalancer` (`build_external_service`). Operator HTTP Gateway — тоже HTTP, но `sticky-statefulset` (route ownership, свой `LoadBalancer`, инбаунд-вебхук от оператора не нуждается в L7-хосте) — оставлен как есть, не переведён на Ingress, чтобы не трогать уже устоявшийся паттерн владения маршрутом ради необязательной унификации.

**Что сделано:**
1. `k8s/generate_manifests.py` — новая `build_ingress(svc)`, генерирует `Ingress` по тому же признаку, что уже отличал `ClusterIP` от `LoadBalancer` в `build_external_service` (`workload_class != "sticky-statefulset"`) — не новый флаг на сервисе, переиспользование существующего. `EXTERNAL_DOMAIN`/`INGRESS_CLASS`/`CLUSTER_ISSUER` — новые константы, `EXTERNAL_DOMAIN = "mpp.example"` — плейсхолдер, как раньше `IMAGE_REGISTRY = "registry.mpp.internal"`: `.example` — зарезервированная IANA-зона, Let's Encrypt не выпустит для неё сертификат, это ожидаемо и не блокирует остальную инфраструктуру.
2. `infra/terraform/ingress.tf` — `helm_release` для `ingress-nginx` (`ingressClassResource.name = "nginx"`, совпадает с `INGRESS_CLASS`) и `cert-manager`.
3. `infra/ingress/cluster-issuer.yaml` — `ClusterIssuer` (`letsencrypt-http01`, имя совпадает с `CLUSTER_ISSUER`) — не Terraform-ресурс, тот же chicken-and-egg с CRD, что уже решён для `PeerAuthentication`/`PodMonitor`: применяется отдельным шагом после того, как `cert-manager` реально установлен, не через `terraform kubernetes_manifest`. HTTP-01, не DNS-01 — не привязано к конкретному DNS-провайдеру, тот же принцип портируемости, что уже применён к Kafka (self-hosted) и Vault (self-hosted).

**Реальная проверка, не только генерация:** `kubeconform -verbose` подтвердил `Ingress backoffice-ui is valid` против настоящей схемы Kubernetes 1.34 (core-ресурс, в отличие от CRD выше — `Ingress` есть в офлайн-каталоге kubeconform, `ClusterIssuer` — нет, honest skip, тот же паттерн).

## Фаза 1 — итог

Все 10 пунктов закрыты. Открытый технический долг (см. выше и development_plan.md): дублирование списка сервисов между `k8s/` и `infra/terraform/registry.tf`, Vault standalone без HA/auto-unseal, Kafka без SASL-аутентификации клиентов, `EXTERNAL_DOMAIN`/e-mail в `ClusterIssuer` — плейсхолдеры до покупки реального домена, alerting rules/дашборды намеренно отложены до Фазы 6.
