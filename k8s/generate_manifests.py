"""
Генератор Kubernetes-манифестов для всех микросервисов MPP.

Почему генератор, а не 28 руками написанных манифестов: сервисы делятся всего
на 4 класса нагрузки (см. WorkloadClass ниже), внутри класса манифест
идентичен по структуре и отличается только именем/языком/размером/портами.
Написание 28 копипаст-файлов руками — верный способ рассинхронизировать один
из них после следующего изменения политики (например, добавления
PodDisruptionBudget). Источник истины — таблица SERVICES ниже, с цитатами
на capacity_model.md/services_specifictaion.md на каждое число.

Валидация — реальная, не "похоже на правильный YAML": `python3
generate_manifests.py` рендерит в `rendered/`, затем
`kubeconform -strict -kubernetes-version 1.34.0` проверяет каждый файл против
настоящих OpenAPI-схем Kubernetes 1.34 (README.md — результат прогона).

Стандартные размеры инстансов — capacity_model.md §... "Rust 4 vCPU/8GB,
Java-session 8 vCPU/16GB, Go 2 vCPU/4GB" (см. Explore-отчёт, capacity_model.md:92).
"""

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import yaml

NAMESPACE = "mpp"
IMAGE_REGISTRY = "registry.mpp.internal"
HEALTH_PORT = 9090  # /metrics (Prometheus), /healthz, /readyz — новая платформенная конвенция,
                     # ни в одном из HLD/LLD документов порт/путь не был зафиксирован явно.

# HIGH находка кодревью (PART 2, partner-rest-receiver #4) — см. полный
# комментарий у SECRET_DEPENDENCIES ниже. Оба сервиса, читающие статический
# partner-конфиг (partner-rest-receiver для auth, partner-notification-service
# для callback-роутинга — тот же файл, два разных потребителя), получают его
# из одного и того же ConfigMap вместо несуществующего relative dev-пути.
PARTNER_CONFIG_SERVICES = ["partner-rest-receiver", "partner-notification-service"]
PARTNER_CONFIG_MOUNT_DIR = "/etc/mpp/partner-config"
PARTNER_CONFIG_FIXTURE = "partner.valid.json"
PARTNER_CONFIG_CONFIGMAP_NAME = "partner-config-fixture"


def credential_ref_to_env_var(credential_ref: str) -> str:
    """Python-порт `services/partner-rest-receiver/src/auth.rs::credential_ref_to_env_var`
    — ДОЛЖЕН оставаться байт-в-байт синхронным с ним, иначе сгенерированный
    здесь Secret не совпадёт по именам ключей с тем, что `EnvAuthVerifier`
    реально читает через `std::env::var`. Тот же тестовый вектор, что в
    `auth.rs` (`credential_ref_maps_to_deterministic_env_var_name`), повторён
    в `k8s/generate_manifests_test.py` (или аналоге), чтобы дрейф между
    Rust- и Python-версией ловился автоматически, не только при живом деплое.
    """
    sanitized = "".join(c.upper() if c.isascii() and c.isalnum() else "_" for c in credential_ref)
    return f"PARTNER_CRED_{sanitized}"


def load_partner_fixture() -> dict:
    fixture_path = Path(__file__).parent.parent / "config_schemas" / "examples" / PARTNER_CONFIG_FIXTURE
    return json.loads(fixture_path.read_text())

# Плейсхолдер, как IMAGE_REGISTRY выше — реальный зарегистрированный домен нигде
# не зафиксирован ни в одном документе. mpp.example не выпустит настоящий
# Let's Encrypt сертификат (example. — зарезервированная IANA-зона), это
# ожидаемо: инфраструктура (Ingress/cert-manager/ClusterIssuer) валидна и
# готова, реальная выдача сертификата начнётся с заменой на купленный домен.
EXTERNAL_DOMAIN = "mpp.example"
INGRESS_CLASS = "nginx"
CLUSTER_ISSUER = "letsencrypt-http01"  # infra/terraform/ingress.tf

INSTANCE_PROFILE = {
    "rust": {"cpu": 4, "mem_gi": 8},
    "java": {"cpu": 8, "mem_gi": 16},
    "go": {"cpu": 2, "mem_gi": 4},
}

# Оценка, не измерение — как и остальные числа в capacity_model.md (см. заголовок
# документа: "Числа по-прежнему оценки, не измерения; каждая цифра подлежит
# подтверждению нагрузочным тестированием"). Для этих 13 admin/control-plane
# сервисов нет отдельной записи в capacity_model.md вообще (не только числа
# неточные — самой строки в таблице нет), это стартовая точка до
# load-test/VPA-подтверждения, не измеренное значение.
CONTROL_PLANE_RESOURCES = {
    "limits": {"cpu": "500m", "memory": "1Gi"},
    "requests": {"cpu": "250m", "memory": "512Mi"},
}

MIN_REPLICAS = 2  # HA floor — ни один сервис не должен работать в единственном экземпляре


@dataclass
class Service:
    name: str
    lang: str  # rust | java | go
    workload_class: str  # stateless | sticky-statefulset | kafka-streams-statefulset | frontend
    vcpu_total: int | None  # None => нет отдельной записи в capacity_model.md, используется MIN_REPLICAS floor
    sizing_source: str  # цитата, откуда взято число
    kafka_consumer: bool = False  # потребляет топик стадии/события => KEDA ScaledObject по lag
    external_port: dict | None = None  # {"name":..., "port":..., "protocol":"TCP"|"HTTP"} — внешний inbound
    internal_grpc_port: int | None = None  # gRPC-порт для instance-addressed вызовов (sticky-сервисы)
    rocksdb_pvc_gi: int | None = None  # для kafka-streams-statefulset — размер PVC под RocksDB
    notes: str = ""
    secrets: list[str] = field(default_factory=list)  # ключи из SECRET_K8S_NAME — envFrom на соответствующий Secret,
                                                        # см. SECRET_DEPENDENCIES ниже и infra/secrets/
    resource_tier: str = "default"  # "control-plane" => CONTROL_PLANE_RESOURCES вместо INSTANCE_PROFILE[lang]


SERVICES = [
    Service("partner-rest-receiver", "rust", "stateless", 16,
            "capacity_model.md:100", kafka_consumer=False,
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}),
    Service("partner-smpp-gateway", "java", "sticky-statefulset", 48,
            "capacity_model.md:101", internal_grpc_port=9000,
            external_port={"name": "smpp", "port": 2775, "protocol": "TCP"},
            notes="Обычная балансировка между pod не используется (hld.md:1040) — headless Service "
                  "для instance-addressed доступа, LoadBalancer только для приёма новых bind."),
    Service("operator-smpp-session-manager", "java", "sticky-statefulset", 24,
            "capacity_model.md:102", internal_grpc_port=9000,
            notes="Egress-only (ESME к оператору) — нет внешнего inbound, только headless Service "
                  "для instance-addressed вызовов от Delivery."),
    Service("operator-http-gateway", "go", "sticky-statefulset", 8,
            "capacity_model.md:260-263,270 (v3 placeholder, 'реальный размер неизвестен')",
            internal_grpc_port=9000,
            external_port={"name": "webhook", "port": 8080, "protocol": "TCP"}),
    Service("pipeline-engine", "rust", "stateless", 68,
            "capacity_model.md:103,240 (v3: 17 instances x 4vCPU)", kafka_consumer=True),
    Service("destination-resolution-service", "rust", "stateless", 8,
            "capacity_model.md:250-251 (2 instances x 4vCPU)", kafka_consumer=True),
    Service("policy-service", "rust", "stateless", 12,
            "capacity_model.md:104,78 (v2)", kafka_consumer=True),
    Service("billing-service", "java", "stateless", 24,
            "capacity_model.md:105", kafka_consumer=True),
    Service("routing-service", "rust", "stateless", 8,
            "capacity_model.md:106", kafka_consumer=True),
    Service("delivery-service", "java", "stateless", 24,
            "capacity_model.md:107", kafka_consumer=True),
    Service("delivery-reconciliation-service", "java", "stateless", 8,
            "capacity_model.md:108", kafka_consumer=True),
    Service("scheduler-critical-sweep", "go", "stateless", 8,
            "capacity_model.md:35,109 (плоский размер, не масштабируется по TPS)", kafka_consumer=False,
            notes="Redis sorted-set polling ~1с, не Kafka consumer — без KEDA lag-триггера, "
                  "фиксированное число реплик."),
    Service("scheduler-standard-lane", "java", "kafka-streams-statefulset", 16,
            "capacity_model.md:110", kafka_consumer=True, rocksdb_pvc_gi=50),
    Service("scheduler-background-lane", "java", "kafka-streams-statefulset", 16,
            "capacity_model.md:111", kafka_consumer=True, rocksdb_pvc_gi=50),
    Service("message-state-resolver", "java", "kafka-streams-statefulset", 32,
            "capacity_model.md:112", kafka_consumer=True, rocksdb_pvc_gi=100),
    Service("execution-control-service", "go", "stateless", None,
            "не в отдельной таблице capacity_model.md — пул 'Мелкие Go control-plane' (capacity_model.md:12,113)",
            internal_grpc_port=9000, resource_tier="control-plane"),
    Service("iam-service", "go", "stateless", None,
            "не в отдельной таблице capacity_model.md — сервис появился уже после того, как этот документ "
            "писался, но по форме (маленький Go control-plane сервис, синхронный gRPC, без внешнего inbound) "
            "тот же пул 'Мелкие Go control-plane' (capacity_model.md:12,113), что execution-control-service/"
            "configuration-service", internal_grpc_port=9000, resource_tier="control-plane"),
    Service("credential-issuer-service", "go", "stateless", None,
            "не в отдельной таблице capacity_model.md — сервис появился уже после того, как этот документ "
            "писался (luminous-hugging-charm.md Фаза 1), но по форме (маленький Go control-plane сервис, "
            "синхронный gRPC, без внешнего inbound) тот же пул 'Мелкие Go control-plane' "
            "(capacity_model.md:12,113), что execution-control-service/iam-service/configuration-service",
            internal_grpc_port=9000, resource_tier="control-plane"),
    Service("incident-service", "go", "stateless", None,
            "не в отдельной таблице capacity_model.md — сервис появился уже после того, как этот документ "
            "писался (luminous-hugging-charm.md Фаза 7), но по форме (маленький Go control-plane сервис, "
            "синхронный gRPC, без внешнего inbound) тот же пул 'Мелкие Go control-plane' "
            "(capacity_model.md:12,113), что execution-control-service/iam-service/configuration-service",
            internal_grpc_port=9000, resource_tier="control-plane"),
    Service("ops-visibility-service", "go", "stateless", None,
            "не в отдельной таблице capacity_model.md — сервис появился уже после того, как этот документ "
            "писался (luminous-hugging-charm.md Фаза 8), тот же пул 'Мелкие Go control-plane' "
            "(capacity_model.md:12,113). Без internal_grpc_port и без external_port — у этого сервиса "
            "нет отдельного бизнес-порта вообще: GET /snapshot отдаётся с того же HEALTH_PORT=9090, что "
            "и /healthz/readyz/metrics (ops-visibility-service/README.md — решение того шага, не редизайн "
            "здесь), не через отдельный gRPC/HTTP-сервер, как у остальных сервисов этого пула.",
            kafka_consumer=False, resource_tier="control-plane"),
    Service("configuration-service", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", internal_grpc_port=9000,
            resource_tier="control-plane"),
    Service("config-event-publisher", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", kafka_consumer=False,
            resource_tier="control-plane"),
    Service("config-cache-projector", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", kafka_consumer=True,
            resource_tier="control-plane"),
    Service("consent-cache-projector", "go", "stateless", None,
            "не указан ни в одной vCPU-таблице (services_specifictaion.md:703-707,1131)", kafka_consumer=True,
            resource_tier="control-plane"),
    Service("dlr-correlation-writer", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)", kafka_consumer=True),
    Service("dlr-manager", "go", "stateless", 12,
            "capacity_model.md:114", kafka_consumer=True),
    Service("billing-outbox-publisher", "java", "stateless", None,
            "language ПО services_specifictaion.md (Java, billing-platform-java репо) конфликтует с "
            "capacity_model.md, который группирует этот сервис в пул Go control-plane (capacity_model.md:12,113) "
            "— расхождение между документами, не решённое здесь, см. README.md", kafka_consumer=True),
    Service("billing-ledger-writer", "java", "stateless", None,
            "тот же языковой конфликт документов, что у billing-outbox-publisher, см. README.md",
            kafka_consumer=True),
    Service("billing-reconciliation", "java", "stateless", 16,
            "capacity_model.md:115", internal_grpc_port=9000),
    Service("partner-api", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)",
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}, resource_tier="control-plane"),
    Service("backoffice-api", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)",
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}, internal_grpc_port=9000,
            resource_tier="control-plane"),
    Service("replay-service", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)", internal_grpc_port=9000,
            resource_tier="control-plane"),
    Service("lifecycle-writer", "go", "stateless", 8,
            "capacity_model.md:116 (v2)", kafka_consumer=True),
    Service("analytics-writer", "go", "stateless", 16,
            "capacity_model.md:117", kafka_consumer=True),
    Service("pdu-log-writer", "go", "stateless", None,
            "не в capacity_model.md — сервис появился уже после того, как этот документ писался "
            "(BACKOFFICE_DESIGN_SPEC.md Экраны 38-40, A2P/DLR per-PDU логи), тот же пул 'Мелкие Go "
            "control-plane' (capacity_model.md:12,113), что analytics-writer по форме (Kafka "
            "consumer -> ClickHouse batch insert), но НАМЕРЕННО отдельный сервис, а не расширение "
            "analytics-writer: схема operator.pdu.log (operator_id/direction/pdu_type/sequence_number/"
            "smsc_message_id) не имеет ничего общего с analytics.stage_events "
            "(stage_name/outcome/reason_code) — общий консьюмер означал бы либо мешать две разные "
            "ClickHouse-схемы в одном FlushBatch, либо форкать batching-логику внутри одного сервиса "
            "на две независимые ветки. Отдельный маленький сервис — тот же выбор, что уже сделан для "
            "dlr-correlation-writer рядом со своим доменом.",
            kafka_consumer=True, resource_tier="control-plane"),
    Service("partner-notification-service", "go", "stateless", 24,
            "capacity_model.md:118", kafka_consumer=True, internal_grpc_port=9000),
    Service("backoffice-ui", "go", "frontend", None,
            "не в capacity model — статический SPA, floor MIN_REPLICAS",
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}, resource_tier="control-plane"),
]

# Секретные зависимости по сервису — из четырёх ключей ниже, каждый мапится
# на отдельный k8s Secret (SECRET_K8S_NAME), наполняемый ExternalSecret из
# Vault (infra/secrets/). Источник — services_specifictaion.md (упоминания
# "Runtime Redis"/"Configuration Redis"/"Billing Redis"/"PostgreSQL"/
# "ClickHouse" по каждому сервису); там, где документ не называет хранилище
# явно (Config Event Publisher), секрет не назначен — не додумано, см.
# infra/README.md. DLR Manager раньше был в этом списке "не додумано" —
# закрыто при реализации `services/dlr-manager`: lookup_correlation читает
# PostgreSQL (dlr.dlr_correlation), а кэш "ожидающих корреляции" DLR
# (см. README сервиса — реальный пробел контракта SchedulerBackgroundTask,
# закрытый локальным кэшем) живёт в Runtime Redis.
SECRET_DEPENDENCIES: dict[str, list[str]] = {
    "dlr-manager": ["postgresql", "redis-runtime"],
    "partner-rest-receiver": ["redis-runtime", "redis-configuration"],
    "partner-smpp-gateway": ["redis-runtime", "redis-configuration"],
    "operator-smpp-session-manager": ["redis-runtime", "redis-configuration"],
    "operator-http-gateway": ["redis-runtime", "redis-configuration"],
    "pipeline-engine": ["redis-runtime", "redis-configuration"],
    "destination-resolution-service": ["redis-configuration"],
    "policy-service": ["redis-runtime", "redis-configuration"],
    "billing-service": ["redis-billing", "redis-configuration"],
    "routing-service": ["redis-configuration"],
    "delivery-service": ["redis-runtime", "redis-configuration"],
    "delivery-reconciliation-service": ["redis-runtime", "redis-configuration", "postgresql"],
    "scheduler-critical-sweep": ["redis-runtime"],
    "scheduler-standard-lane": ["redis-runtime", "redis-configuration"],
    "scheduler-background-lane": ["redis-runtime", "redis-configuration"],
    # Найдено при реализации services/message-state-resolver: не было
    # обоснования этой записи ни в одном документе — service_io_contracts.md
    # §2.4 перечисляет ровно 2 входа (stage.completed, delivery.status) и
    # 2 выхода (message.lifecycle, message-state.changelog), ни один не
    # Redis. Убрано как стороннее/скопированное значение, не додуманное
    # заново — реального использования Redis в этом сервисе нет.
    "execution-control-service": ["postgresql", "redis-configuration"],
    # services/iam-service/cmd/iam-service/main.go: только pgxpool.Pool
    # (buildPostgresDSN -> POSTGRES_*) — нет Redis-клиента, нет Kafka
    # consumer/producer, нет ClickHouse; CheckPermission синхронно идёт в
    # Postgres на каждый вызов (явно, без кеша — см. README.md сервиса).
    "iam-service": ["postgresql"],
    # services/credential-issuer-service/cmd/credential-issuer-service/main.go:
    # только pgxpool.Pool (buildPostgresDSN -> POSTGRES_*) — нет Redis/Kafka/
    # ClickHouse. Сознательно НЕТ записи для Vault: этот сервис ходит в
    # Vault по Kubernetes auth (ServiceAccount JWT обменивается на Vault
    # token — infra/terraform/vault-secrets.tf
    # vault_kubernetes_auth_backend_role.credential_issuer_service), не по
    # статическому токену из k8s Secret — весь смысл этого механизма в том,
    # что на стороне Vault-клиента нет вообще никакого статического секрета.
    # VAULT_ADDR — не секрет (см. infra/secrets/generate_external_secrets.py
    # VAULT_ADDR), и main.go уже дефолтит его на то же самое значение
    # (http://vault.vault-system.svc:8200), поэтому ничего дополнительно
    # инжектить здесь не нужно.
    "credential-issuer-service": ["postgresql"],
    # services/incident-service/cmd/incident-service/main.go: только
    # pgxpool.Pool (buildPostgresDSN -> POSTGRES_*) — своя схема incident.*
    # (migrations/V028__incident.sql), никакого Redis/Kafka/ClickHouse.
    "incident-service": ["postgresql"],
    # services/ops-visibility-service/cmd/ops-visibility-service/main.go:
    # REDIS_RUNTIME_* (снапшот-хранилище, короткий TTL, см. README.md) +
    # KAFKA_BOOTSTRAP_SERVERS — но Kafka bootstrap-адрес нигде в этом
    # кодбейзе не идёт через SECRET_DEPENDENCIES (не секрет, обычный plain
    # env с дефолтом kafka-bootstrap.mpp.svc:9092 — та же конвенция, что
    # execution-control-service/config-cache-projector и все остальные
    # Kafka-потребители ниже, ни один из них не несёт запись "kafka" в
    # этой таблице). Никакого Postgres — эта фаза явно не заводит таблицу
    # трендов (ops.health_snapshots отложена планом).
    "ops-visibility-service": ["redis-runtime"],
    "configuration-service": ["postgresql"],
    "config-cache-projector": ["redis-configuration"],
    "consent-cache-projector": ["redis-runtime"],
    "dlr-correlation-writer": ["postgresql"],
    "billing-outbox-publisher": ["redis-billing"],
    "billing-ledger-writer": ["postgresql"],
    "billing-reconciliation": ["redis-billing", "postgresql"],
    "partner-api": ["postgresql", "clickhouse"],
    "backoffice-api": ["postgresql", "clickhouse"],
    "replay-service": ["postgresql"],
    "lifecycle-writer": ["postgresql"],
    "analytics-writer": ["clickhouse"],
    "pdu-log-writer": ["clickhouse"],
    "partner-notification-service": ["redis-runtime"],
}

# HIGH находка кодревью (PART 2, partner-rest-receiver #4): EnvAuthVerifier
# (services/partner-rest-receiver/src/auth.rs) ищет `PARTNER_CRED_*`
# переменные окружения, детерминированно построенные из `credential_ref`
# каждого application'а — но ни здесь, ни в infra/secrets/ для них не было
# ни одной записи, и ни для одного из двух consumer'ов статического
# partner-конфига (partner-rest-receiver, partner-notification-service)
# не был wiring даже для самого файла конфига (`PARTNER_CONFIG_PATH` нигде
# не выставлялся -> откат на relative dev-путь, которого в образе нет ->
# паника при старте). Реальный fix (полноценный per-partner Vault-клиент,
# либо config.changes-based динамическая загрузка) — за пределами этого
# среза (см. auth.rs/README про статический файл вместо Vault-клиента),
# но то, что ЕСТЬ (EnvAuthVerifier поверх env vars) можно и нужно реально
# завести на единственного тестового партнёра, который этот срез
# моделирует (partner.valid.json) — не оставлять полностью мёртвым.
# (PARTNER_CONFIG_SERVICES/PARTNER_CONFIG_MOUNT_DIR/PARTNER_CONFIG_FIXTURE — см. константы вверху файла.)

for _svc in SERVICES:
    _svc.secrets = SECRET_DEPENDENCIES.get(_svc.name, [])
    if _svc.name == "partner-rest-receiver":
        _svc.secrets = [*_svc.secrets, "partner-credentials"]


def replicas_for(svc: Service) -> int:
    profile = INSTANCE_PROFILE[svc.lang]
    if svc.vcpu_total is None:
        return MIN_REPLICAS
    return max(MIN_REPLICAS, math.ceil(svc.vcpu_total / profile["cpu"]))


def _resources(svc: Service, guaranteed: bool) -> dict:
    if svc.resource_tier == "control-plane":
        # Изолированная ветка, не проходит через языковую арифметику ниже —
        # та ломается на дробных vCPU из-за форматирования f"{cpu}000m".
        return CONTROL_PLANE_RESOURCES
    profile = INSTANCE_PROFILE[svc.lang]
    limits = {"cpu": f"{profile['cpu']}000m", "memory": f"{profile['mem_gi']}Gi"}
    if guaranteed:
        requests = dict(limits)
    else:
        # Burstable QoS для pooled control-plane сервисов без выделенной capacity_model-записи —
        # запас по факту не посчитан, не даём им Guaranteed-приоритет над hot-path сервисами.
        requests = {"cpu": f"{profile['cpu'] * 500}m", "memory": f"{profile['mem_gi'] // 2}Gi"}
    return {"limits": limits, "requests": requests}


def _probes() -> dict:
    return {
        "livenessProbe": {
            "httpGet": {"path": "/healthz", "port": HEALTH_PORT},
            "initialDelaySeconds": 10, "periodSeconds": 15, "failureThreshold": 3,
        },
        "readinessProbe": {
            "httpGet": {"path": "/readyz", "port": HEALTH_PORT},
            "initialDelaySeconds": 5, "periodSeconds": 10, "failureThreshold": 3,
        },
    }


# Секретный ключ (SECRET_DEPENDENCIES) -> имя k8s Secret, создаваемого ExternalSecret
# (infra/secrets/generate_external_secrets.py) — единственное место, где эта связь
# зафиксирована, оба генератора читают её.
SECRET_K8S_NAME = {
    "postgresql": "postgresql-credentials",
    "redis-runtime": "redis-runtime-credentials",
    "redis-configuration": "redis-configuration-credentials",
    "redis-billing": "redis-billing-credentials",
    "clickhouse": "clickhouse-credentials",
    # Форма отличается от пяти секретов выше (один Vault-путь = одна запись
    # в этом Secret) — у partner-credentials каждый ключ (один на
    # credential_ref) читает СВОЙ собственный Vault-путь, не общую запись.
    # infra/secrets/generate_external_secrets.py обрабатывает этот ключ
    # отдельным билдером (build_partner_credentials_external_secret), не
    # общим циклом по SECRET_KEYS/VAULT_PATH — см. комментарий там.
    "partner-credentials": "partner-credentials",
}


def _container(svc: Service) -> dict:
    guaranteed = svc.workload_class in ("sticky-statefulset", "kafka-streams-statefulset") or svc.vcpu_total is not None
    ports = [{"containerPort": HEALTH_PORT, "name": "health"}]
    if svc.external_port:
        ports.append({"containerPort": svc.external_port["port"], "name": svc.external_port["name"]})
    if svc.internal_grpc_port:
        ports.append({"containerPort": svc.internal_grpc_port, "name": "grpc"})
    container = {
        "name": svc.name,
        "image": f"{IMAGE_REGISTRY}/{svc.name}:latest",
        "ports": ports,
        "resources": _resources(svc, guaranteed),
        **_probes(),
    }
    volume_mounts = []
    if svc.rocksdb_pvc_gi:
        volume_mounts.append({"name": "rocksdb-state", "mountPath": "/var/lib/rocksdb"})
    if svc.name in PARTNER_CONFIG_SERVICES:
        volume_mounts.append({"name": "partner-config", "mountPath": PARTNER_CONFIG_MOUNT_DIR, "readOnly": True})
    if volume_mounts:
        container["volumeMounts"] = volume_mounts
    if svc.secrets:
        container["envFrom"] = [{"secretRef": {"name": SECRET_K8S_NAME[key]}} for key in svc.secrets]
    if svc.name in PARTNER_CONFIG_SERVICES:
        # Оба Go/Rust consumer'а (main.rs/main.go) читают именно
        # PARTNER_CONFIG_PATH, откатываясь на relative dev-путь, если её
        # нет — тот путь в реальном образе не существует (см. комментарий у
        # PARTNER_CONFIG_SERVICES выше).
        container["env"] = [{"name": "PARTNER_CONFIG_PATH", "value": f"{PARTNER_CONFIG_MOUNT_DIR}/{PARTNER_CONFIG_FIXTURE}"}]
    return container


def build_service_account(svc: Service) -> dict:
    """До этого изменения этот генератор вообще не создавал ServiceAccount —
    каждый под неявно работал под `default` ServiceAccount namespace `mpp`.
    Это молча ломало Vault Kubernetes auth (infra/terraform/vault-secrets.tf):
    `vault_kubernetes_auth_backend_role.credential_issuer_service` и
    `.partner_credential_readers` биндятся на `bound_service_account_names`
    (`credential-issuer-service`, `partner-rest-receiver`,
    `partner-smpp-gateway`), которые без этой функции никогда не существовали
    бы и ни один под никогда бы их не предъявил. Заведено для КАЖДОГО
    сервиса (не только этих трёх) — единообразно и с наименьшими
    привилегиями: остальные сервисы сегодня не нуждаются в отдельном Vault
    role, но у каждого всё равно своя identity, а не общий `default`, и
    ничего не нужно будет специально заводить, если такая потребность
    появится позже. Имя ServiceAccount совпадает с именем сервиса байт-в-
    байт — это ровно то, что `bound_service_account_names` в Terraform
    ожидает увидеть."""
    return {
        "apiVersion": "v1", "kind": "ServiceAccount",
        "metadata": _metadata(svc),
    }


def _pod_template(svc: Service) -> dict:
    spec = {
        "serviceAccountName": svc.name,
        "containers": [_container(svc)],
        "terminationGracePeriodSeconds": 60 if svc.workload_class != "kafka-streams-statefulset" else 120,
    }
    if svc.workload_class == "sticky-statefulset":
        # Разъезд по нодам важен для доступности живых bind-соединений при отказе ноды.
        spec["topologySpreadConstraints"] = [{
            "maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"app": svc.name}},
        }]
    if svc.name in PARTNER_CONFIG_SERVICES:
        spec["volumes"] = [{"name": "partner-config", "configMap": {"name": PARTNER_CONFIG_CONFIGMAP_NAME}}]
    return {
        "metadata": {"labels": {"app": svc.name, "mpp.io/workload-class": svc.workload_class}},
        "spec": spec,
    }


def _metadata(svc: Service) -> dict:
    return {"name": svc.name, "namespace": NAMESPACE, "labels": {"app": svc.name, "mpp.io/lang": svc.lang}}


def build_workload(svc: Service, replicas: int) -> dict:
    pod_template = _pod_template(svc)
    if svc.workload_class in ("stateless", "frontend"):
        return {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": _metadata(svc),
            "spec": {
                "replicas": replicas,
                "selector": {"matchLabels": {"app": svc.name}},
                "strategy": {"type": "RollingUpdate",
                             "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}},
                "template": pod_template,
            },
        }

    # StatefulSet — оба sticky-класса нуждаются в стабильной identity/DNS.
    spec = {
        "serviceName": f"{svc.name}-headless",
        "replicas": replicas,
        "podManagementPolicy": "Parallel" if svc.workload_class == "sticky-statefulset" else "OrderedReady",
        "selector": {"matchLabels": {"app": svc.name}},
        "updateStrategy": {"type": "RollingUpdate",
                            "rollingUpdate": {"partition": 0}},
        "template": pod_template,
    }
    if svc.rocksdb_pvc_gi:
        spec["volumeClaimTemplates"] = [{
            "metadata": {"name": "rocksdb-state"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": f"{svc.rocksdb_pvc_gi}Gi"}},
            },
        }]
    return {"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": _metadata(svc), "spec": spec}


def build_headless_service(svc: Service) -> dict:
    ports = [{"name": "health", "port": HEALTH_PORT, "targetPort": HEALTH_PORT}]
    if svc.internal_grpc_port:
        ports.append({"name": "grpc", "port": svc.internal_grpc_port, "targetPort": svc.internal_grpc_port})
    return {
        "apiVersion": "v1", "kind": "Service",
        "metadata": _metadata(svc) | {"name": f"{svc.name}-headless"},
        "spec": {"clusterIP": "None", "selector": {"app": svc.name}, "ports": ports},
    }


def build_external_service(svc: Service) -> dict:
    port = svc.external_port
    service_type = "LoadBalancer" if svc.workload_class == "sticky-statefulset" else "ClusterIP"
    return {
        "apiVersion": "v1", "kind": "Service",
        "metadata": _metadata(svc) | {"name": svc.name},
        "spec": {
            "type": service_type,
            "selector": {"app": svc.name},
            "ports": [{"name": port["name"], "port": port["port"], "targetPort": port["port"],
                       "protocol": port["protocol"]}],
        },
    }


def build_ingress(svc: Service) -> dict:
    """Только для внешних HTTP ClusterIP-сервисов (build_external_service даёт
    им ClusterIP именно потому, что workload_class != sticky-statefulset — тот
    же признак здесь переиспользуется, не заводится отдельное поле). SMPP
    Gateway (raw TCP) и Operator HTTP Gateway (sticky — route ownership,
    уже на собственном LoadBalancer) через Ingress не идут, L7-роутинг для
    них не имеет смысла/уже устроен иначе — см. infra/README.md."""
    host = f"{svc.name}.{EXTERNAL_DOMAIN}"
    port = svc.external_port
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
        "metadata": _metadata(svc) | {
            "name": svc.name,
            "annotations": {"cert-manager.io/cluster-issuer": CLUSTER_ISSUER},
        },
        "spec": {
            "ingressClassName": INGRESS_CLASS,
            "tls": [{"hosts": [host], "secretName": f"{svc.name}-tls"}],
            "rules": [{
                "host": host,
                "http": {"paths": [{
                    "path": "/", "pathType": "Prefix",
                    "backend": {"service": {"name": svc.name, "port": {"number": port["port"]}}},
                }]},
            }],
        },
    }


def build_pdb(svc: Service, replicas: int) -> dict:
    max_unavailable = 1 if svc.workload_class in ("sticky-statefulset", "kafka-streams-statefulset") else max(1, replicas // 4)
    return {
        "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
        "metadata": _metadata(svc),
        "spec": {"maxUnavailable": max_unavailable, "selector": {"matchLabels": {"app": svc.name}}},
    }


def build_keda_scaledobject(svc: Service, replicas: int) -> dict:
    """KEDA, не голый HPA — новое архитектурное решение этого LLD-шага: CPU
    плохо коррелирует с нагрузкой у Kafka-consumer сервисов, реальный сигнал
    — consumer lag на топике стадии (это же используется в мониторинге,
    hld.md: 'Kafka consumer lag' в списке метрик). Ни один документ раньше
    не выбирал конкретный autoscaler — здесь выбор сделан явно."""
    return {
        "apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
        "metadata": _metadata(svc) | {"name": f"{svc.name}-scaler"},
        "spec": {
            "scaleTargetRef": {"name": svc.name},
            "minReplicaCount": replicas,
            "maxReplicaCount": replicas * 3,
            "triggers": [{
                "type": "kafka",
                "metadata": {
                    "bootstrapServers": "kafka-bootstrap.mpp.svc:9092",
                    "consumerGroup": svc.name,
                    "topic": f"stage.{svc.name}",
                    "lagThreshold": "1000",
                },
            }],
        },
    }


def build_partner_config_configmap() -> dict:
    """Часть исправления HIGH находки кодревью (PART 2, partner-rest-receiver
    #4) — без этого ConfigMap `PARTNER_CONFIG_PATH` не выставлялась вообще,
    оба consumer'а (partner-rest-receiver/main.rs, partner-notification-service/main.go)
    откатывались на relative dev-путь и падали при старте до того, как вопрос
    авторизации вообще возникал. Тот же файл, что оба сервиса уже грузят по
    умолчанию в dev/тестах (`config_schemas/examples/partner.valid.json`) —
    не новые данные, просто реально смонтирован в контейнер вместо
    несуществующего пути."""
    fixture_path = Path(__file__).parent.parent / "config_schemas" / "examples" / PARTNER_CONFIG_FIXTURE
    return {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": PARTNER_CONFIG_CONFIGMAP_NAME, "namespace": NAMESPACE},
        "data": {PARTNER_CONFIG_FIXTURE: fixture_path.read_text()},
    }


def render_service(svc: Service) -> list[dict]:
    replicas = replicas_for(svc)
    docs = [build_service_account(svc), build_workload(svc, replicas), build_pdb(svc, replicas)]
    if svc.workload_class in ("sticky-statefulset", "kafka-streams-statefulset"):
        docs.append(build_headless_service(svc))
    if svc.external_port:
        docs.append(build_external_service(svc))
        if svc.workload_class != "sticky-statefulset":  # ClusterIP HTTP-сервисы — см. build_ingress
            docs.append(build_ingress(svc))
    if svc.kafka_consumer and svc.workload_class == "stateless":
        docs.append(build_keda_scaledobject(svc, replicas))
    return docs


def main():
    out_dir = Path(__file__).parent / "rendered"
    out_dir.mkdir(exist_ok=True)
    for f in out_dir.glob("*.yaml"):
        f.unlink()

    namespace_doc = {
        "apiVersion": "v1", "kind": "Namespace",
        "metadata": {
            "name": NAMESPACE,
            # mTLS = Istio (infra/terraform/istio.tf, infra/istio/) — sidecar-инъекция
            # включается на namespace, PeerAuthentication STRICT требует её для всех подов.
            "labels": {"istio-injection": "enabled"},
        },
    }
    (out_dir / "00-namespace.yaml").write_text(yaml.dump(namespace_doc, sort_keys=False))
    (out_dir / "00-partner-config.yaml").write_text(yaml.dump(build_partner_config_configmap(), sort_keys=False))

    summary = []
    for svc in SERVICES:
        docs = render_service(svc)
        path = out_dir / f"{svc.name}.yaml"
        path.write_text(yaml.dump_all(docs, sort_keys=False))
        summary.append((svc.name, svc.workload_class, replicas_for(svc), len(docs)))

    print(f"{len(SERVICES)} сервисов -> {out_dir}/")
    for name, cls, replicas, doc_count in summary:
        print(f"  {name:38s} {cls:26s} replicas={replicas:<3d} docs={doc_count}")


if __name__ == "__main__":
    main()
