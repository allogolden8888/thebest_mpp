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

import math
from dataclasses import dataclass, field
from pathlib import Path

import yaml

NAMESPACE = "mpp"
IMAGE_REGISTRY = "registry.mpp.internal"
HEALTH_PORT = 9090  # /metrics (Prometheus), /healthz, /readyz — новая платформенная конвенция,
                     # ни в одном из HLD/LLD документов порт/путь не был зафиксирован явно.

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
            internal_grpc_port=9000),
    Service("configuration-service", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", internal_grpc_port=9000),
    Service("config-event-publisher", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", kafka_consumer=False),
    Service("config-cache-projector", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:12,113)", kafka_consumer=True),
    Service("consent-cache-projector", "go", "stateless", None,
            "не указан ни в одной vCPU-таблице (services_specifictaion.md:703-707,1131)", kafka_consumer=True),
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
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}),
    Service("backoffice-api", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)",
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}, internal_grpc_port=9000),
    Service("replay-service", "go", "stateless", None,
            "пул 'Мелкие Go control-plane' (capacity_model.md:113)", internal_grpc_port=9000),
    Service("lifecycle-writer", "go", "stateless", 8,
            "capacity_model.md:116 (v2)", kafka_consumer=True),
    Service("analytics-writer", "go", "stateless", 16,
            "capacity_model.md:117", kafka_consumer=True),
    Service("partner-notification-service", "go", "stateless", 24,
            "capacity_model.md:118", kafka_consumer=True, internal_grpc_port=9000),
    Service("backoffice-ui", "go", "frontend", None,
            "не в capacity model — статический SPA, floor MIN_REPLICAS",
            external_port={"name": "http", "port": 8080, "protocol": "TCP"}),
]

# Секретные зависимости по сервису — из четырёх ключей ниже, каждый мапится
# на отдельный k8s Secret (SECRET_K8S_NAME), наполняемый ExternalSecret из
# Vault (infra/secrets/). Источник — services_specifictaion.md (упоминания
# "Runtime Redis"/"Configuration Redis"/"Billing Redis"/"PostgreSQL"/
# "ClickHouse" по каждому сервису); там, где документ не называет хранилище
# явно (DLR Manager, Config Event Publisher), секрет не назначен — не
# додумано, см. infra/README.md.
SECRET_DEPENDENCIES: dict[str, list[str]] = {
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
    "message-state-resolver": ["redis-configuration"],
    "execution-control-service": ["postgresql", "redis-configuration"],
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
    "partner-notification-service": ["redis-runtime"],
}
for _svc in SERVICES:
    _svc.secrets = SECRET_DEPENDENCIES.get(_svc.name, [])


def replicas_for(svc: Service) -> int:
    profile = INSTANCE_PROFILE[svc.lang]
    if svc.vcpu_total is None:
        return MIN_REPLICAS
    return max(MIN_REPLICAS, math.ceil(svc.vcpu_total / profile["cpu"]))


def _resources(svc: Service, guaranteed: bool) -> dict:
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
    if svc.rocksdb_pvc_gi:
        container["volumeMounts"] = [{"name": "rocksdb-state", "mountPath": "/var/lib/rocksdb"}]
    if svc.secrets:
        container["envFrom"] = [{"secretRef": {"name": SECRET_K8S_NAME[key]}} for key in svc.secrets]
    return container


def _pod_template(svc: Service) -> dict:
    spec = {
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


def render_service(svc: Service) -> list[dict]:
    replicas = replicas_for(svc)
    docs = [build_workload(svc, replicas), build_pdb(svc, replicas)]
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
