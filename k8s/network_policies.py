"""
NetworkPolicy для namespace mpp: default-deny и минимальные L3/L4 allow.

CALL_GRAPH описывает прямые pod-to-pod вызовы, найденные в реальных
*_ADDR/*_URL defaults сервисов. Для каждого ребра генерируются ОБЕ стороны:
ingress callee и egress caller. Kafka-взаимодействия сюда не входят — к
брокерам разрешается отдельный egress только явному списку Kafka-клиентов.

NetworkPolicy не аутентифицирует вызывающий сервис; identity проверяет mTLS.
Здесь ограничивается только возможность открыть TCP-соединение.
"""

from dataclasses import dataclass
from pathlib import Path

import yaml

from generate_manifests import HEALTH_PORT, NAMESPACE, SERVICES

KAFKA_PORT = 9092
DNS_PORT = 53
VAULT_PORT = 8200
MONITORING_NAMESPACE = "monitoring"
VAULT_NAMESPACE = "vault-system"
NAMESPACE_NAME_LABEL = "kubernetes.io/metadata.name"

# Этот label явно задаётся Prometheus pod через
# infra/terraform/observability.tf, поэтому policy не зависит от внутренних
# default labels конкретной версии Helm chart.
PROMETHEUS_LABELS = {"app": "prometheus"}

# Kafka CR называется mpp-kafka. Strimzi размечает broker pods этими labels;
# имя bootstrap Service не является pod label и для podSelector не подходит.
KAFKA_BROKER_LABELS = {
    "strimzi.io/cluster": "mpp-kafka",
    "strimzi.io/name": "mpp-kafka-kafka",
}

# kafka_consumer в Service не покрывает producer-only процессы, поэтому здесь
# явный полный список фактических Kafka-клиентов из исполняемого кода.
KAFKA_CLIENTS = frozenset({
    "analytics-writer",
    "backoffice-api",
    "billing-ledger-writer",
    "billing-outbox-publisher",
    "billing-service",
    "config-cache-projector",
    "config-event-publisher",
    "consent-cache-projector",
    "delivery-reconciliation-service",
    "delivery-service",
    "destination-resolution-service",
    "dlr-correlation-writer",
    "dlr-manager",
    "execution-control-service",
    "lifecycle-writer",
    "message-state-resolver",
    "operator-http-gateway",
    "operator-smpp-session-manager",
    "ops-visibility-service",
    "partner-notification-service",
    "partner-rest-receiver",
    "partner-smpp-gateway",
    "pdu-log-writer",
    "pipeline-engine",
    "policy-service",
    "replay-service",
    "routing-service",
    "scheduler-background-lane",
    "scheduler-critical-sweep",
    "scheduler-standard-lane",
    "template-management-service",
})


@dataclass(frozen=True, order=True)
class DirectCall:
    caller: str
    callee: str
    port: int


@dataclass(frozen=True)
class NamespacedCall:
    caller: str
    namespace: str
    pod_labels: dict[str, str]
    port: int
    name: str


# Порт является частью ребра: старое предположение «все вызовы — gRPC»
# скрывало HTTP-вызовы compliance, templates и operational snapshot.
CALL_GRAPH = [
    DirectCall("backoffice-api", "chat-service", 9000),
    DirectCall("backoffice-api", "compliance-api", 8080),
    DirectCall("backoffice-api", "compliance-api", HEALTH_PORT),
    DirectCall("backoffice-api", "configuration-service", 9000),
    DirectCall("backoffice-api", "credential-issuer-service", 9000),
    DirectCall("backoffice-api", "execution-control-service", 9000),
    DirectCall("backoffice-api", "iam-service", 9000),
    DirectCall("backoffice-api", "incident-service", 9000),
    DirectCall("backoffice-api", "ops-visibility-service", HEALTH_PORT),
    DirectCall("backoffice-api", "replay-service", 9000),
    # SPA containers proxy same-origin browser calls through nginx. With
    # default-deny egress these are ordinary pod-to-pod calls too; allowing
    # only the API ingress half is insufficient.
    DirectCall("backoffice-ui", "backoffice-api", 8080),
    DirectCall("billing-reconciliation", "execution-control-service", 9000),
    DirectCall("billing-self-service-api", "configuration-service", 9000),
    DirectCall("compliance-api", "configuration-service", 9000),
    DirectCall("compliance-api", "iam-service", 9000),
    DirectCall("delivery-reconciliation-service", "operator-smpp-session-manager", 9000),
    DirectCall("delivery-service", "operator-http-gateway", 9000),
    DirectCall("delivery-service", "operator-smpp-session-manager", 9000),
    DirectCall("partner-notification-service", "partner-smpp-gateway", 9000),
    DirectCall("partner-portal-ui", "billing-self-service-api", 8080),
    DirectCall("partner-portal-ui", "partner-self-service-api", 8080),
    DirectCall("partner-self-service-api", "chat-service", 9000),
    DirectCall("partner-self-service-api", "configuration-service", 9000),
    DirectCall("partner-self-service-api", "credential-issuer-service", 9000),
    DirectCall("partner-self-service-api", "template-management-service", 8080),
]

# Межnamespace-вызовы, которые можно ограничить одновременно namespace И pod
# selector без широкого ipBlock. В одном NetworkPolicyPeer эти selectors
# пересекаются (AND), а не складываются как OR.
CROSS_NAMESPACE_CALLS = [
    NamespacedCall(
        "execution-control-service",
        MONITORING_NAMESPACE,
        PROMETHEUS_LABELS,
        9090,
        "prometheus-query",
    ),
    NamespacedCall(
        "credential-issuer-service",
        VAULT_NAMESPACE,
        {"app.kubernetes.io/name": "vault", "app.kubernetes.io/instance": "vault", "component": "server"},
        VAULT_PORT,
        "vault",
    ),
    NamespacedCall(
        "partner-rest-receiver",
        VAULT_NAMESPACE,
        {"app.kubernetes.io/name": "vault", "app.kubernetes.io/instance": "vault", "component": "server"},
        VAULT_PORT,
        "vault",
    ),
    NamespacedCall(
        "partner-smpp-gateway",
        VAULT_NAMESPACE,
        {"app.kubernetes.io/name": "vault", "app.kubernetes.io/instance": "vault", "component": "server"},
        VAULT_PORT,
        "vault",
    ),
]

SERVICES_BY_NAME = {service.name: service for service in SERVICES}


def _meta(name: str) -> dict:
    return {"name": name, "namespace": NAMESPACE}


def _namespace_pod_peer(namespace: str, pod_labels: dict[str, str]) -> dict:
    return {
        "namespaceSelector": {"matchLabels": {NAMESPACE_NAME_LABEL: namespace}},
        "podSelector": {"matchLabels": pod_labels},
    }


def build_default_deny() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("default-deny-all"),
        "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
    }


def build_common_dns_egress() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("allow-dns-egress"),
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [{
                "to": [_namespace_pod_peer("kube-system", {"k8s-app": "kube-dns"})],
                "ports": [
                    {"protocol": "UDP", "port": DNS_PORT},
                    {"protocol": "TCP", "port": DNS_PORT},
                ],
            }],
        },
    }


def build_common_kafka_egress() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("allow-kafka-egress"),
        "spec": {
            "podSelector": {"matchExpressions": [{
                "key": "app", "operator": "In", "values": sorted(KAFKA_CLIENTS),
            }]},
            "policyTypes": ["Egress"],
            "egress": [{
                "to": [{"podSelector": {"matchLabels": KAFKA_BROKER_LABELS}}],
                "ports": [{"protocol": "TCP", "port": KAFKA_PORT}],
            }],
        },
    }


def build_common_prometheus_ingress() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("allow-prometheus-scrape-ingress"),
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress"],
            "ingress": [{
                "from": [_namespace_pod_peer(MONITORING_NAMESPACE, PROMETHEUS_LABELS)],
                "ports": [{"protocol": "TCP", "port": HEALTH_PORT}],
            }],
        },
    }


def build_external_ingress(service) -> dict:
    """Внешний inbound ограничивается LB/firewall перед кластером.

    Здесь from не задан намеренно: сервисы с external_port должны принимать
    трафик от ingress/LB, чьи source IP/labels зависят от CNI и cloud LB.
    """
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-external-ingress-{service.name}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": service.name}},
            "policyTypes": ["Ingress"],
            "ingress": [{"ports": [{
                "protocol": service.external_port["protocol"],
                "port": service.external_port["port"],
            }]}],
        },
    }


def build_internal_call_ingress(callee_name: str, callers_by_port: dict[int, list[str]]) -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-internal-calls-to-{callee_name}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": callee_name}},
            "policyTypes": ["Ingress"],
            "ingress": [
                {
                    "from": [
                        {"podSelector": {"matchLabels": {"app": caller}}}
                        for caller in sorted(caller_names)
                    ],
                    "ports": [{"protocol": "TCP", "port": port}],
                }
                for port, caller_names in sorted(callers_by_port.items())
            ],
        },
    }


def build_internal_call_egress(caller_name: str, calls: list[DirectCall]) -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-internal-calls-from-{caller_name}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": caller_name}},
            "policyTypes": ["Egress"],
            "egress": [
                {
                    "to": [{"podSelector": {"matchLabels": {"app": call.callee}}}],
                    "ports": [{"protocol": "TCP", "port": call.port}],
                }
                for call in sorted(calls)
            ],
        },
    }


def build_namespaced_call_egress(call: NamespacedCall) -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-{call.name}-from-{call.caller}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": call.caller}},
            "policyTypes": ["Egress"],
            "egress": [{
                "to": [_namespace_pod_peer(call.namespace, call.pod_labels)],
                "ports": [{"protocol": "TCP", "port": call.port}],
            }],
        },
    }


def _served_ports(service) -> set[int]:
    ports = {HEALTH_PORT}
    for attr in ("internal_http_port", "internal_grpc_port", "external_port"):
        configured = getattr(service, attr, None)
        if isinstance(configured, int):
            ports.add(configured)
        elif configured:
            ports.add(configured["port"])
    return ports


def generate_all() -> list[dict]:
    docs = [
        build_default_deny(),
        build_common_dns_egress(),
        build_common_kafka_egress(),
        build_common_prometheus_ingress(),
    ]

    for service in SERVICES:
        if service.external_port:
            docs.append(build_external_ingress(service))

    unknown_kafka_clients = KAFKA_CLIENTS - SERVICES_BY_NAME.keys()
    assert not unknown_kafka_clients, f"Kafka clients missing from SERVICES: {sorted(unknown_kafka_clients)}"

    callers_by_callee: dict[str, dict[int, list[str]]] = {}
    calls_by_caller: dict[str, list[DirectCall]] = {}
    for call in CALL_GRAPH:
        assert call.caller in SERVICES_BY_NAME, f"CALL_GRAPH caller missing from SERVICES: {call.caller}"
        assert call.callee in SERVICES_BY_NAME, f"CALL_GRAPH callee missing from SERVICES: {call.callee}"
        assert call.port in _served_ports(SERVICES_BY_NAME[call.callee]), (
            f"{call.caller} -> {call.callee}:{call.port} targets a port not exposed by callee pod"
        )
        callers_by_callee.setdefault(call.callee, {}).setdefault(call.port, []).append(call.caller)
        calls_by_caller.setdefault(call.caller, []).append(call)

    for callee_name, callers_by_port in sorted(callers_by_callee.items()):
        docs.append(build_internal_call_ingress(callee_name, callers_by_port))
    for caller_name, calls in sorted(calls_by_caller.items()):
        docs.append(build_internal_call_egress(caller_name, calls))

    for call in CROSS_NAMESPACE_CALLS:
        assert call.caller in SERVICES_BY_NAME, f"Cross-namespace caller missing from SERVICES: {call.caller}"
        docs.append(build_namespaced_call_egress(call))

    return docs


def main():
    out_dir = Path(__file__).parent / "rendered"
    out_dir.mkdir(exist_ok=True)
    docs = generate_all()
    path = out_dir / "01-network-policies.yaml"
    path.write_text(yaml.dump_all(docs, sort_keys=False))
    print(f"{len(docs)} NetworkPolicy -> {path}")


if __name__ == "__main__":
    main()
