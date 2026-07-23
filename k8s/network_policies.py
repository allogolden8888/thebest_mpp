"""
NetworkPolicy — L3/L4 сегментация (security LLD, roadmap-пункт, флагован как
незакрытый в k8s/README.md после генерации Deployment/StatefulSet-манифестов).

Модель — default-deny + явный allow, поверх той же таблицы SERVICES
(k8s/generate_manifests.py), плюс CALL_GRAPH — реальные прямые (не через
Kafka) вызовы между сервисами, взятые из hld.md (mermaid-диаграмма, стрелки
gRPC) и services_specifictaion.md.

Важное ограничение, которое НЕ нужно замалчивать: NetworkPolicy — это L3/L4
(IP/порт), она разрешает или запрещает TCP-соединение между подами по
label-селектору. Она НЕ проверяет identity/aутентификацию — это делает mTLS
поверх уже разрешённого соединения (hld.md: "mTLS gRPC to exact StatefulSet
pod", "gRPC + mTLS со стандартной балансировкой Kubernetes" — везде, где в
HLD упомянут gRPC, подразумевается mTLS). NetworkPolicy и mTLS — два
независимых слоя обороны: первый сокращает saved attack surface (кто вообще
может открыть TCP-сессию), второй проверяет, кто на самом деле на другом
конце уже открытой сессии. Реализация mTLS (service mesh vs. app-level TLS)
— решение, которое из этого шага сознательно НЕ принимается (см. README).

Все сервисы, потребляющие/публикующие Kafka, НЕ нуждаются в персональных
NetworkPolicy друг для друга — они не открывают соединения друг к другу
напрямую, только к брокеру. Одно общее правило "egress к kafka-bootstrap"
покрывает весь этот класс взаимодействия. Именно поэтому Billing-сервисы
(billing-service, billing-ledger-writer, billing-outbox-publisher) не
получают НИКАКИХ специальных ingress-правил ниже: по документам к ним никто
не обращается напрямую (только через Kafka) — изоляция Billing получается
как естественное следствие default-deny + пустого списка direct-call
ingress-правил, а не отдельным специальным механизмом.
"""

from pathlib import Path

import yaml

from generate_manifests import HEALTH_PORT, NAMESPACE, SERVICES

KAFKA_PORT = 9092
DNS_PORT = 53
PROMETHEUS_LABEL = "prometheus"  # ожидаемый label scraper-пода: app=prometheus

# (caller, callee) — прямые gRPC-вызовы, не через Kafka. Источник — цитаты в
# k8s/README.md ("Расхождение..." раздел уже покрыл billing/outbox;
# здесь — вызовы, не связанные с тем расхождением):
#   backoffice-api -> configuration-service   hld.md:417
#   backoffice-api -> execution-control-service  hld.md:418
#   backoffice-api -> replay-service           hld.md:421
#   billing-reconciliation -> execution-control-service  hld.md:353
#   partner-notification-service -> partner-smpp-gateway  hld.md:410,1038
#   delivery-service -> operator-smpp-session-manager   services_specifictaion.md:452, hld.md:1099
#   delivery-service -> operator-http-gateway           services_specifictaion.md:452, hld.md:1099
#   delivery-reconciliation-service -> operator-smpp-session-manager  services_specifictaion.md:487-488
CALL_GRAPH = [
    ("backoffice-api", "configuration-service"),
    ("backoffice-api", "execution-control-service"),
    ("backoffice-api", "replay-service"),
    ("billing-reconciliation", "execution-control-service"),
    ("partner-notification-service", "partner-smpp-gateway"),
    ("delivery-service", "operator-smpp-session-manager"),
    ("delivery-service", "operator-http-gateway"),
    ("delivery-reconciliation-service", "operator-smpp-session-manager"),
]

SERVICES_BY_NAME = {s.name: s for s in SERVICES}


def _meta(name: str) -> dict:
    return {"name": name, "namespace": NAMESPACE}


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
            "egress": [{"to": [{"namespaceSelector": {}}],
                        "ports": [{"protocol": "UDP", "port": DNS_PORT},
                                  {"protocol": "TCP", "port": DNS_PORT}]}],
        },
    }


def build_common_kafka_egress() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("allow-kafka-egress"),
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [{"to": [{"podSelector": {"matchLabels": {"app": "kafka-bootstrap"}}}],
                        "ports": [{"protocol": "TCP", "port": KAFKA_PORT}]}],
        },
    }


def build_common_prometheus_ingress() -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta("allow-prometheus-scrape-ingress"),
        "spec": {
            "podSelector": {},
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [{"podSelector": {"matchLabels": {"app": PROMETHEUS_LABEL}}}],
                         "ports": [{"protocol": "TCP", "port": HEALTH_PORT}]}],
        },
    }


def build_external_ingress(svc) -> dict:
    """Внешний inbound (partner/operator/оператор Backoffice) — ограничение
    источника здесь не k8s-задача (это делает cloud LB/firewall перед
    кластером), поэтому `from` намеренно не указан = разрешено с любого
    источника на этот порт. Это осознанное решение, не забытая проверка."""
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-external-ingress-{svc.name}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": svc.name}},
            "policyTypes": ["Ingress"],
            "ingress": [{"ports": [{"protocol": svc.external_port["protocol"], "port": svc.external_port["port"]}]}],
        },
    }


def build_internal_call_ingress(callee_name: str, caller_names: list[str], port: int) -> dict:
    return {
        "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
        "metadata": _meta(f"allow-internal-grpc-{callee_name}"),
        "spec": {
            "podSelector": {"matchLabels": {"app": callee_name}},
            "policyTypes": ["Ingress"],
            "ingress": [{
                "from": [{"podSelector": {"matchLabels": {"app": caller}}} for caller in caller_names],
                "ports": [{"protocol": "TCP", "port": port}],
            }],
        },
    }


def generate_all() -> list[dict]:
    docs = [
        build_default_deny(),
        build_common_dns_egress(),
        build_common_kafka_egress(),
        build_common_prometheus_ingress(),
    ]

    for svc in SERVICES:
        if svc.external_port:
            docs.append(build_external_ingress(svc))

    callers_by_callee: dict[str, list[str]] = {}
    for caller, callee in CALL_GRAPH:
        callers_by_callee.setdefault(callee, []).append(caller)
    for callee_name, caller_names in callers_by_callee.items():
        callee = SERVICES_BY_NAME[callee_name]
        port = callee.internal_grpc_port
        assert port is not None, f"{callee_name} — цель CALL_GRAPH, но internal_grpc_port не задан в SERVICES"
        docs.append(build_internal_call_ingress(callee_name, caller_names, port))

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
