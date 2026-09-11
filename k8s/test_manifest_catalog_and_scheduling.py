"""Production invariants for the Kubernetes service catalog.

These checks deliberately compare independent sources (runnable Docker build
contexts, Kubernetes catalog, Terraform registry and tainted node pools). A
test that only re-read SERVICES would not detect catalog drift.
"""

import re
from pathlib import Path

from generate_manifests import (
    KAFKA_BOOTSTRAP_SERVERS,
    KEDA_KAFKA_TRIGGERS,
    KAFKA_NON_CONSUMER_CLIENTS,
    PROMETHEUS_URL,
    SERVICES,
    build_workload,
    node_pool_for,
    render_service,
    replicas_for,
)

ROOT = Path(__file__).parent.parent

# Acceptance-set from the actual consumer construction in service entrypoints.
# Deliberately independent from KEDA_KAFKA_TRIGGERS so a bad edit cannot make
# both generator and expectation drift together.
EXPECTED_KEDA_TRIGGERS = {
    "pipeline-engine": {
        ("incoming.messages", "pipeline-engine"),
        ("stage.completed", "pipeline-engine"),
        ("pipeline.retry.triggers", "pipeline-engine-retry-trigger"),
    },
    "destination-resolution-service": {("stage.destination-resolution", "destination-resolution-service")},
    "policy-service": {("stage.policy", "policy-service")},
    "billing-service": {("stage.billing", "billing-service")},
    "routing-service": {("stage.routing", "routing-service")},
    "delivery-service": {("stage.delivery", "delivery-service")},
    "delivery-reconciliation-service": {
        ("stage.delivery-reconciliation", "delivery-reconciliation-service"),
        ("operator.submit.accepted", "delivery-reconciliation-service-submit-accepted"),
        ("delivery.status", "delivery-reconciliation-service-delivery-status"),
    },
    "config-cache-projector": {("config.changes", "config-cache-projector")},
    "consent-cache-projector": {("config.changes", "consent-cache-projector")},
    "dlr-correlation-writer": {("operator.submit.accepted", "dlr-correlation-writer")},
    "dlr-manager": {("operator.dlr", "dlr-manager"), ("operator.dlr.unresolved", "dlr-manager")},
    "billing-ledger-writer": {("billing.ledger", "billing-ledger-writer")},
    "lifecycle-writer": {
        ("incoming.messages", "lifecycle-writer"),
        ("message.lifecycle", "lifecycle-writer"),
        ("stage.destination-resolution.dlq", "lifecycle-writer"),
        ("stage.policy.dlq", "lifecycle-writer"),
        ("stage.billing.dlq", "lifecycle-writer"),
        ("stage.routing.dlq", "lifecycle-writer"),
        ("stage.delivery.dlq", "lifecycle-writer"),
        ("stage.delivery-reconciliation.dlq", "lifecycle-writer"),
    },
    "analytics-writer": {
        ("incoming.messages", "analytics-writer"),
        ("stage.completed", "analytics-writer"),
        ("message.lifecycle", "analytics-writer"),
    },
    "pdu-log-writer": {("operator.pdu.log", "pdu-log-writer")},
    "partner-notification-service": {
        ("message.lifecycle", "partner-notification-service"),
        ("notification.retry", "partner-notification-service"),
    },
    "template-management-service": {("config.changes", "template-management-service")},
}


def _service_names_from_registry() -> set[str]:
    source = (ROOT / "infra" / "terraform" / "registry.tf").read_text()
    match = re.search(r"service_names\s*=\s*\[(.*?)\]", source, re.DOTALL)
    assert match is not None, "infra/terraform/registry.tf has no service_names list"
    return set(re.findall(r'"([a-z0-9-]+)"', match.group(1)))


def _terraform_node_pools() -> set[str]:
    source = (ROOT / "infra" / "terraform" / "variables.tf").read_text()
    return set(re.findall(r"^\s*([a-z]+-pool)\s*=", source, re.MULTILINE))


def _provisioned_kafka_topics() -> set[str]:
    source = (ROOT / "infra" / "kafka" / "generate_kafka_topics.py").read_text()
    return set(re.findall(r'Topic\("([a-z0-9.-]+)"', source))


def _pod_spec(svc) -> dict:
    workload = build_workload(svc, replicas_for(svc))
    return workload["spec"]["template"]["spec"]


def _container(svc) -> dict:
    return _pod_spec(svc)["containers"][0]


def test_every_runnable_service_is_in_kubernetes_catalog_exactly_once():
    runnable = {dockerfile.parent.name for dockerfile in (ROOT / "services").glob("*/Dockerfile")}
    catalog = [svc.name for svc in SERVICES]
    assert len(catalog) == len(set(catalog)), "SERVICES contains duplicate names"
    assert set(catalog) == runnable, (
        f"only runnable={sorted(runnable - set(catalog))}; "
        f"only Kubernetes catalog={sorted(set(catalog) - runnable)}"
    )


def test_terraform_registry_matches_kubernetes_catalog():
    catalog = {svc.name for svc in SERVICES}
    registry = _service_names_from_registry()
    assert registry == catalog, (
        f"only Kubernetes catalog={sorted(catalog - registry)}; "
        f"only registry={sorted(registry - catalog)}"
    )


def test_every_pod_selects_and_tolerates_an_existing_tainted_pool():
    pools = _terraform_node_pools()
    assert pools == {"rust-pool", "java-pool", "go-pool", "sticky-pool"}
    for svc in SERVICES:
        expected_pool = node_pool_for(svc)
        assert expected_pool in pools, f"{svc.name}: unknown node pool {expected_pool}"
        pod_spec = _pod_spec(svc)
        assert pod_spec["nodeSelector"] == {"mpp.io/workload-class": expected_pool}
        assert {
            "key": "mpp.io/workload-class",
            "operator": "Equal",
            "value": expected_pool,
            "effect": "NoSchedule",
        } in pod_spec["tolerations"]


def test_internal_http_services_have_cluster_ip_but_no_public_ingress():
    for svc in SERVICES:
        if not svc.internal_http_port:
            continue
        docs = render_service(svc)
        services = [doc for doc in docs if doc["kind"] == "Service"]
        assert any(
            doc["metadata"]["name"] == svc.name
            and doc["spec"]["type"] == "ClusterIP"
            and doc["spec"]["ports"][0]["port"] == svc.internal_http_port
            for doc in services
        ), f"{svc.name}: missing internal HTTP ClusterIP"
        assert not any(doc["kind"] == "Ingress" for doc in docs), (
            f"{svc.name}: internal-only HTTP service must not get a public Ingress"
        )


def test_stateless_grpc_servers_have_discoverable_cluster_ip():
    for svc in SERVICES:
        if not svc.internal_grpc_port or svc.workload_class != "stateless":
            continue
        docs = render_service(svc)
        service = next(
            doc for doc in docs
            if doc["kind"] == "Service" and doc["metadata"]["name"] == svc.name
        )
        assert service["spec"]["type"] == "ClusterIP"
        assert service["spec"]["ports"] == [{
            "name": "grpc",
            "port": svc.internal_grpc_port,
            "targetPort": svc.internal_grpc_port,
            "protocol": "TCP",
        }]


def test_runtime_infrastructure_addresses_match_installed_resources():
    # infra/kafka/generate_kafka_topics.py creates Kafka metadata.name=mpp-kafka;
    # Strimzi's bootstrap Service naming contract is <name>-kafka-bootstrap.
    assert KAFKA_BOOTSTRAP_SERVERS == "mpp-kafka-kafka-bootstrap.mpp.svc:9092"
    assert PROMETHEUS_URL == "http://prometheus-operated.monitoring.svc:9090"

    for svc in SERVICES:
        env = {item["name"]: item["value"] for item in _container(svc).get("env", [])}
        if svc.kafka_consumer or svc.name in KAFKA_NON_CONSUMER_CLIENTS:
            assert env.get("KAFKA_BOOTSTRAP_SERVERS") == KAFKA_BOOTSTRAP_SERVERS, svc.name
        if svc.name == "execution-control-service":
            assert env.get("PROMETHEUS_URL") == PROMETHEUS_URL

        for doc in render_service(svc):
            if doc["kind"] == "ScaledObject":
                trigger = doc["spec"]["triggers"][0]
                assert trigger["metadata"]["bootstrapServers"] == KAFKA_BOOTSTRAP_SERVERS


def test_keda_uses_real_consumer_topics_and_groups():
    actual_mapping = {
        service: set(triggers)
        for service, triggers in KEDA_KAFKA_TRIGGERS.items()
    }
    assert actual_mapping == EXPECTED_KEDA_TRIGGERS

    expected_scaled_services = {
        svc.name for svc in SERVICES
        if svc.kafka_consumer and svc.workload_class == "stateless"
    }
    assert set(actual_mapping) == expected_scaled_services

    provisioned_topics = _provisioned_kafka_topics()
    for svc in SERVICES:
        if svc.name not in EXPECTED_KEDA_TRIGGERS:
            continue
        scaled_object = next(doc for doc in render_service(svc) if doc["kind"] == "ScaledObject")
        rendered = {
            (trigger["metadata"]["topic"], trigger["metadata"]["consumerGroup"])
            for trigger in scaled_object["spec"]["triggers"]
        }
        assert rendered == EXPECTED_KEDA_TRIGGERS[svc.name]
        assert {topic for topic, _ in rendered} <= provisioned_topics


def test_container_port_names_and_numbers_are_unique():
    for svc in SERVICES:
        ports = _container(svc)["ports"]
        names = [port["name"] for port in ports]
        numbers = [port["containerPort"] for port in ports]
        assert len(names) == len(set(names)), f"{svc.name}: duplicate container port name"
        assert len(numbers) == len(set(numbers)), f"{svc.name}: duplicate container port number"


if __name__ == "__main__":
    tests = [value for name, value in list(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\n{len(tests)}/{len(tests)} тестов прошли")
