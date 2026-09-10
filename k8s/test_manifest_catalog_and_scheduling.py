"""Production invariants for the Kubernetes service catalog.

These checks deliberately compare independent sources (runnable Docker build
contexts, Kubernetes catalog, Terraform registry and tainted node pools). A
test that only re-read SERVICES would not detect catalog drift.
"""

import re
from pathlib import Path

from generate_manifests import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_NON_CONSUMER_CLIENTS,
    PROMETHEUS_URL,
    SERVICES,
    build_workload,
    node_pool_for,
    render_service,
    replicas_for,
)

ROOT = Path(__file__).parent.parent


def _service_names_from_registry() -> set[str]:
    source = (ROOT / "infra" / "terraform" / "registry.tf").read_text()
    match = re.search(r"service_names\s*=\s*\[(.*?)\]", source, re.DOTALL)
    assert match is not None, "infra/terraform/registry.tf has no service_names list"
    return set(re.findall(r'"([a-z0-9-]+)"', match.group(1)))


def _terraform_node_pools() -> set[str]:
    source = (ROOT / "infra" / "terraform" / "variables.tf").read_text()
    return set(re.findall(r"^\s*([a-z]+-pool)\s*=", source, re.MULTILINE))


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
