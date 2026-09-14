"""Production invariants for the Kubernetes service catalog.

These checks deliberately compare independent sources (runnable Docker build
contexts, Kubernetes catalog, Terraform registry and tainted node pools). A
test that only re-read SERVICES would not detect catalog drift.
"""

import re
from pathlib import Path

from generate_manifests import (
    DISTROLESS_NONROOT_UID,
    KAFKA_BOOTSTRAP_SERVERS,
    KEDA_KAFKA_TRIGGERS,
    KAFKA_NON_CONSUMER_CLIENTS,
    NONROOT_UID,
    PROMETHEUS_URL,
    SERVICES,
    _pod_security_uid_gid,
    _writable_paths,
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
        # KAFKA_SASL_PASSWORD (KAFKA_SASL_DEMO_SERVICES pilot, BACKOFFICE_ROADMAP.md
        # P1) is the one env entry that is a secretKeyRef, not a plain "value" —
        # skip those here, checked separately below.
        env = {item["name"]: item["value"] for item in _container(svc).get("env", []) if "value" in item}
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


# BACKOFFICE_ROADMAP.md P1 "Security": "ни один Dockerfile не задаёт USER,
# нет pod security context". Every Dockerfile now runs its final stage as a
# verified non-root uid (`grep -L "^USER " services/*/Dockerfile` — 0 hits);
# these tests catch the generator regressing back to an implicit-root pod,
# independent of whatever the Dockerfiles say (a test that only re-read
# Dockerfiles would not catch the generator dropping the field).
def test_every_pod_runs_as_the_verified_non_root_uid_from_its_dockerfile():
    for svc in SERVICES:
        pod_spec = _pod_spec(svc)
        pod_sc = pod_spec["securityContext"]
        expected_uid = _pod_security_uid_gid(svc)
        # frontend (nginx:alpine) and non-Go (eclipse-temurin/debian:bookworm-slim)
        # families all explicitly groupadd/useradd (or Alpine's addgroup/adduser)
        # the same 10001:10001 in their Dockerfile; only distroless Go images use
        # the baked-in 65532 nonroot convention instead.
        assert expected_uid in (DISTROLESS_NONROOT_UID, NONROOT_UID), svc.name
        assert pod_sc == {
            "runAsNonRoot": True,
            "runAsUser": expected_uid,
            "runAsGroup": expected_uid,
            "fsGroup": expected_uid,
            "seccompProfile": {"type": "RuntimeDefault"},
        }, f"{svc.name}: unexpected pod securityContext {pod_sc}"

        container_sc = _container(svc)["securityContext"]
        assert container_sc == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        }, f"{svc.name}: unexpected container securityContext {container_sc}"


def test_frontend_gets_the_shared_nonroot_uid_not_distroless():
    # backoffice-ui/partner-portal-ui carry lang="go" purely to select the
    # go-pool (node_pool_for) — their actual final-stage image is
    # nginx:1.27-alpine, not a Go distroless binary. A generator that
    # naively branched on svc.lang instead of workload_class would give them
    # uid 65532, which the Dockerfile's own `useradd -u 10001` does not
    # produce — this catches exactly that class of drift.
    frontends = [svc for svc in SERVICES if svc.workload_class == "frontend"]
    assert frontends, "expected at least one frontend service in the catalog"
    for svc in frontends:
        assert svc.lang == "go", svc.name  # sanity: still true today, see docstring above
        assert _pod_security_uid_gid(svc) == NONROOT_UID, svc.name


def test_writable_paths_are_backed_by_dedicated_read_write_empty_dirs():
    # readOnlyRootFilesystem: true (see test above) means every path a
    # service actually writes to (see _writable_paths' docstring for the
    # live-verified finding behind each one — JVM Attach API .java_pid
    # socket/JFR under /tmp, nginx's own /var/cache/nginx + pid file) must
    # have its own emptyDir volume mounted read-write, and nothing else
    # should silently gain a writable mount.
    for svc in SERVICES:
        pod_spec = _pod_spec(svc)
        container = _container(svc)
        volumes_by_name = {v["name"]: v for v in pod_spec.get("volumes", [])}
        mounts_by_name = {m["name"]: m for m in container.get("volumeMounts", [])}

        expected_paths = _writable_paths(svc)
        writable_mount_names = {name for name in mounts_by_name if name.startswith("writable-")}
        assert len(writable_mount_names) == len(expected_paths), svc.name

        for i, path in enumerate(expected_paths):
            name = f"writable-{i}"
            mount = mounts_by_name[name]
            assert mount["mountPath"] == path, f"{svc.name}: {name} mountPath"
            assert not mount.get("readOnly"), f"{svc.name}: {name} must be read-write"
            volume = volumes_by_name[name]
            assert "emptyDir" in volume, f"{svc.name}: {name} must be an emptyDir, got {volume}"

        if not expected_paths:
            # Go (distroless) and Rust (debian:bookworm-slim) services: verified
            # no on-disk writes (grep across every service's source, see
            # _writable_paths docstring) — readOnlyRootFilesystem: true holds
            # with zero extra volumes, not papered over with a wildcard mount.
            assert svc.lang in ("go", "rust"), (
                f"{svc.name}: lang={svc.lang} has no writable path but isn't go/rust — "
                "was a real write requirement missed?"
            )


if __name__ == "__main__":
    tests = [value for name, value in list(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\n{len(tests)}/{len(tests)} тестов прошли")
