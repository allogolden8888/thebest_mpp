"""Семантические regression tests сгенерированных NetworkPolicy."""

from pathlib import Path

import yaml

from generate_manifests import HEALTH_PORT, SERVICES
from network_policies import (
    CALL_GRAPH,
    CROSS_NAMESPACE_CALLS,
    KAFKA_BROKER_LABELS,
    KAFKA_CLIENTS,
    KAFKA_PORT,
    MONITORING_NAMESPACE,
    NAMESPACE_NAME_LABEL,
    PROMETHEUS_LABELS,
    VAULT_NAMESPACE,
)

RENDERED = Path(__file__).parent / "rendered" / "01-network-policies.yaml"

# Независимый acceptance-set из фактических service endpoints. Он намеренно
# не вычисляется из CALL_GRAPH: удаление ребра из генератора должно ломать
# тест, а не уменьшать ожидание вместе с багом.
EXPECTED_DIRECT_CALLS = {
    ("backoffice-api", "chat-service", 9000),
    ("backoffice-api", "compliance-api", 8080),
    ("backoffice-api", "compliance-api", HEALTH_PORT),
    ("backoffice-api", "configuration-service", 9000),
    ("backoffice-api", "credential-issuer-service", 9000),
    ("backoffice-api", "execution-control-service", 9000),
    ("backoffice-api", "iam-service", 9000),
    ("backoffice-api", "incident-service", 9000),
    ("backoffice-api", "ops-visibility-service", HEALTH_PORT),
    ("backoffice-api", "replay-service", 9000),
    ("billing-reconciliation", "execution-control-service", 9000),
    ("billing-self-service-api", "configuration-service", 9000),
    ("compliance-api", "configuration-service", 9000),
    ("compliance-api", "iam-service", 9000),
    ("delivery-reconciliation-service", "operator-smpp-session-manager", 9000),
    ("delivery-service", "operator-http-gateway", 9000),
    ("delivery-service", "operator-smpp-session-manager", 9000),
    ("partner-notification-service", "partner-smpp-gateway", 9000),
    ("partner-self-service-api", "chat-service", 9000),
    ("partner-self-service-api", "configuration-service", 9000),
    ("partner-self-service-api", "credential-issuer-service", 9000),
    ("partner-self-service-api", "template-management-service", 8080),
}


def _load_docs() -> list[dict]:
    return list(yaml.safe_load_all(RENDERED.read_text()))


def _find(docs: list[dict], name: str) -> dict | None:
    return next((doc for doc in docs if doc["metadata"]["name"] == name), None)


def _rule_port(rule: dict) -> int:
    ports = rule.get("ports", [])
    assert len(ports) == 1, f"expected one exact port per internal rule, got {ports}"
    return ports[0]["port"]


def _allowed_ingress_callers(docs: list[dict], callee: str, port: int) -> set[str]:
    doc = _find(docs, f"allow-internal-calls-to-{callee}")
    if doc is None:
        return set()
    callers: set[str] = set()
    for rule in doc["spec"]["ingress"]:
        if _rule_port(rule) != port:
            continue
        callers.update(peer["podSelector"]["matchLabels"]["app"] for peer in rule["from"])
    return callers


def _allowed_egress_callees(docs: list[dict], caller: str, port: int) -> set[str]:
    doc = _find(docs, f"allow-internal-calls-from-{caller}")
    if doc is None:
        return set()
    callees: set[str] = set()
    for rule in doc["spec"]["egress"]:
        if _rule_port(rule) != port:
            continue
        callees.update(peer["podSelector"]["matchLabels"]["app"] for peer in rule["to"])
    return callees


def _actual_ingress_edges(docs: list[dict]) -> set[tuple[str, str, int]]:
    result = set()
    prefix = "allow-internal-calls-to-"
    for doc in docs:
        name = doc["metadata"]["name"]
        if not name.startswith(prefix):
            continue
        callee = name.removeprefix(prefix)
        for rule in doc["spec"]["ingress"]:
            port = _rule_port(rule)
            for peer in rule["from"]:
                result.add((peer["podSelector"]["matchLabels"]["app"], callee, port))
    return result


def _actual_egress_edges(docs: list[dict]) -> set[tuple[str, str, int]]:
    result = set()
    prefix = "allow-internal-calls-from-"
    for doc in docs:
        name = doc["metadata"]["name"]
        if not name.startswith(prefix):
            continue
        caller = name.removeprefix(prefix)
        for rule in doc["spec"]["egress"]:
            port = _rule_port(rule)
            for peer in rule["to"]:
                result.add((caller, peer["podSelector"]["matchLabels"]["app"], port))
    return result


def test_default_deny_baseline_present():
    docs = _load_docs()
    deny = _find(docs, "default-deny-all")
    assert deny is not None
    assert deny["spec"]["podSelector"] == {}
    assert set(deny["spec"]["policyTypes"]) == {"Ingress", "Egress"}


def test_call_graph_matches_code_derived_acceptance_set():
    actual = {(call.caller, call.callee, call.port) for call in CALL_GRAPH}
    assert actual == EXPECTED_DIRECT_CALLS


def test_every_direct_call_has_both_ingress_and_egress_permission():
    docs = _load_docs()
    for caller, callee, port in EXPECTED_DIRECT_CALLS:
        assert caller in _allowed_ingress_callers(docs, callee, port), (
            f"missing ingress half for {caller} -> {callee}:{port}"
        )
        assert callee in _allowed_egress_callees(docs, caller, port), (
            f"missing egress half for {caller} -> {callee}:{port}"
        )


def test_internal_call_policies_grant_no_extra_edges():
    docs = _load_docs()
    assert _actual_ingress_edges(docs) == EXPECTED_DIRECT_CALLS
    assert _actual_egress_edges(docs) == EXPECTED_DIRECT_CALLS


def test_billing_services_have_no_direct_ingress_rule_at_all():
    docs = _load_docs()
    for name in ("billing-service", "billing-ledger-writer", "billing-outbox-publisher"):
        assert _find(docs, f"allow-internal-calls-to-{name}") is None


def test_execution_control_callers_are_exact():
    docs = _load_docs()
    assert _allowed_ingress_callers(docs, "execution-control-service", 9000) == {
        "backoffice-api", "billing-reconciliation",
    }


def test_operator_smpp_session_manager_callers_are_exact():
    docs = _load_docs()
    assert _allowed_ingress_callers(docs, "operator-smpp-session-manager", 9000) == {
        "delivery-service", "delivery-reconciliation-service",
    }


def test_prometheus_scrape_requires_monitoring_namespace_and_prometheus_pod():
    docs = _load_docs()
    policy = _find(docs, "allow-prometheus-scrape-ingress")
    assert policy is not None
    assert policy["spec"]["podSelector"] == {}
    assert policy["spec"]["ingress"] == [{
        "from": [{
            "namespaceSelector": {"matchLabels": {NAMESPACE_NAME_LABEL: MONITORING_NAMESPACE}},
            "podSelector": {"matchLabels": PROMETHEUS_LABELS},
        }],
        "ports": [{"protocol": "TCP", "port": HEALTH_PORT}],
    }]


def test_dns_is_limited_to_coredns_pods_in_kube_system():
    docs = _load_docs()
    policy = _find(docs, "allow-dns-egress")
    peer = policy["spec"]["egress"][0]["to"][0]
    assert peer == {
        "namespaceSelector": {"matchLabels": {NAMESPACE_NAME_LABEL: "kube-system"}},
        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
    }


def test_kafka_egress_is_limited_to_explicit_clients_and_broker_pods():
    docs = _load_docs()
    policy = _find(docs, "allow-kafka-egress")
    expression = policy["spec"]["podSelector"]["matchExpressions"][0]
    assert expression == {"key": "app", "operator": "In", "values": sorted(KAFKA_CLIENTS)}
    rule = policy["spec"]["egress"][0]
    assert rule == {
        "to": [{"podSelector": {"matchLabels": KAFKA_BROKER_LABELS}}],
        "ports": [{"protocol": "TCP", "port": KAFKA_PORT}],
    }
    assert KAFKA_CLIENTS <= {service.name for service in SERVICES}


def test_cross_namespace_egress_uses_namespace_and_pod_intersection():
    docs = _load_docs()
    expected = {
        ("execution-control-service", MONITORING_NAMESPACE, 9090),
        ("credential-issuer-service", VAULT_NAMESPACE, 8200),
        ("partner-rest-receiver", VAULT_NAMESPACE, 8200),
        ("partner-smpp-gateway", VAULT_NAMESPACE, 8200),
    }
    assert {(call.caller, call.namespace, call.port) for call in CROSS_NAMESPACE_CALLS} == expected
    for call in CROSS_NAMESPACE_CALLS:
        policy = _find(docs, f"allow-{call.name}-from-{call.caller}")
        assert policy is not None
        rule = policy["spec"]["egress"][0]
        assert rule["to"] == [{
            "namespaceSelector": {"matchLabels": {NAMESPACE_NAME_LABEL: call.namespace}},
            "podSelector": {"matchLabels": call.pod_labels},
        }]
        assert rule["ports"] == [{"protocol": "TCP", "port": call.port}]


def test_external_ingress_matches_public_service_table_exactly():
    docs = _load_docs()
    expected = {service.name for service in SERVICES if service.external_port}
    actual = {
        doc["metadata"]["name"].removeprefix("allow-external-ingress-")
        for doc in docs
        if doc["metadata"]["name"].startswith("allow-external-ingress-")
    }
    assert actual == expected


if __name__ == "__main__":
    tests = [value for key, value in list(globals().items()) if key.startswith("test_")]
    passed = 0
    for test in tests:
        test()
        passed += 1
        print(f"PASS  {test.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
