"""
Проверяет не форму (это делает kubeconform), а СЕМАНТИКУ сгенерированных
NetworkPolicy: кто на самом деле может достучаться до кого. kubeconform
подтверждает, что YAML — валидный NetworkPolicy; он не знает и не может
знать, что "Billing не должен быть достижим напрямую" — это утверждение из
архитектуры, не из schema. Здесь оно проверяется явно, на самих
сгенерированных манифестах (не на исходном CALL_GRAPH — если генератор
собрал не то, что в CALL_GRAPH, тест должен это поймать, а не подтвердить
собственный баг).
"""

from pathlib import Path

import yaml

from generate_manifests import SERVICES
from network_policies import CALL_GRAPH

RENDERED = Path(__file__).parent / "rendered" / "01-network-policies.yaml"


def _load_docs() -> list[dict]:
    return list(yaml.safe_load_all(RENDERED.read_text()))


def _find(docs: list[dict], name: str) -> dict | None:
    return next((d for d in docs if d["metadata"]["name"] == name), None)


def _allowed_grpc_callers(docs: list[dict], callee_name: str) -> set[str]:
    doc = _find(docs, f"allow-internal-grpc-{callee_name}")
    if doc is None:
        return set()
    froms = doc["spec"]["ingress"][0]["from"]
    return {f["podSelector"]["matchLabels"]["app"] for f in froms}


def test_default_deny_baseline_present():
    docs = _load_docs()
    deny = _find(docs, "default-deny-all")
    assert deny is not None
    assert deny["spec"]["podSelector"] == {}
    assert set(deny["spec"]["policyTypes"]) == {"Ingress", "Egress"}


def test_every_call_graph_edge_is_actually_permitted():
    docs = _load_docs()
    for caller, callee in CALL_GRAPH:
        allowed = _allowed_grpc_callers(docs, callee)
        assert caller in allowed, f"{caller} -> {callee} объявлен в CALL_GRAPH, но не разрешён сгенерированной policy"


def test_billing_services_have_no_direct_ingress_rule_at_all():
    """Изоляция Billing — не отдельный механизм, а следствие того, что
    никто не вызывает эти сервисы напрямую (только через Kafka, покрытую
    общим правилом). Если бы генератор ошибочно создал для них
    allow-internal-grpc-*, это был бы дефект изоляции — тест обязан его
    поймать."""
    docs = _load_docs()
    for name in ("billing-service", "billing-ledger-writer", "billing-outbox-publisher"):
        assert _find(docs, f"allow-internal-grpc-{name}") is None, (
            f"{name} не должен иметь прямого ingress-правила — по документам к нему никто не обращается "
            f"напрямую, только через Kafka"
        )


def test_non_authorized_caller_cannot_reach_execution_control():
    """execution-control-service ДЕЙСТВИТЕЛЬНО имеет ingress-правило (от
    backoffice-api и billing-reconciliation) — проверяем, что оно не шире,
    чем нужно: произвольный третий сервис не должен туда попадать."""
    docs = _load_docs()
    allowed = _allowed_grpc_callers(docs, "execution-control-service")
    assert allowed == {"backoffice-api", "billing-reconciliation"}
    assert "policy-service" not in allowed
    assert "partner-notification-service" not in allowed


def test_external_ingress_matches_service_table_exactly():
    docs = _load_docs()
    expected = {s.name for s in SERVICES if s.external_port}
    actual = {
        d["metadata"]["name"].removeprefix("allow-external-ingress-")
        for d in docs
        if d["metadata"]["name"].startswith("allow-external-ingress-")
    }
    assert actual == expected, f"расхождение: только в expected={expected - actual}, только в actual={actual - expected}"


def test_operator_smpp_session_manager_reachable_only_by_delivery_services():
    docs = _load_docs()
    allowed = _allowed_grpc_callers(docs, "operator-smpp-session-manager")
    assert allowed == {"delivery-service", "delivery-reconciliation-service"}


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
