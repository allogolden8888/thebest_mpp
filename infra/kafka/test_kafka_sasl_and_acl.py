"""
BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя HLD
требует ACL": regression tests for the dual-listener SASL_SCRAM migration
(generate_kafka_topics.py::build_kafka_cluster_crd) and the per-service ACL
generator (generate_kafka_users.py), which reuses
k8s/generate_manifests.py::KEDA_KAFKA_TRIGGERS as the single source of truth
for "who needs access to what" (same mapping already used for KEDA
ScaledObject triggers).
"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "k8s"))

from generate_kafka_topics import build_kafka_cluster_crd  # noqa: E402
from generate_kafka_users import build_kafka_user_crd  # noqa: E402
from generate_manifests import (  # noqa: E402
    KAFKA_SASL_DEMO_SERVICES,
    KEDA_KAFKA_TRIGGERS,
    SERVICES,
    kafka_user_name,
    render_service,
)


def test_sasl_listener_is_added_next_to_unchanged_plaintext_and_tls_listeners():
    """Additive dual-listener migration: the original plain/tls listeners must
    survive byte-for-byte (~37 existing plaintext clients keep working
    unmodified), the SASL_SCRAM+TLS listener is a genuine addition, not a
    replacement."""
    listeners = build_kafka_cluster_crd()["spec"]["kafka"]["listeners"]
    by_name = {listener["name"]: listener for listener in listeners}

    assert by_name["plain"] == {"name": "plain", "port": 9092, "type": "internal", "tls": False}
    assert by_name["tls"] == {"name": "tls", "port": 9093, "type": "internal", "tls": True}

    sasl = by_name["sasl"]
    assert sasl["port"] == 9094
    assert sasl["type"] == "internal"
    assert sasl["tls"] is True, "SASL_SCRAM без TLS не защищает канал от активного MITM внутри mesh"
    assert sasl["authentication"] == {"type": "scram-sha-512"}


def test_cluster_authorization_is_enabled_but_keeps_anonymous_as_superuser():
    """Turning on `authorization: simple` cluster-wide without keeping
    User:ANONYMOUS as a superuser would instantly deny every one of the ~37
    still-plaintext services (unauthenticated connections map to
    User:ANONYMOUS) — this is what makes the rollout additive rather than a
    breaking cutover in this same commit."""
    kafka_spec = build_kafka_cluster_crd()["spec"]["kafka"]
    assert kafka_spec["authorization"]["type"] == "simple"
    assert "User:ANONYMOUS" in kafka_spec["authorization"]["superUsers"]


def test_kafka_user_generated_for_every_keda_mapped_service():
    expected_names = {kafka_user_name(service) for service in KEDA_KAFKA_TRIGGERS}
    actual_names = {
        build_kafka_user_crd(service, triggers)["metadata"]["name"]
        for service, triggers in KEDA_KAFKA_TRIGGERS.items()
    }
    assert actual_names == expected_names


def test_kafka_user_acls_grant_exactly_the_keda_mapped_topics_and_groups_read_only():
    """The ACL generator must not hand-maintain a second list: every topic/group
    Read ACL on a KafkaUser has to trace back to an entry in
    KEDA_KAFKA_TRIGGERS for that same service, and nothing beyond Read (no
    accidental Write/All grants) is produced by this generator."""
    for service, triggers in KEDA_KAFKA_TRIGGERS.items():
        user = build_kafka_user_crd(service, triggers)
        assert user["spec"]["authentication"] == {"type": "scram-sha-512"}
        acls = user["spec"]["authorization"]["acls"]

        expected_topics = {topic for topic, _ in triggers}
        expected_groups = {group for _, group in triggers}
        actual_topics = {a["resource"]["name"] for a in acls if a["resource"]["type"] == "topic"}
        actual_groups = {a["resource"]["name"] for a in acls if a["resource"]["type"] == "group"}

        assert actual_topics == expected_topics, service
        assert actual_groups == expected_groups, service
        assert all(a["operations"] == ["Read"] for a in acls), (
            f"{service}: ACL generator scope is consumer-only (Read) — see module docstring "
            "for why producer-side Write ACLs are explicitly out of scope"
        )
        assert all(a["resource"]["patternType"] == "literal" for a in acls)


def test_sasl_demo_services_are_a_small_named_subset_of_keda_mapped_services():
    """Guards against silent scope creep in either direction: the pilot must
    stay a small, explicit, cross-language sample (not grow into "everyone",
    which would misrepresent the rollout as complete), and every demo
    service must actually have a matching KafkaUser/ACL generated above."""
    assert 1 <= len(KAFKA_SASL_DEMO_SERVICES) <= 5
    assert set(KAFKA_SASL_DEMO_SERVICES) <= set(KEDA_KAFKA_TRIGGERS)


def test_sasl_demo_service_pods_reference_the_matching_kafka_user_secret():
    """Cross-file wiring check: the container env's KAFKA_SASL_USERNAME and the
    KAFKA_SASL_PASSWORD secretKeyRef in k8s/generate_manifests.py must point at
    the exact same KafkaUser-managed secret name that generate_kafka_users.py
    actually creates for that service — a naming drift here would mean the
    pilot pods reference a Secret Strimzi never provisions."""
    demo_services = [svc for svc in SERVICES if svc.name in KAFKA_SASL_DEMO_SERVICES]
    assert {svc.name for svc in demo_services} == set(KAFKA_SASL_DEMO_SERVICES)

    for svc in demo_services:
        expected_user_secret = kafka_user_name(svc.name)
        generated_user = build_kafka_user_crd(svc.name, KEDA_KAFKA_TRIGGERS[svc.name])
        assert generated_user["metadata"]["name"] == expected_user_secret

        docs = render_service(svc)
        workload = next(d for d in docs if d["kind"] in ("Deployment", "StatefulSet"))
        container = workload["spec"]["template"]["spec"]["containers"][0]
        env_by_name = {item["name"]: item for item in container["env"]}

        assert env_by_name["KAFKA_SASL_USERNAME"]["value"] == expected_user_secret
        password_ref = env_by_name["KAFKA_SASL_PASSWORD"]["valueFrom"]["secretKeyRef"]
        assert password_ref["name"] == expected_user_secret
        assert password_ref["key"] == "password"

        volumes = workload["spec"]["template"]["spec"]["volumes"]
        ca_volume = next(v for v in volumes if v["name"] == "kafka-tls-ca")
        assert ca_volume["secret"]["secretName"] == "mpp-kafka-cluster-ca-cert"
