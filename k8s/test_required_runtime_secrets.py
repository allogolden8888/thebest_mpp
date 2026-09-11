from pathlib import Path

import yaml

from generate_manifests import SECRET_K8S_NAME


RENDERED = Path(__file__).parent / "rendered"

EXPECTED_SECRET_REFS = {
    "backoffice-api": {"postgresql", "clickhouse", "backoffice-jwt-keypair"},
    "partner-api": {"postgresql", "clickhouse", "partner-oidc-verification"},
    "partner-self-service-api": {"partner-oidc-verification"},
    "billing-self-service-api": {"postgresql", "partner-oidc-verification"},
    "compliance-api": {"redis-runtime", "partner-oidc-verification"},
    # BACKOFFICE_ROADMAP.md P0#1 (2026-09): "operator-webhook-auth" removed —
    # operator-http-gateway now reads per-operator webhook credentials from
    # Vault via Kubernetes auth (no static k8s Secret involved), same as
    # credential-issuer-service carries no Vault entry here either.
    "operator-http-gateway": {"redis-runtime", "redis-configuration"},
}


def _workload_secret_refs(service: str) -> set[str]:
    docs = list(yaml.safe_load_all((RENDERED / f"{service}.yaml").read_text()))
    workload = next(doc for doc in docs if doc["kind"] in {"Deployment", "StatefulSet"})
    container = workload["spec"]["template"]["spec"]["containers"][0]
    return {entry["secretRef"]["name"] for entry in container.get("envFrom", [])}


def test_services_that_fail_without_auth_material_receive_it():
    for service, dependency_keys in EXPECTED_SECRET_REFS.items():
        expected_names = {SECRET_K8S_NAME[key] for key in dependency_keys}
        assert _workload_secret_refs(service) == expected_names
