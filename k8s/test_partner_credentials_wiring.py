"""
Проверяет исправление HIGH находки кодревью (PART 2, partner-rest-receiver
#4, "auth verifier can never succeed as deployed"): что рендер реально
монтирует partner-конфиг и заводит PARTNER_CRED_* секрет для
partner-rest-receiver, и что генерация env var имени из credential_ref
байт-в-байт совпадает с Rust-версией (services/partner-rest-receiver/src/auth.rs
::credential_ref_to_env_var) — расхождение между Python- и Rust-версией
сделало бы сгенерированный Secret бесполезным (EnvAuthVerifier искал бы
переменную под другим именем), не поймать это тестом формы YAML
(kubeconform не знает про Rust-код).
"""

from pathlib import Path

import yaml

from generate_manifests import (
    PARTNER_CONFIG_CONFIGMAP_NAME,
    PARTNER_CONFIG_FIXTURE,
    PARTNER_CONFIG_MOUNT_DIR,
    PARTNER_CONFIG_SERVICES,
    credential_ref_to_env_var,
)

RENDERED = Path(__file__).parent / "rendered"


def _load_docs(filename: str) -> list[dict]:
    return list(yaml.safe_load_all((RENDERED / filename).read_text()))


def test_credential_ref_to_env_var_matches_rust_test_vector():
    # Тот же вектор, что services/partner-rest-receiver/src/auth.rs::tests::
    # credential_ref_maps_to_deterministic_env_var_name — намеренно
    # продублирован здесь, а не импортирован (разные языки), но ОБЯЗАН
    # оставаться синхронным.
    assert credential_ref_to_env_var("vault://partners/click_uz/main/api_key") == \
        "PARTNER_CRED_VAULT___PARTNERS_CLICK_UZ_MAIN_API_KEY"


def test_partner_config_configmap_rendered_with_real_fixture_content():
    docs = _load_docs("00-partner-config.yaml")
    assert len(docs) == 1
    cm = docs[0]
    assert cm["kind"] == "ConfigMap"
    assert cm["metadata"]["name"] == PARTNER_CONFIG_CONFIGMAP_NAME
    assert PARTNER_CONFIG_FIXTURE in cm["data"]
    assert '"partner_id"' in cm["data"][PARTNER_CONFIG_FIXTURE]


def test_partner_rest_receiver_mounts_config_and_sets_path_env():
    docs = _load_docs("partner-rest-receiver.yaml")
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    pod_spec = deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    volumes = {v["name"]: v for v in pod_spec["volumes"]}
    assert "partner-config" in volumes
    assert volumes["partner-config"]["configMap"]["name"] == PARTNER_CONFIG_CONFIGMAP_NAME

    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts["partner-config"]["mountPath"] == PARTNER_CONFIG_MOUNT_DIR

    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["PARTNER_CONFIG_PATH"] == f"{PARTNER_CONFIG_MOUNT_DIR}/{PARTNER_CONFIG_FIXTURE}"


def test_partner_rest_receiver_gets_partner_credentials_secret():
    docs = _load_docs("partner-rest-receiver.yaml")
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    secret_refs = {ef["secretRef"]["name"] for ef in container["envFrom"]}
    assert "partner-credentials" in secret_refs


def test_partner_notification_service_also_mounts_config_but_not_credentials():
    # partner-notification-service читает тот же файл для callback-роутинга,
    # но не верифицирует входящий partner auth — PARTNER_CRED_* ему не нужен.
    docs = _load_docs("partner-notification-service.yaml")
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in container.get("env", [])}
    assert env.get("PARTNER_CONFIG_PATH") == f"{PARTNER_CONFIG_MOUNT_DIR}/{PARTNER_CONFIG_FIXTURE}"
    secret_refs = {ef["secretRef"]["name"] for ef in container.get("envFrom", [])}
    assert "partner-credentials" not in secret_refs


def test_every_partner_config_consumer_mounts_the_same_config():
    # Эти четыре сервиса вызывают загрузчик PARTNER_CONFIG_PATH при старте или
    # используют его для runtime routing/auth. Пропуск любого из них означает
    # либо crash-loop (billing), либо пустой/неработающий runtime-контур.
    assert set(PARTNER_CONFIG_SERVICES) == {
        "billing-service",
        "partner-notification-service",
        "partner-rest-receiver",
        "partner-smpp-gateway",
    }

    for service in PARTNER_CONFIG_SERVICES:
        docs = _load_docs(f"{service}.yaml")
        workload = next(d for d in docs if d["kind"] in {"Deployment", "StatefulSet"})
        pod_spec = workload["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]

        volumes = {v["name"]: v for v in pod_spec.get("volumes", [])}
        assert volumes["partner-config"]["configMap"]["name"] == PARTNER_CONFIG_CONFIGMAP_NAME

        mounts = {m["name"]: m for m in container.get("volumeMounts", [])}
        assert mounts["partner-config"]["mountPath"] == PARTNER_CONFIG_MOUNT_DIR

        env = {e["name"]: e["value"] for e in container.get("env", [])}
        assert env["PARTNER_CONFIG_PATH"] == f"{PARTNER_CONFIG_MOUNT_DIR}/{PARTNER_CONFIG_FIXTURE}"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
