"""
Secrets — Vault (self-hosted, infra/terraform/vault.tf) + External Secrets
Operator. Решение выбрано, не задано пользователем явно (в отличие от
провайдера/Kafka/mTLS) — обоснование в infra/README.md: Vault не привязан к
конкретному облаку (тот же принцип портируемости, что уже применён к Kafka
self-hosted через Strimzi), ESO поддерживает Vault "из коробки" одним из
первых провайдеров, в отличие от нишевых cloud-specific secret-менеджеров.

Пять секретов — ровно пять хранилищ, к которым обращаются сервисы
(k8s/generate_manifests.py SECRET_K8S_NAME — единственный источник имён,
импортируется отсюда, не дублируется). Каждому ExternalSecret соответствует
Vault KV v2 путь `mpp/data/<key>`, наполняемый Terraform'ом
(infra/terraform/vault-secrets.tf) теми же значениями, что реально были
заданы managed-ресурсам (postgresql.tf/redis.tf/clickhouse.tf) — не
переизобретёнными вручную, иначе они бы разошлись при первой ротации.
"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "k8s"))
from generate_manifests import (  # noqa: E402
    NAMESPACE,
    SECRET_K8S_NAME,
    credential_ref_to_env_var,
    load_partner_fixture,
)

VAULT_ADDR = "http://vault.vault-system.svc:8200"
VAULT_KV_MOUNT = "mpp"

# Ключи внутри каждого k8s Secret — переменные окружения, которые
# envFrom/secretRef подставит в контейнер сервиса напрямую (без ConfigMap —
# host/port тоже секретны в managed-окружении, DSN не собирается на лету).
SECRET_KEYS = {
    "postgresql-credentials": ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"],
    "redis-runtime-credentials": ["REDIS_RUNTIME_HOST", "REDIS_RUNTIME_PORT", "REDIS_RUNTIME_PASSWORD"],
    "redis-configuration-credentials": ["REDIS_CONFIGURATION_HOST", "REDIS_CONFIGURATION_PORT", "REDIS_CONFIGURATION_PASSWORD"],
    "redis-billing-credentials": ["REDIS_BILLING_HOST", "REDIS_BILLING_PORT", "REDIS_BILLING_PASSWORD"],
    "clickhouse-credentials": ["CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DB", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"],
}

# k8s Secret name -> Vault KV v2 путь (без /data/ префикса — ESO сам его подставляет для kv-v2).
VAULT_PATH = {
    "postgresql-credentials": "postgresql",
    "redis-runtime-credentials": "redis-runtime",
    "redis-configuration-credentials": "redis-configuration",
    "redis-billing-credentials": "redis-billing",
    "clickhouse-credentials": "clickhouse",
}


def build_cluster_secret_store() -> dict:
    return {
        "apiVersion": "external-secrets.io/v1beta1",
        "kind": "ClusterSecretStore",
        "metadata": {"name": "vault-backend"},
        "spec": {
            "provider": {
                "vault": {
                    "server": VAULT_ADDR,
                    "path": VAULT_KV_MOUNT,
                    "version": "v2",
                    "auth": {
                        # Kubernetes auth method — ESO аутентифицируется в Vault собственным
                        # ServiceAccount-токеном, не статичным секретом (нечего ротировать/утекать
                        # на стороне ESO). Vault-сторона (роль/policy) — infra/terraform/vault.tf.
                        "kubernetes": {
                            "mountPath": "kubernetes",
                            "role": "external-secrets",
                            "serviceAccountRef": {"name": "external-secrets", "namespace": "external-secrets"},
                        },
                    },
                },
            },
        },
    }


def build_external_secret(k8s_secret_name: str) -> dict:
    vault_path = VAULT_PATH[k8s_secret_name]
    keys = SECRET_KEYS[k8s_secret_name]
    return {
        "apiVersion": "external-secrets.io/v1beta1",
        "kind": "ExternalSecret",
        "metadata": {"name": k8s_secret_name, "namespace": NAMESPACE},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {"name": "vault-backend", "kind": "ClusterSecretStore"},
            "target": {"name": k8s_secret_name, "creationPolicy": "Owner"},
            "data": [
                {"secretKey": key, "remoteRef": {"key": vault_path, "property": key}}
                for key in keys
            ],
        },
    }


def vault_kv_path_and_property(credential_ref: str) -> tuple[str, str]:
    """`vault://partners/click_uz/main/api_key` -> (`partners/click_uz/main`, `api_key`)
    — группирует по application (один Vault KV v2 entry на app), не по
    отдельной записи на credential, тот же принцип, что у пяти статических
    секретов (несколько полей под одним KV entry)."""
    assert credential_ref.startswith("vault://"), f"ожидали vault:// credential_ref, получили {credential_ref!r}"
    path = credential_ref[len("vault://"):]
    kv_path, _, property_name = path.rpartition("/")
    return kv_path, property_name


def build_partner_credentials_external_secret() -> dict:
    """HIGH находка кодревью (PART 2, partner-rest-receiver #4) —
    `PARTNER_CRED_*` не имело ни одной Vault-записи нигде в infra/. В отличие
    от build_external_secret (один Vault-путь, N полей), здесь у каждого
    credential_ref — СВОЙ отдельный Vault KV entry (`vault_kv_path_and_property`),
    поэтому форма ExternalSecret.spec.data — по одной записи на приложение,
    не общий remoteRef.key. Источник партнёров — тот же статический
    partner.valid.json, что реально монтируется в под через ConfigMap
    (k8s/generate_manifests.py PARTNER_CONFIG_SERVICES) — тот же явно
    раскрытый класс упрощения (в проде — динамически из config.changes),
    не новый источник рассинхронизации."""
    partner = load_partner_fixture()
    data = []
    for app in partner["applications"]:
        credential_ref = app["auth"]["credential_ref"]
        env_var = credential_ref_to_env_var(credential_ref)
        kv_path, property_name = vault_kv_path_and_property(credential_ref)
        data.append({"secretKey": env_var, "remoteRef": {"key": kv_path, "property": property_name}})
    return {
        "apiVersion": "external-secrets.io/v1beta1",
        "kind": "ExternalSecret",
        "metadata": {"name": "partner-credentials", "namespace": NAMESPACE},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {"name": "vault-backend", "kind": "ClusterSecretStore"},
            "target": {"name": "partner-credentials", "creationPolicy": "Owner"},
            "data": data,
        },
    }


def main():
    out_dir = Path(__file__).parent / "rendered"
    out_dir.mkdir(exist_ok=True)
    for f in out_dir.glob("*.yaml"):
        f.unlink()

    docs = [build_cluster_secret_store()]
    for k8s_secret_name in SECRET_K8S_NAME.values():
        if k8s_secret_name == "partner-credentials":
            docs.append(build_partner_credentials_external_secret())
            continue
        docs.append(build_external_secret(k8s_secret_name))

    path = out_dir / "external-secrets.yaml"
    path.write_text(yaml.dump_all(docs, sort_keys=False))
    print(f"1 ClusterSecretStore + {len(SECRET_K8S_NAME)} ExternalSecret -> {path}")


if __name__ == "__main__":
    main()
