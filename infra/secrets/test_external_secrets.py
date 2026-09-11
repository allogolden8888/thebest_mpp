from generate_external_secrets import (
    SECRET_KEYS,
    VAULT_PATH,
    build_external_secret,
)


EXPECTED_RUNTIME_SECRETS = {
    "backoffice-jwt-keypair": {
        "path": "backoffice-jwt",
        # JWT_PREVIOUS_PUBLIC_KEYS_PEM — JWKS/kid rotation grace window
        # (infra/terraform/vault-secrets.tf, backoffice-api/internal/auth/keys.go).
        "keys": {"JWT_PUBLIC_KEY_PEM", "JWT_PRIVATE_KEY_PEM", "JWT_PREVIOUS_PUBLIC_KEYS_PEM"},
    },
    "partner-oidc-verification": {
        "path": "partner-oidc-verification",
        "keys": {"JWT_PUBLIC_KEY_PEM"},
    },
    # "operator-webhook-auth" removed — BACKOFFICE_ROADMAP.md P0#1 (2026-09):
    # see generate_external_secrets.py SECRET_KEYS comment.
}


def test_required_runtime_secrets_have_exact_vault_contracts():
    for name, expected in EXPECTED_RUNTIME_SECRETS.items():
        assert set(SECRET_KEYS[name]) == expected["keys"]
        assert VAULT_PATH[name] == expected["path"]

        external_secret = build_external_secret(name)
        assert external_secret["metadata"]["name"] == name
        data = external_secret["spec"]["data"]
        assert {item["secretKey"] for item in data} == expected["keys"]
        assert {item["remoteRef"]["key"] for item in data} == {expected["path"]}
        assert all(item["remoteRef"]["property"] == item["secretKey"] for item in data)
