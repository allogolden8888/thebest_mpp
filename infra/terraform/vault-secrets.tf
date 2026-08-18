# Замыкает контур: managed-ресурсы (postgresql.tf/redis.tf/clickhouse.tf)
# создаются с паролями из переменных, ЭТИ ЖЕ значения пишутся в Vault по
# путям, которые читает infra/secrets/generate_external_secrets.py — поды
# получают реальный, а не заново придуманный пароль. Без этого файла
# ExternalSecret указывали бы на пустой Vault path.
#
# Требует, чтобы Vault был инициализирован и разблокирован (см. vault.tf —
# это НЕ Terraform-управляемый шаг для standalone-режима), иначе provider
# "vault" не сможет аутентифицироваться — терраформ здесь предполагает
# root/admin token окружения (TF_VAR_vault_token), не создаёт Vault с нуля.

provider "vault" {
  address = "http://127.0.0.1:8200" # port-forward к vault.vault-system.svc:8200 — Vault не имеет
  # внешнего LoadBalancer (см. vault.tf), CI/оператор пробрасывает порт
  token = var.vault_token
}

variable "vault_token" {
  description = "Vault root/admin токен для bootstrap — секрет окружения (TF_VAR_vault_token), никогда не в git"
  type        = string
  sensitive   = true
}

resource "vault_mount" "mpp" {
  path = "mpp"
  type = "kv-v2"
}

resource "vault_auth_backend" "kubernetes" {
  type = "kubernetes"
}

resource "vault_kubernetes_auth_backend_config" "mpp" {
  backend            = vault_auth_backend.kubernetes.path
  kubernetes_host    = "https://${yandex_kubernetes_cluster.mpp.master[0].external_v4_endpoint}"
  kubernetes_ca_cert = yandex_kubernetes_cluster.mpp.master[0].cluster_ca_certificate
}

resource "vault_policy" "read_mpp_secrets" {
  name   = "mpp-read-secrets"
  policy = <<-EOT
    path "mpp/data/*" {
      capabilities = ["read"]
    }
  EOT
}

resource "vault_kubernetes_auth_backend_role" "external_secrets" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "external-secrets" # совпадает с role в infra/secrets ClusterSecretStore
  bound_service_account_names      = ["external-secrets"]
  bound_service_account_namespaces = ["external-secrets"]
  token_policies                   = [vault_policy.read_mpp_secrets.name]
  token_ttl                        = 900
}

# luminous-hugging-charm.md Фаза 1 — живой выпуск/ротация partner
# credentials (services/credential-issuer-service). read_mpp_secrets выше
# (mpp/data/* read-only, роль external-secrets) слишком широк для writer'а
# и не даёт write вообще — новые узкие policy/role, scoped на
# mpp/data/partners/* (не на весь mpp/data/*, где живут
# postgresql/redis/clickhouse — credential-issuer-service не должен мочь
# даже ПРОЧИТАТЬ платформенные секреты, не то что писать в них).
resource "vault_policy" "write_partner_credentials" {
  name   = "mpp-write-partner-credentials"
  policy = <<-EOT
    path "mpp/data/partners/*" {
      capabilities = ["create", "update", "read"]
    }
  EOT
}

resource "vault_kubernetes_auth_backend_role" "credential_issuer_service" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "credential-issuer-service" # совпадает с VAULT_K8S_AUTH_ROLE default в cmd/credential-issuer-service/main.go
  bound_service_account_names      = ["credential-issuer-service"]
  bound_service_account_namespaces = ["mpp"]
  token_policies                   = [vault_policy.write_partner_credentials.name]
  token_ttl                        = 900
}

# Читающая сторона той же Фазы 1: partner-rest-receiver (VaultAuthVerifier)
# и partner-smpp-gateway (аналог для SMPP_BIND) — только read, только
# partners/*, один shared role/policy для обоих (идентичная потребность,
# та же экономия, что один read_mpp_secrets role для ESO вместо пяти).
resource "vault_policy" "read_partner_credentials" {
  name   = "mpp-read-partner-credentials"
  policy = <<-EOT
    path "mpp/data/partners/*" {
      capabilities = ["read"]
    }
  EOT
}

resource "vault_kubernetes_auth_backend_role" "partner_credential_readers" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "partner-credential-readers"
  bound_service_account_names      = ["partner-rest-receiver", "partner-smpp-gateway"]
  bound_service_account_namespaces = ["mpp"]
  token_policies                   = [vault_policy.read_partner_credentials.name]
  token_ttl                        = 900
}

resource "vault_kv_secret_v2" "postgresql" {
  mount = vault_mount.mpp.path
  name  = "postgresql"
  data_json = jsonencode({
    POSTGRES_HOST     = yandex_mdb_postgresql_cluster.mpp.host[0].fqdn
    POSTGRES_PORT     = "6432" # Yandex managed PostgreSQL — порт пулера соединений (pgbouncer), не 5432 напрямую
    POSTGRES_DB       = yandex_mdb_postgresql_database.mpp.name
    POSTGRES_USER     = "mpp_app"
    POSTGRES_PASSWORD = var.postgresql_app_password
  })
}

resource "vault_kv_secret_v2" "redis" {
  for_each = local.redis_clusters
  mount    = vault_mount.mpp.path
  name     = "redis-${each.key}"
  data_json = jsonencode({
    "REDIS_${upper(replace(each.key, "-", "_"))}_HOST"     = yandex_mdb_redis_cluster.mpp[each.key].host[0].fqdn
    "REDIS_${upper(replace(each.key, "-", "_"))}_PORT"     = "6379"
    "REDIS_${upper(replace(each.key, "-", "_"))}_PASSWORD" = var.redis_passwords[each.key]
  })
}

resource "vault_kv_secret_v2" "clickhouse" {
  mount = vault_mount.mpp.path
  name  = "clickhouse"
  data_json = jsonencode({
    CLICKHOUSE_HOST     = values(yandex_mdb_clickhouse_cluster_v2.mpp.hosts)[0].fqdn
    CLICKHOUSE_PORT     = "9440" # native protocol + TLS, managed ClickHouse по умолчанию
    CLICKHOUSE_DB       = yandex_mdb_clickhouse_database.mpp_analytics.name
    CLICKHOUSE_USER     = "admin"
    CLICKHOUSE_PASSWORD = var.clickhouse_password
  })
}
