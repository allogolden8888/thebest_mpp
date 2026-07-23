# Secrets — Vault (self-hosted) + External Secrets Operator. Решение и
# обоснование — infra/README.md "Secrets" (портируемость, тот же принцип,
# что уже применён к Kafka self-hosted через Strimzi вместо managed-сервиса).
#
# ВАЖНОЕ ОГРАНИЧЕНИЕ, не спрятанное: standalone-режим с file-storage — не
# production HA. Реальный прод нуждается в Raft-кластере (3+ реплики) и
# auto-unseal (KMS), что требует Terraform-модуля vault-helm с другим
# набором values и отдельного bootstrap-потока (см. "Известный технический
# долг" в infra/README.md) — здесь стоит намеренно упрощённая версия ради
# LLD, не готовый прод.

resource "kubernetes_namespace" "vault_system" {
  metadata {
    name = "vault-system"
  }
}

resource "helm_release" "vault" {
  name       = "vault"
  repository = "https://helm.releases.hashicorp.com"
  chart      = "vault"
  version    = "0.28.1"
  namespace  = kubernetes_namespace.vault_system.metadata[0].name

  values = [yamlencode({
    server = {
      standalone = {
        enabled = true
        config  = <<-EOT
          listener "tcp" {
            address     = "0.0.0.0:8200"
            tls_disable = true # TLS терминируется Istio (PeerAuthentication STRICT, infra/istio/) —
                                 # см. infra/README.md о разделении слоёв mTLS/NetworkPolicy
          }
          storage "file" {
            path = "/vault/data"
          }
        EOT
      }
      dataStorage = {
        enabled = true
        size    = "10Gi"
      }
    }
  })]
}

resource "kubernetes_namespace" "external_secrets" {
  metadata {
    name = "external-secrets"
  }
}

resource "helm_release" "external_secrets" {
  name       = "external-secrets"
  repository = "https://charts.external-secrets.io"
  chart      = "external-secrets"
  version    = "0.10.4"
  namespace  = kubernetes_namespace.external_secrets.metadata[0].name

  depends_on = [helm_release.vault]
}

# Vault init/unseal и включение kubernetes auth method + kv-v2 mount "mpp" —
# однократные операционные шаги ПОСЛЕ первого helm_release (Vault standalone
# стартует sealed, Terraform не может писать в него, пока кто-то физически
# не инициализировал/не разблокировал под) — намеренно НЕ terraform-ресурс,
# задокументировано как ручной/скриптовый bootstrap-шаг в infra/README.md,
# а не спрятано за "terraform apply само разберётся".
