# mTLS — решение принято: service mesh (Istio), не app-level TLS (закрывает
# пункт "Решение по mTLS" из k8s/README.md / development_plan.md 1.8).
# PeerAuthentication STRICT (infra/istio/peer-authentication-strict.yaml)
# требует, чтобы istiod уже стоял в кластере — отсюда два helm_release здесь.

provider "kubernetes" {
  host                   = "https://${yandex_kubernetes_cluster.mpp.master[0].external_v4_endpoint}"
  cluster_ca_certificate = yandex_kubernetes_cluster.mpp.master[0].cluster_ca_certificate
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "yc"
    args        = ["k8s", "create-token"]
  }
}

provider "helm" {
  kubernetes {
    host                   = "https://${yandex_kubernetes_cluster.mpp.master[0].external_v4_endpoint}"
    cluster_ca_certificate = yandex_kubernetes_cluster.mpp.master[0].cluster_ca_certificate
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "yc"
      args        = ["k8s", "create-token"]
    }
  }
}

resource "kubernetes_namespace" "istio_system" {
  metadata {
    name = "istio-system"
  }
}

resource "helm_release" "istio_base" {
  name       = "istio-base"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "base"
  version    = "1.24.1"
  namespace  = kubernetes_namespace.istio_system.metadata[0].name
}

resource "helm_release" "istiod" {
  name       = "istiod"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "istiod"
  version    = "1.24.1"
  namespace  = kubernetes_namespace.istio_system.metadata[0].name

  depends_on = [helm_release.istio_base]
}

# Namespace mpp сам создаётся не здесь (k8s/generate_manifests.py уже
# генерирует 00-namespace.yaml с меткой istio-injection=enabled — см.
# infra/README.md), Terraform только ставит control plane.
