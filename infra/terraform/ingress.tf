# Фаза 1.9 (development_plan.md) — последний пункт Фазы 1. Обслуживает 4
# внешних HTTP ClusterIP-сервиса, для которых k8s/generate_manifests.py
# build_ingress() уже генерирует Ingress-объекты (Partner REST Receiver,
# Partner API, Backoffice API, Backoffice UI). Partner SMPP Gateway (raw TCP)
# и Operator HTTP Gateway (sticky — свой LoadBalancer, route ownership) через
# Ingress не идут — см. build_ingress() docstring и infra/README.md.

resource "kubernetes_namespace" "ingress_nginx" {
  metadata {
    name = "ingress-nginx"
  }
}

resource "helm_release" "ingress_nginx" {
  name       = "ingress-nginx"
  repository = "https://kubernetes.github.io/ingress-nginx"
  chart      = "ingress-nginx"
  version    = "4.11.3"
  namespace  = kubernetes_namespace.ingress_nginx.metadata[0].name

  values = [yamlencode({
    controller = {
      ingressClassResource = {
        name    = "nginx" # k8s/generate_manifests.py INGRESS_CLASS
        default = true
      }
      service = {
        type = "LoadBalancer"
      }
    }
  })]
}

resource "kubernetes_namespace" "cert_manager" {
  metadata {
    name = "cert-manager"
  }
}

resource "helm_release" "cert_manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  version    = "1.16.1"
  namespace  = kubernetes_namespace.cert_manager.metadata[0].name

  set {
    name  = "crds.enabled"
    value = "true"
  }
}

# ClusterIssuer (letsencrypt-http01, k8s/generate_manifests.py CLUSTER_ISSUER)
# — не Terraform-ресурс здесь, а infra/ingress/cluster-issuer.yaml: cert-manager
# CRD должен реально существовать в API-сервере до apply, тот же
# chicken-and-egg, что уже решён для PeerAuthentication (требует istiod) и
# PodMonitor (требует Prometheus Operator) — применяется отдельным шагом
# после helm_release, не через terraform kubernetes_manifest.
