# Фаза 1.10 (development_plan.md) — KEDA + Prometheus Operator + Grafana.
# Закрывает конкретный, уже существующий разрыв: k8s/generate_manifests.py
# генерирует 16 ScaledObject (kafka_consumer=True stateless-сервисы) с самого
# первого прогона k8s/, но ни один оператор, способный их прочитать, до сих
# пор не был развёрнут — ScaledObject лежали в кластере мёртвым грузом.
#
# Alerting rules и Grafana-дашборды — НЕ здесь. development_plan.md Фаза 6
# ("6.1 конкретные Prometheus alerting rules", "6.2 Grafana-дашборды") идёт
# после нагрузочного тестирования (6.4) — придумывать пороги тревог и
# панели до того, как есть хоть один реальный прогон под нагрузкой, значит
# зафиксировать угаданные, а не откалиброванные числа. Здесь — только то,
# что делает наблюдаемость технически возможной (операторы установлены,
# метрики реально скрейпятся), не сами правила/панели.

resource "kubernetes_namespace" "keda" {
  metadata {
    name = "keda"
  }
}

resource "helm_release" "keda" {
  name       = "keda"
  repository = "https://kedacore.github.io/charts"
  chart      = "keda"
  version    = "2.16.0"
  namespace  = kubernetes_namespace.keda.metadata[0].name
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
  }
}

resource "random_password" "grafana_admin" {
  length  = 24
  special = true
}

resource "kubernetes_secret" "grafana_admin" {
  metadata {
    name      = "grafana-admin-credentials"
    namespace = kubernetes_namespace.monitoring.metadata[0].name
  }
  data = {
    admin-user     = "admin"
    admin-password = random_password.grafana_admin.result
  }
}

# Пароль дублируется в Vault тем же способом, что и остальные credentials
# (vault-secrets.tf) — не для потребления сервисами (Grafana не входит в
# SERVICES/SECRET_DEPENDENCIES, она инфраструктурный компонент, не сервис
# платформы), а чтобы восстановить доступ, если k8s Secret будет потерян
# без пересоздания helm-релиза.
resource "vault_kv_secret_v2" "grafana" {
  mount = vault_mount.mpp.path
  name  = "grafana-admin"
  data_json = jsonencode({
    GRAFANA_ADMIN_USER     = "admin"
    GRAFANA_ADMIN_PASSWORD = random_password.grafana_admin.result
  })
}

resource "helm_release" "kube_prometheus_stack" {
  name       = "kube-prometheus-stack"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "65.5.1"
  namespace  = kubernetes_namespace.monitoring.metadata[0].name

  values = [yamlencode({
    grafana = {
      admin = {
        existingSecret = kubernetes_secret.grafana_admin.metadata[0].name
        userKey        = "admin-user"
        passwordKey    = "admin-password"
      }
    }
    prometheus = {
      prometheusSpec = {
        # Явно проставленный label, а не предположение о внутреннем дефолте чарта —
        # k8s/network_policies.py allow-prometheus-scrape-ingress уже написан и
        # протестирован против podSelector {app: prometheus} (см. PROMETHEUS_LABEL),
        # поэтому лейбл подгоняется под уже существующую NetworkPolicy, а не наоборот.
        podMetadata = {
          labels = {
            app = "prometheus"
          }
        }
        # PodMonitor/ServiceMonitor из любого namespace — сервисы MPP не размечены
        # своим Prometheus Operator release-лейблом (namespace-релиз для приложений
        # не тот же, что у мониторинга), ограничивать selector'ом смысла нет.
        podMonitorSelectorNilUsesHelmValues     = false
        serviceMonitorSelectorNilUsesHelmValues = false
      }
    }
  })]

  depends_on = [kubernetes_secret.grafana_admin]
}
