# Managed Kubernetes control plane. Kafka НЕ здесь — Kafka self-hosted через
# Strimzi-оператор поверх этого кластера (infra/kafka/), не managed-сервис,
# по решению из чата (снижает vendor lock-in, соответствует "региональный
# провайдер, не только Yandex" — Strimzi портируем на любой k8s).

resource "yandex_kubernetes_cluster" "mpp" {
  name       = "mpp-${var.environment}"
  network_id = yandex_vpc_network.mpp.id

  master {
    version = "1.34"
    regional {
      region = "ru-central1"
      dynamic "location" {
        for_each = var.zones
        content {
          zone      = location.value
          subnet_id = yandex_vpc_subnet.mpp[location.value].id
        }
      }
    }
    public_ip = true
  }

  service_account_id      = yandex_iam_service_account.k8s_cluster.id
  node_service_account_id = yandex_iam_service_account.k8s_node.id

  release_channel = "STABLE"
}

resource "yandex_iam_service_account" "k8s_cluster" {
  name = "mpp-${var.environment}-k8s-cluster"
}

resource "yandex_iam_service_account" "k8s_node" {
  name = "mpp-${var.environment}-k8s-node"
}

# Один node group на класс нагрузки из k8s/generate_manifests.py
# (WorkloadClass: stateless -> любой из rust/java/go-pool по языку сервиса,
# sticky-statefulset и kafka-streams-statefulset -> sticky-pool/java-pool
# соответственно). Раздельные пулы = отдельные taints, чтобы
# планировщик k8s не подсаживал сервисы разных классов на один узел
# (особенно важно для sticky-pool — шумный сосед не должен делить узел с
# подом, держащим живые SMPP bind-сессии).
resource "yandex_kubernetes_node_group" "pool" {
  for_each = var.node_pool_sizes

  cluster_id = yandex_kubernetes_cluster.mpp.id
  name       = "mpp-${var.environment}-${each.key}"

  instance_template {
    platform_id = "standard-v3"

    resources {
      cores  = each.value.cpu
      memory = each.value.mem_gb
    }

    boot_disk {
      type = each.key == "sticky-pool" ? "network-ssd-nonreplicated" : "network-ssd"
      size = each.key == "sticky-pool" ? 100 : 50 # sticky-pool держит kafka-streams-statefulset RocksDB PVC при co-location
    }

    network_interface {
      subnet_ids         = [for s in yandex_vpc_subnet.mpp : s.id]
      security_group_ids = [yandex_vpc_security_group.mpp_internal.id]
    }
  }

  scale_policy {
    auto_scale {
      min     = var.node_pool_min_size
      max     = var.node_pool_max_size
      initial = var.node_pool_min_size
    }
  }

  allocation_policy {
    dynamic "location" {
      for_each = var.zones
      content {
        zone = location.value
      }
    }
  }

  node_labels = {
    "mpp.io/workload-class" = each.key
  }

  node_taints = ["mpp.io/workload-class=${each.key}:NoSchedule"]
}
