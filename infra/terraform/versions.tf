# Провайдер — Yandex Cloud, конкретная реализация выбора "региональный облачный
# провайдер" (обсуждение: Yandex Cloud / Beeline Cloud UZ и т.п.). Yandex Cloud
# взят как конкретика ради валидируемого Terraform — единственный из
# упомянутых с официальным, поддерживаемым Terraform-провайдером и
# присутствием в регионе СНГ/Центральной Азии. Если реальный выбор — другой
# OpenStack-совместимый провайдер, меняются только provider-блок и
# resource-типы в *.tf, структура (VPC -> managed k8s node pools per
# workload class -> managed PostgreSQL/Redis x3/ClickHouse -> registry)
# сохраняется.

terraform {
  required_version = ">= 1.9.0"

  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "~> 0.218.0" # 3-компонентный pessimistic constraint — для provider major=0 двухкомпонентный
      # "~> 0.218" фиксирует только major (=0) и пропускает любой minor, что не пин
      # вообще; проверено `terraform init` — реальная последняя версия на момент
      # написания 0.218.0
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.15"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.33"
    }
    vault = {
      source  = "hashicorp/vault"
      version = "~> 4.5"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}
