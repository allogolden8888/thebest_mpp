variable "cloud_id" {
  description = "Yandex Cloud cloud ID — выдаётся при регистрации, секрет окружения (TF_VAR_cloud_id), не хардкодится"
  type        = string
}

variable "folder_id" {
  description = "Yandex Cloud folder ID проекта MPP"
  type        = string
}

variable "zones" {
  description = "Зоны доступности для мультизонального размещения — HA дата-плейна и managed-сервисов"
  type        = list(string)
  default     = ["ru-central1-a", "ru-central1-b", "ru-central1-d"]
}

variable "environment" {
  description = "Имя окружения (staging/production) — префикс имён ресурсов, не влияет на топологию"
  type        = string
  default     = "staging"
}

# Стандартные размеры инстансов — те же, что в k8s/generate_manifests.py
# INSTANCE_PROFILE (capacity_model.md:92): Rust 4 vCPU/8GB, Java-session
# 8 vCPU/16GB, Go 2 vCPU/4GB. Node-пулы размечены под них 1:1, чтобы каждый
# под получал целый узел без соседей другого класса (bin-packing по классам,
# не по факту — sticky/kafka-streams-классы не должны делить узел с
# произвольным stateless-подом).
variable "node_pool_sizes" {
  description = "vCPU/RAM(GB) на узел по классам нагрузки — зеркалирует INSTANCE_PROFILE из k8s/generate_manifests.py"
  type = map(object({
    cpu    = number
    mem_gb = number
  }))
  default = {
    rust-pool   = { cpu = 4, mem_gb = 8 }
    java-pool   = { cpu = 8, mem_gb = 16 }
    go-pool     = { cpu = 2, mem_gb = 4 }
    sticky-pool = { cpu = 8, mem_gb = 16 } # sticky-statefulset — Java(8vCPU) и Go(2vCPU) сервисы вперемешку,
    # берём больший профиль с запасом, а не два отдельных sticky-пула
  }
}

variable "node_pool_min_size" {
  description = "Нижняя граница авто-масштабирования узлов на пул — не 0, чтобы не терять StatefulSet-поды при простое"
  type        = number
  default     = 2
}

variable "node_pool_max_size" {
  description = "Верхняя граница авто-масштабирования узлов на пул"
  type        = number
  default     = 20
}
