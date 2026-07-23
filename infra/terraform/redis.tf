# Три физически изолированных Redis-кластера — архитектурное решение
# зафиксировано с самого начала (hld.md: Runtime / Configuration / Billing
# Redis раздельны), здесь — реальное развёртывание этого решения, не
# пересмотр. Разные persistence-режимы отражают разную роль каждого:
#   Runtime      — CAS execution state, deadlines, rate limits, session/route
#                  registries. Восстановим из Kafka replay при полной потере
#                  (кроме subscriber_consent — см. ниже) -> persistence не
#                  критичен для корректности, важна скорость.
#   Configuration — bootstrap-кэш из config.changes, полностью пересобираем
#                  из Kafka в любой момент -> без AOF, самый дешёвый профиль.
#   Billing      — финансовое состояние, AOF обязателен (hld.md §15) —
#                  потеря нефиксированной мутации без durable outbox-записи
#                  = потерянное списание.

locals {
  redis_clusters = {
    runtime = {
      resource_preset  = "hm3-c4-m16" # выше остальных — hot path на каждое сообщение
      disk_size        = 32
      persistence_mode = "ON" # AOF — не для durability баланса, а чтобы registry/rate-limit
      # переживали быстрый рестарт без полного replay из Kafka
    }
    configuration = {
      resource_preset  = "hm3-c2-m8"
      disk_size        = 16
      persistence_mode = "OFF" # полностью пересобирается из config.changes, экономим IOPS
    }
    billing = {
      resource_preset  = "hm3-c4-m16"
      disk_size        = 32
      persistence_mode = "ON" # AOF — обязателен, financial state (hld.md §15)
    }
  }
}

resource "yandex_mdb_redis_cluster" "mpp" {
  for_each    = local.redis_clusters
  name        = "mpp-${var.environment}-redis-${each.key}"
  environment = var.environment == "production" ? "PRODUCTION" : "PRESTABLE"
  network_id  = yandex_vpc_network.mpp.id

  # persistence_mode — top-level аргумент ресурса, не поле config{} (реальная
  # ошибка `terraform validate` на первой попытке: "Unsupported argument" —
  # схема провайдера отличалась от интуитивного предположения).
  persistence_mode = each.value.persistence_mode

  config {
    version  = "7"
    password = var.redis_passwords[each.key]
  }

  resources {
    resource_preset_id = each.value.resource_preset
    disk_type_id       = "network-ssd"
    disk_size          = each.value.disk_size
  }

  dynamic "host" {
    for_each = var.zones
    content {
      zone      = host.value
      subnet_id = yandex_vpc_subnet.mpp[host.value].id
    }
  }

  sharded = false # Runtime/Billing по нынешним capacity_model.md числам не требуют шардирования;
  # переход на sharded=true — путь масштабирования, не сегодняшняя необходимость
}

variable "redis_passwords" {
  description = "Пароли для 3 Redis-кластеров — секреты окружения, не хардкодятся"
  type        = map(string)
  sensitive   = true
}
