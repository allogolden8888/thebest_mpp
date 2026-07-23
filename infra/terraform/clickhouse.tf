# Analytics Writer / Partner API / Backoffice API — аналитическое хранилище,
# не source of truth (тот — PostgreSQL). При недоступности ClickHouse
# Analytics Writer копит lag, обработка сообщений продолжается (hld.md,
# failure-mode table) — поэтому HA здесь мягче, чем у PostgreSQL/Billing Redis.
#
# `_v2` — `yandex_mdb_clickhouse_cluster` (без `_v2`) помечен deprecated
# провайдером (`terraform validate` реально это подтвердил при первой
# попытке), схема `_v2` — nested attributes (map), не повторяемые blocks,
# отсюда `hosts = {...}` вместо `host {...}`.

resource "yandex_mdb_clickhouse_cluster_v2" "mpp" {
  name        = "mpp-${var.environment}-ch"
  environment = var.environment == "production" ? "PRODUCTION" : "PRESTABLE"
  network_id  = yandex_vpc_network.mpp.id

  sql_user_management     = true
  sql_database_management = true
  admin_password          = var.clickhouse_password

  clickhouse = {
    resources = {
      resource_preset_id = "s2.medium"
      disk_type_id       = "network-ssd"
      disk_size          = 200
    }
  }

  hosts = { for z in var.zones : z => {
    zone      = z
    subnet_id = yandex_vpc_subnet.mpp[z].id
    type      = "CLICKHOUSE"
  } }

  maintenance_window {
    type = "ANYTIME" # staging по умолчанию; production — переключить на WEEKLY с конкретным окном
  }
}

resource "yandex_mdb_clickhouse_database" "mpp_analytics" {
  cluster_id = yandex_mdb_clickhouse_cluster_v2.mpp.id
  name       = "mpp_analytics"
}

variable "clickhouse_password" {
  description = "Пароль admin-пользователя ClickHouse (sql_user_management=true) — секрет окружения, не хардкодится"
  type        = string
  sensitive   = true
}
