# HA PostgreSQL — source of truth для message_lifecycle_history, billing_ledger,
# dlr_correlation и т.д. (migrations/V001-V017). 3 хоста = 1 primary + 2 sync
# replica, переживает потерю одной зоны без потери данных (synchronous_mode).

resource "yandex_mdb_postgresql_cluster" "mpp" {
  name        = "mpp-${var.environment}-pg"
  environment = var.environment == "production" ? "PRODUCTION" : "PRESTABLE"
  network_id  = yandex_vpc_network.mpp.id

  config {
    version = 17
    resources {
      resource_preset_id = "s2.medium"
      disk_type_id       = "network-ssd"
      disk_size          = 200
    }
    postgresql_config = {
      # Партиционирование обслуживается application-side функциями
      # (migrations/V015__partition_maintenance.sql), не pg_partman —
      # решение зафиксировано ещё на этапе DDL ради портируемости.
      max_connections = 400
    }

    # Автоматический backup — управляемый сервис уже умеет это нативно
    # (`backup_retain_period_days`/`backup_window_start` внутри config{},
    # подтверждено `terraform providers schema -json` на реально
    # установленном yandex-cloud/yandex 0.218.0, а не предположено).
    # PostgreSQL — durable source of truth для config/IAM/billing
    # (data_infrastructure_spec.md §1) — самый строгий RPO из всех
    # хранилищ платформы, отсюда 14 суток хранения бэкапов (не дефолтные
    # 7 — восстановление после инцидента, обнаруженного не в тот же день,
    # не должно упираться в уже вытесненный бэкап) и окно в 03:00 UTC
    # (минимум трафика по HLD-профилю нагрузки, вне SLA-чувствительных
    # часов). Управляемый сервис держит непрерывный WAL-архив поверх
    # суточного full backup, поэтому фактический RPO определяется
    # задержкой WAL-стриминга (секунды-минуты), а не интервалом между
    # full-бэкапами — см. DISASTER_RECOVERY_RUNBOOK.md за обоснованием
    # итоговых цифр RPO/RTO.
    backup_retain_period_days = 14
    backup_window_start {
      hours   = 3
      minutes = 0
    }
  }

  host {
    zone      = var.zones[0]
    subnet_id = yandex_vpc_subnet.mpp[var.zones[0]].id
  }
  host {
    zone                    = var.zones[1]
    subnet_id               = yandex_vpc_subnet.mpp[var.zones[1]].id
    replication_source_name = ""
  }
  host {
    zone                    = var.zones[2]
    subnet_id               = yandex_vpc_subnet.mpp[var.zones[2]].id
    replication_source_name = ""
  }

  user {
    name       = "mpp_app"
    password   = var.postgresql_app_password
    conn_limit = 200
  }
}

# `database{}` внутри yandex_mdb_postgresql_cluster — deprecated (тот же
# паттерн провайдера, что у ClickHouse), отдельный ресурс — актуальная форма.
resource "yandex_mdb_postgresql_database" "mpp" {
  cluster_id = yandex_mdb_postgresql_cluster.mpp.id
  name       = "mpp"
  owner      = "mpp_app"
}

variable "postgresql_app_password" {
  description = "Пароль mpp_app — секрет окружения (TF_VAR_postgresql_app_password), не хардкодится; в проде — через Vault/External Secrets, см. infra/README.md"
  type        = string
  sensitive   = true
}
