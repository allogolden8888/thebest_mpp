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

# Production Readiness Review P1 "Security" — "один Postgres-юзер на все
# сервисы": до этих ресурсов КАЖДЫЙ из ~17 сервисов с прямым PostgreSQL-
# подключением подключался как mpp_app (owner базы mpp, то есть фактически
# неограниченные права на всё), а не только на свою схему. Роли ниже —
# идентичность (CREATE ROLE) для каждого домена; САМИ гранты на схемы/таблицы
# не может выразить этот ресурс — `terraform providers schema -json` на
# установленном yandex-cloud/yandex 0.218.0 показывает, что
# `yandex_mdb_postgresql_user.permission` умеет только `database_name` —
# это connectivity-грант уровня Yandex control plane (какие базы этой роли
# вообще разрешено видеть/коннектиться), эквивалент GRANT ALL ON DATABASE
# (CONNECT/CREATE schema), НЕ GRANT на объекты внутри уже существующих схем.
# Ниже он всё равно нужен (без него роль не сможет подключиться к базе
# "mpp" в принципе), но он НЕ заменяет и не ослабляет схемные/табличные
# гранты — свежесозданная роль с одним только этим permission-блоком видит
# базу, но ни одной строки ни в одной существующей схеме (проверено живьём
# на локальном PostgreSQL 17, см. отчёт этой сессии: роль без единого GRANT
# получает "permission denied for schema X" на любой чужой схеме). Реальные
# GRANT USAGE ON SCHEMA / GRANT SELECT|INSERT|UPDATE|DELETE ON <table> —
# в migrations/V036__per_service_postgresql_grants.sql, тем же
# управляемым раннером (infra/migrations/migrate.py), что уже накатывает
# остальной DDL; см. комментарий в начале этого файла с полным списком
# ролей и их обоснованием (какой сервис к какой схеме реально обращается,
# выведено чтением исходников, не из названия сервиса).
#
# Группировка — по домену, не строго 1 сервис = 1 роль: несколько пар
# сервисов реально делят жизненный цикл ОДНОЙ таблицы одной схемы
# (dlr-correlation-writer/dlr-manager; lifecycle-writer/replay-service;
# billing-ledger-writer/billing-reconciliation; configuration-service/
# config-event-publisher) — им назначена ОДНА роль на пару, это честное
# отражение их связанности, не сокращение анализа. Отдельная роль всегда
# заводится там, где отличается периметр доверия, даже если набор грантов
# был бы подмножеством другой роли: mpp_backoffice_read (внутренний
# админский read-only агрегатор поперёк 6 схем) и mpp_messaging_read
# (partner-api, внешний per-partner read-only) НЕ переиспользуют ничью
# чужую роль.
locals {
  postgresql_service_users = {
    # config: configuration-service (владелец config_versions/config_outbox)
    # + config-event-publisher (claim/publish config_outbox, читает
    # config_versions для join статуса) — один outbox producer/consumer контур.
    config = {}
    # billing (write): billing-ledger-writer (владелец billing_ledger) +
    # billing-reconciliation (читает billing_ledger, пишет reconciliation_audit).
    billing_write = {}
    # billing (read-only): billing-self-service-api — партнёрский self-service
    # просмотр истории списаний, никогда не пишет.
    billing_read = {}
    # dlr: dlr-correlation-writer (INSERT) + dlr-manager (SELECT + вызывает
    # create_correlation_partition/drop_old_correlation_partitions).
    dlr = {}
    # policy (write): template-management-service — владелец policy_template,
    # плюс табличный (не схемный) доступ к config.config_outbox — см. V036.
    policy_write = {}
    # policy (read-only): consent-cache-projector — startup resync читает
    # policy.subscriber_consent целиком, никогда не пишет (нет ни одного
    # реального писателя этой таблицы сегодня вне тестовых фикстур —
    # честно задокументированный отдельный пробел, не выдуманный здесь).
    policy_read = {}
    # reconciliation: delivery-reconciliation-service — единственный писатель.
    reconciliation = {}
    # control: execution-control-service — единственный писатель.
    control = {}
    # iam: iam-service — единственный писатель.
    iam = {}
    # incident: incident-service — владелец incident.* + read-only
    # cross-schema control.execution_control_audit (TimelineForIncident).
    incident = {}
    # credentials: credential-issuer-service — владелец credentials.* +
    # read-only cross-schema config.config_versions (LookupCredentialRef).
    credentials = {}
    # support: chat-service — единственный писатель.
    support = {}
    # messaging (write): lifecycle-writer (владелец message_lifecycle_history/
    # message_read_model, создаёт dlq_record) + replay-service (мутирует
    # dlq_record.replay_status, пишет replay_audit).
    messaging = {}
    # messaging (read-only): partner-api — внешний (за OIDC) per-partner
    # просмотр СВОИХ сообщений; отдельный периметр доверия от mpp_messaging
    # и от backoffice-api ниже.
    messaging_read = {}
    # backoffice read-only aggregator: backoffice-api — административный BFF,
    # читает 6 чужих схем напрямую (не через gRPC), но НИ ОДНОГО
    # INSERT/UPDATE/DELETE не найдено ни в одном store-файле этого сервиса.
    backoffice_read = {}
  }
}

resource "yandex_mdb_postgresql_user" "service" {
  for_each   = local.postgresql_service_users
  cluster_id = yandex_mdb_postgresql_cluster.mpp.id
  name       = "mpp_${each.key}"
  password   = var.postgresql_service_passwords[each.key]
  conn_limit = 20 # каждый домен-сервис — 1-2 реплики, не hot-path pipeline-engine (200 у mpp_app)

  permission {
    database_name = yandex_mdb_postgresql_database.mpp.name
  }
}

variable "postgresql_service_passwords" {
  description = "Пароли для per-domain PostgreSQL-ролей (mpp_config, mpp_billing_write, ...) — секреты окружения (TF_VAR_postgresql_service_passwords), map(domain -> password), не хардкодятся; в проде — через Vault/External Secrets"
  type        = map(string)
  sensitive   = true
}
