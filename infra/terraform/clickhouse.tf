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

  # Автоматический backup — top-level атрибуты в `_v2` (не block, в отличие
  # от `yandex_mdb_postgresql_cluster.config.backup_window_start`), тот же
  # "nested attributes, не repeatable blocks" паттерн, что уже отмечен в
  # комментарии наверху файла про `hosts = {...}`; подтверждено
  # `terraform providers schema -json` на установленном провайдере, не
  # предположено. ClickHouse — не source of truth (PostgreSQL — источник),
  # при недоступности Analytics Writer копит lag и продолжает работу
  # (комментарий наверху файла) — отсюда более мягкий RPO/retention, чем у
  # PostgreSQL: 7 суток бэкапов достаточно для аналитического/диагностического
  # хранилища, не транзакционного. Обоснование цифр — DISASTER_RECOVERY_RUNBOOK.md.
  backup_retain_period_days = 7
  backup_window_start = {
    hours   = 4
    minutes = 0
  }

  maintenance_window {
    type = "ANYTIME" # staging по умолчанию; production — переключить на WEEKLY с конкретным окном
  }
}

resource "yandex_mdb_clickhouse_database" "mpp_analytics" {
  cluster_id = yandex_mdb_clickhouse_cluster_v2.mpp.id
  name       = "mpp_analytics"
}

# ПРЕДСУЩЕСТВУЮЩЕЕ РАСХОЖДЕНИЕ (обнаружено в ходе этой сессии, не мой
# скоуп чинить сейчас — переименование рискует конфликтом с уже
# импортированным state, отдельная задача): ни analytics-writer, ни
# pdu-log-writer не используют это Terraform-имя базы вообще —
# EnsureSchema в обоих (`internal/store/store.go`) делает
# `CREATE DATABASE IF NOT EXISTS analytics` и все запросы адресуют
# `analytics.<table>` буквально, независимо от CLICKHOUSE_DB. Ресурс
# `mpp_analytics` выше сегодня фактически не используется приложением.
# local.clickhouse_service_grants ниже поэтому ссылается на "analytics" —
# реальное имя базы в проде и локально, не на этот orphaned ресурс.
variable "clickhouse_password" {
  description = "Пароль admin-пользователя ClickHouse (sql_user_management=true) — секрет окружения, не хардкодится"
  type        = string
  sensitive   = true
}

# Production Readiness Review P1 "Security" — "ClickHouse: admin": до этих
# ресурсов analytics-writer, pdu-log-writer, backoffice-api (PDU-log read
# endpoint) и partner-api подключались ОДНИМ И ТЕМ ЖЕ ClickHouse-пользователем
# "admin" — полный доступ на запись/чтение/DDL для всего кластера каждому из
# четырёх сервисов, включая два read-only HTTP API.
#
# `terraform providers schema -json` на установленном provider'е показывает,
# что `yandex_mdb_clickhouse_user.permission` — тот же "database_name-only"
# потолок, что у Postgres-эквивалента: грант на уровне БАЗЫ целиком, нет
# табличного уровня. analytics-writer и pdu-log-writer СОЗНАТЕЛЬНО пишут в
# одну и ту же базу "analytics" (см. pdu-log-writer/internal/store/store.go
# package doc: "Отдельная таблица... в ТОЙ ЖЕ базе analytics... тот же
# выбор, что уже работает") — это already-deliberate архитектурное решение
# этой кодовой базы, разносить их по отдельным базам ради того, чтобы
# Terraform-нативный `permission`-блок мог их изолировать, значило бы
# отменить это решение без указания на то в задаче, поэтому здесь другой
# путь: сами роли создаются БЕЗ `permission`-блока (ноль грантов по
# умолчанию — ClickHouse RBAC, ровно как голая CREATE ROLE в PostgreSQL
# выше), а точные табличные GRANT SELECT/INSERT — bootstrap-скриптом
# (infra/clickhouse/production_grants.sql), потому что у ClickHouse (в
# отличие от Postgres) нет отдельного управляемого раннера миграций в этом
# репозитории — см. README там за тем, как и когда его накатывать. Это
# честно более лёгкий механизm, чем infra/migrations/migrate.py, не
# притворяется его эквивалентом.
#
# Реальный результат (живая проверка на локальном ClickHouse 24.8, см. отчёт
# сессии): analytics_writer не может прочитать operator_pdu_log,
# pdu_log_writer не может прочитать stage_events, оба read-only пользователя
# (readonly=1) не могут писать никуда — настоящая табличная изоляция, не
# только "GRANT выглядит синтаксически верно".
#
# Единственный оставшийся общий периметр — CREATE TABLE: EnsureSchema в
# analytics-writer/pdu-log-writer сами создают свою таблицу при старте
# (`CREATE TABLE IF NOT EXISTS`), а CREATE TABLE в ClickHouse грантуется
# только на уровне БАЗЫ (`ON analytics.*`), не на конкретное ещё
# не существующее имя таблицы — так что оба писателя технически МОГУТ
# создать/уронить таблицу друг друга, хотя ни один не может прочитать или
# записать данные в чужую. Честно задокументированный, не устранённый здесь
# пробел (закрывался бы вынесением DDL-провижининга из старта приложения в
# отдельный bootstrap-шаг — за пределами этого среза).
locals {
  clickhouse_service_users = {
    # analytics-writer — владелец analytics.stage_events +
    # stage_events_hourly_mv (materialized view).
    analytics_writer = { readonly = false }
    # pdu-log-writer — владелец analytics.operator_pdu_log.
    pdu_log_writer = { readonly = false }
    # backoffice-api — PDU-log read endpoint (Экраны 38-40) + отчётность по
    # stage_events; читает ОБЕ таблицы, никогда не пишет.
    analytics_reader = { readonly = true }
    # partner-api — партнёрский (внешний, за OIDC) отчёт по своим
    # сообщениям; читает ТОЛЬКО stage_events (не operator_pdu_log — тот
    # виден только внутренней админке). Отдельная роль от analytics_reader
    # даже притом, что грант — подмножество: разный периметр доверия
    # (внешний партнёрский API против внутреннего админского).
    partner_api_reader = { readonly = true }
  }
}

resource "yandex_mdb_clickhouse_user" "service" {
  for_each   = local.clickhouse_service_users
  cluster_id = yandex_mdb_clickhouse_cluster_v2.mpp.id
  name       = each.key
  password   = var.clickhouse_service_passwords[each.key]

  # Намеренно БЕЗ permission{} — см. комментарий выше: этот блок дал бы
  # доступ на всю базу "analytics" целиком (тот самый потолок, который мы
  # обходим), а не только к своим таблицам. Реальные гранты —
  # infra/clickhouse/production_grants.sql.
  dynamic "settings" {
    for_each = each.value.readonly ? [1] : []
    content {
      readonly = 1 # форсирует read-only режим запроса — INSERT/ALTER/CREATE
      # отклоняются на уровне настройки сессии, а не только отсутствием
      # GRANT (defense-in-depth: даже ошибочный будущий GRANT INSERT этому
      # пользователю не даст ему писать, пока стоит этот readonly).
    }
  }
}

variable "clickhouse_service_passwords" {
  description = "Пароли для per-service ClickHouse-пользователей (analytics_writer, pdu_log_writer, analytics_reader, partner_api_reader) — секреты окружения (TF_VAR_clickhouse_service_passwords), map(service -> password), не хардкодятся"
  type        = map(string)
  sensitive   = true
}
