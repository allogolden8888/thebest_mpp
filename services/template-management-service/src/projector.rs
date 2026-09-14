//! Найдено при реализации Ф4 (не предположено): `policy.policy_template`
//! (V013) существовал в схеме, но НИЧЕГО в кодбейзе в него не писало,
//! кроме одноразового seed-INSERT в самой миграции — `policy-service`
//! получает шаблоны через `config.changes` прямо в in-memory `ConfigOverlay`
//! (см. `config_reload.rs`), никогда не материализуя их обратно в эту
//! таблицу. List/search и preview-эндпоинты этого сервиса (см. `list.rs`,
//! `preview.rs`) не имели бы смысла без живой проекции — этот модуль и есть
//! недостающий проектор: тот же принцип, что `config-cache-projector`
//! (`config.changes` -> материализованное состояние), только цель —
//! Postgres, не Redis, потому что list/search нужны SQL-фильтры
//! (partner_id, sender_id, category), а не point-lookup по одному ключу.
//!
//! entity_id в `ConfigChangeEvent` НЕ гарантированно равен `template_id` из
//! payload (проверено по аналогии с находкой в `config-cache-projector` про
//! `entity_id` != `partner_id` для PARTNER — `configuration-service` нигде
//! не проверяет это соответствие ни для одного entity_type) — template_id
//! берётся из самого payload, не из event.entity_id.

use crate::proto::common::ConfigEntityType;
use crate::proto::events::ConfigChangeEvent;
use crate::store::Store;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use serde::Deserialize;

pub const CONFIG_CHANGES_TOPIC: &str = "config.changes";

/// BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя
/// HLD требует ACL": этот сервис — один из трёх пилотных клиентов нового
/// SASL_SCRAM+TLS листенера (порт 9094,
/// infra/kafka/generate_kafka_topics.py::build_kafka_cluster_crd), по
/// одному на язык (Go/Rust/Java) —
/// k8s/generate_manifests.py::KAFKA_SASL_DEMO_SERVICES. Заполняется ТОЛЬКО
/// когда ВСЕ четыре переменные заданы (реальный под этого сервиса, если он
/// не входит в KAFKA_SASL_DEMO_SERVICES, не задаёт ни одну из них) — при их
/// отсутствии сервис продолжает byte-for-byte работать через старый
/// plaintext-листенер (9092), как и остальные ~37 сервисов платформы.
#[derive(Debug, Clone, PartialEq)]
struct KafkaSaslConfig {
    bootstrap_servers: String,
    username: String,
    password: String,
    mechanism: String,
    ca_path: String,
}

fn sasl_config_from_env() -> Option<KafkaSaslConfig> {
    let bootstrap_servers = std::env::var("KAFKA_SASL_BOOTSTRAP_SERVERS").ok()?;
    let username = std::env::var("KAFKA_SASL_USERNAME").ok()?;
    let password = std::env::var("KAFKA_SASL_PASSWORD").ok()?;
    let ca_path = std::env::var("KAFKA_TLS_CA_PATH").ok()?;
    let mechanism = std::env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-512".to_string());
    Some(KafkaSaslConfig { bootstrap_servers, username, password, mechanism, ca_path })
}

/// Вынесено отдельно от `build_consumer`, чтобы конструирование конфига
/// покрывалось unit-тестом без живого брокера (`ClientConfig::create` в
/// тестовом окружении CI не может подключиться ни к какому Kafka).
/// `ssl.ca.location` принимает путь к PEM-файлу напрямую — librdkafka сам
/// читает и парсит сертификат при первом подключении, разбор PEM в Rust-коде
/// здесь не нужен (в отличие от Go/franz-go, где `crypto/tls` требует
/// собранный в памяти `x509.CertPool`).
fn build_client_config(bootstrap_servers: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    match sasl_config_from_env() {
        Some(sasl) => {
            config
                .set("bootstrap.servers", sasl.bootstrap_servers.as_str())
                .set("group.id", group_id)
                .set("enable.auto.commit", "false")
                .set("security.protocol", "SASL_SSL")
                .set("sasl.mechanisms", sasl.mechanism.as_str())
                .set("sasl.username", sasl.username.as_str())
                .set("sasl.password", sasl.password.as_str())
                .set("ssl.ca.location", sasl.ca_path.as_str());
        }
        None => {
            config
                .set("bootstrap.servers", bootstrap_servers)
                .set("group.id", group_id)
                .set("enable.auto.commit", "false");
        }
    }
    config
}

pub fn build_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    build_client_config(bootstrap_servers, group_id)
        .create()
        .expect("не удалось создать Kafka consumer для config.changes")
}

/// Форма payload_json для entity_type=policy_template
/// (config_schemas/policy_template.schema.json) — только поля, нужные для
/// проекции в policy.policy_template.
#[derive(Debug, Deserialize)]
struct PolicyTemplatePayload {
    template_id: String,
    partner_id: String,
    operator_id: Option<String>,
    sender_id: Option<String>,
    channel: String,
    category: String,
    pattern: String,
    version: i32,
    status: String,
}

/// upsert_policy_template — единственное место записи в policy.policy_template
/// из этого сервиса (list/search и preview только читают). ON CONFLICT
/// version-gated: событие со старой версией (переигранное задвоение,
/// out-of-order redelivery) не должно откатить уже применённую более новую
/// версию того же шаблона — тот же принцип, что version-gating в
/// scheduler/execution-control частях кодбейзы.
async fn upsert_policy_template(store: &Store, p: &PolicyTemplatePayload) -> Result<(), sqlx::Error> {
    let template_id: uuid::Uuid = p.template_id.parse().map_err(|e| {
        sqlx::Error::Decode(format!("template_id {:?} не UUID: {e}", p.template_id).into())
    })?;

    sqlx::query(
        r#"
        INSERT INTO policy.policy_template
            (template_id, partner_id, operator_id, sender_id, channel, category, pattern, version, status, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
        ON CONFLICT (template_id) DO UPDATE SET
            partner_id  = EXCLUDED.partner_id,
            operator_id = EXCLUDED.operator_id,
            sender_id   = EXCLUDED.sender_id,
            channel     = EXCLUDED.channel,
            category    = EXCLUDED.category,
            pattern     = EXCLUDED.pattern,
            version     = EXCLUDED.version,
            status      = EXCLUDED.status,
            updated_at  = now()
        WHERE policy_template.version <= EXCLUDED.version
        "#,
    )
    .bind(template_id)
    .bind(&p.partner_id)
    .bind(&p.operator_id)
    .bind(&p.sender_id)
    .bind(&p.channel)
    .bind(&p.category)
    .bind(&p.pattern)
    .bind(p.version)
    .bind(&p.status)
    .execute(&store.pool)
    .await?;
    Ok(())
}

/// run_loop — потребляет config.changes, проецирует только
/// entity_type=POLICY_TEMPLATE, коммитит оффсет только после успешного
/// upsert (тот же commit-after-success паттерн, что в policy-service/
/// kafka_io.rs). Как и в остальном кодбейзе, это означает, что одно
/// неразбираемое сообщение (не ConfigChangeEvent, не валидный
/// PolicyTemplatePayload) блокирует партицию до ручного вмешательства —
/// известный, задокументированный в других местах этого кодбейза
/// компромисс, не решается заново здесь.
pub async fn run_loop(consumer: StreamConsumer, store: Store) {
    consumer.subscribe(&[CONFIG_CHANGES_TOPIC]).expect("не удалось подписаться на config.changes");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    tracing::warn!("config.changes: пустой payload, пропуск без коммита");
                    continue;
                };
                let event = match ConfigChangeEvent::decode(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::error!("config.changes: не удалось декодировать ConfigChangeEvent: {e}");
                        continue;
                    }
                };
                if event.entity_type() != ConfigEntityType::PolicyTemplate {
                    // Не наш entity_type — коммитим сразу, это не ошибка.
                    if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                        tracing::error!("config.changes: commit не прошёл: {e}");
                    }
                    continue;
                }
                let parsed: Result<PolicyTemplatePayload, _> = serde_json::from_slice(&event.payload_json);
                match parsed {
                    Ok(p) => match upsert_policy_template(&store, &p).await {
                        Ok(()) => {
                            if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                                tracing::error!("config.changes: commit не прошёл после upsert: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::error!("config.changes: upsert_policy_template не прошёл (template_id={}): {e}", p.template_id);
                        }
                    },
                    Err(e) => {
                        tracing::error!("config.changes: payload_json не соответствует policy_template.schema.json: {e}");
                    }
                }
            }
            Err(e) => tracing::error!("config.changes: consumer.recv() ошибка: {e}"),
        }
    }
}

#[cfg(test)]
mod sasl_config_tests {
    use super::*;
    use std::sync::Mutex;

    // env::set_var — глобальное состояние процесса; тот же mutex-паттерн,
    // что redis_url.rs (partner-rest-receiver/pipeline-engine) использует
    // по той же причине — тесты этого модуля не должны выполняться
    // параллельно друг с другом.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const KEYS: [&str; 5] = [
        "KAFKA_SASL_BOOTSTRAP_SERVERS",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        "KAFKA_SASL_MECHANISM",
        "KAFKA_TLS_CA_PATH",
    ];

    fn clear_env() {
        for key in KEYS {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn falls_back_to_plaintext_bootstrap_when_no_sasl_env_is_set() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();

        let config = build_client_config("plaintext-broker:9092", "template-management-service");

        clear_env();
        assert_eq!(config.get("bootstrap.servers"), Some("plaintext-broker:9092"));
        assert_eq!(config.get("group.id"), Some("template-management-service"));
        assert_eq!(config.get("security.protocol"), None, "не должно переключаться на SASL_SSL без явных env");
        assert_eq!(config.get("sasl.username"), None);
    }

    #[test]
    fn switches_to_sasl_ssl_only_when_every_required_var_is_present() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094");
            std::env::set_var("KAFKA_SASL_USERNAME", "template-management-service-kafka-user");
            std::env::set_var("KAFKA_SASL_PASSWORD", "s3cr3t");
            std::env::set_var("KAFKA_TLS_CA_PATH", "/etc/mpp/kafka-tls/ca.crt");
            // KAFKA_SASL_MECHANISM intentionally left unset — must default to SCRAM-SHA-512.
        }

        let config = build_client_config("plaintext-broker:9092", "template-management-service");

        clear_env();
        assert_eq!(config.get("bootstrap.servers"), Some("sasl-broker:9094"), "SASL bootstrap must override the plaintext one, not merge with it");
        assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(config.get("sasl.mechanisms"), Some("SCRAM-SHA-512"));
        assert_eq!(config.get("sasl.username"), Some("template-management-service-kafka-user"));
        assert_eq!(config.get("sasl.password"), Some("s3cr3t"));
        assert_eq!(config.get("ssl.ca.location"), Some("/etc/mpp/kafka-tls/ca.crt"));
    }

    #[test]
    fn stays_on_plaintext_when_only_some_sasl_vars_are_present() {
        // Partial config (e.g. a rollout typo dropping one env var) must not
        // silently half-authenticate — falling back to the known-good
        // plaintext path is the safer failure mode than a broker rejecting a
        // malformed SASL handshake at runtime.
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094");
            std::env::set_var("KAFKA_SASL_USERNAME", "template-management-service-kafka-user");
            // KAFKA_SASL_PASSWORD and KAFKA_TLS_CA_PATH left unset.
        }

        let config = build_client_config("plaintext-broker:9092", "template-management-service");

        clear_env();
        assert_eq!(config.get("bootstrap.servers"), Some("plaintext-broker:9092"));
        assert_eq!(config.get("security.protocol"), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    // Тот же паттерн, что list.rs::tests::test_pool — TEST_DATABASE_URL,
    // skip-if-unavailable, не failure всего прогона без Postgres.
    async fn test_pool() -> Option<sqlx::PgPool> {
        let url = match std::env::var("TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                println!("TEST_DATABASE_URL не задан — пропуск теста, требующего Postgres");
                return None;
            }
        };
        match sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&url).await {
            Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                Ok(_) => Some(pool),
                Err(e) => {
                    println!("Postgres недоступен на {url} ({e}) — пропуск теста");
                    None
                }
            },
            Err(e) => {
                println!("не удалось подключиться к Postgres ({e}) — пропуск теста");
                None
            }
        }
    }

    async fn cleanup(pool: &sqlx::PgPool, template_id: uuid::Uuid) {
        let _ = sqlx::query("DELETE FROM policy.policy_template WHERE template_id = $1")
            .bind(template_id)
            .execute(pool)
            .await;
    }

    fn payload(template_id: &str, partner_id: &str, pattern: &str, version: i32, status: &str) -> PolicyTemplatePayload {
        PolicyTemplatePayload {
            template_id: template_id.to_string(),
            partner_id: partner_id.to_string(),
            operator_id: None,
            sender_id: None,
            channel: "SMS".to_string(),
            category: "TRANSACTION".to_string(),
            pattern: pattern.to_string(),
            version,
            status: status.to_string(),
        }
    }

    async fn fetch_row(pool: &sqlx::PgPool, template_id: uuid::Uuid) -> Option<(String, i32, String)> {
        sqlx::query("SELECT pattern, version, status FROM policy.policy_template WHERE template_id = $1")
            .bind(template_id)
            .fetch_optional(pool)
            .await
            .expect("SELECT failed")
            .map(|row| (row.get::<String, _>("pattern"), row.get::<i32, _>("version"), row.get::<String, _>("status")))
    }

    #[tokio::test]
    async fn upsert_inserts_brand_new_template() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let id = uuid::Uuid::new_v4();

        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "hello %w", 1, "active"))
            .await
            .expect("upsert failed");

        let row = fetch_row(&pool, id).await.expect("строка не найдена после INSERT");
        assert_eq!(row, ("hello %w".to_string(), 1, "active".to_string()));

        cleanup(&pool, id).await;
    }

    #[tokio::test]
    async fn upsert_with_higher_version_overwrites_existing_row() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let id = uuid::Uuid::new_v4();

        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "v1 pattern", 1, "active")).await.unwrap();
        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "v2 pattern", 2, "active")).await.unwrap();

        let row = fetch_row(&pool, id).await.expect("строка не найдена");
        assert_eq!(row, ("v2 pattern".to_string(), 2, "active".to_string()), "более новая версия должна применяться");

        cleanup(&pool, id).await;
    }

    // Ядро находки/фикса: version-gating (`WHERE policy_template.version <=
    // EXCLUDED.version`) должен реально отбрасывать устаревшее/переигранное
    // событие, а не просто существовать как SQL, который никто не проверил.
    // Без этого теста регрессия (например, случайно убранный WHERE при
    // рефакторинге) прошла бы незамеченной — INSERT ... ON CONFLICT DO
    // UPDATE без WHERE тоже "работает", просто безусловно откатывает более
    // новую версию первым же переигранным старым событием.
    #[tokio::test]
    async fn upsert_with_lower_version_does_not_overwrite_newer_row() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let id = uuid::Uuid::new_v4();

        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "v5 pattern", 5, "active")).await.unwrap();
        // Переигранное старое событие (например, redelivery после
        // ребаланса Kafka-консьюмера) с версией 3 — не должно откатить уже
        // применённую версию 5.
        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "STALE v3 pattern", 3, "active")).await.unwrap();

        let row = fetch_row(&pool, id).await.expect("строка не найдена");
        assert_eq!(
            row,
            ("v5 pattern".to_string(), 5, "active".to_string()),
            "устаревшая версия 3 не должна была откатить уже применённую версию 5 — version-gating сломан"
        );

        cleanup(&pool, id).await;
    }

    #[tokio::test]
    async fn upsert_with_equal_version_is_idempotent_reapply() {
        // Точное повторное применение той же версии (at-least-once
        // redelivery того же самого события) должно проходить — WHERE
        // использует <=, не <, намеренно.
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let id = uuid::Uuid::new_v4();

        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "original", 4, "active")).await.unwrap();
        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "reapplied same version", 4, "active")).await.unwrap();

        let row = fetch_row(&pool, id).await.expect("строка не найдена");
        assert_eq!(row.1, 4);
        assert_eq!(row.0, "reapplied same version", "повторное применение той же версии обязано пройти (WHERE <=, не <)");

        cleanup(&pool, id).await;
    }

    #[tokio::test]
    async fn upsert_with_archived_status_is_applied() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let id = uuid::Uuid::new_v4();

        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "p", 1, "active")).await.unwrap();
        upsert_policy_template(&store, &payload(&id.to_string(), "acme", "p", 2, "archived")).await.unwrap();

        let row = fetch_row(&pool, id).await.expect("строка не найдена");
        assert_eq!(row.2, "archived", "проектор не должен блокировать переход в archived");

        cleanup(&pool, id).await;
    }

    #[tokio::test]
    async fn upsert_rejects_non_uuid_template_id() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool };

        let result = upsert_policy_template(&store, &payload("not-a-uuid", "acme", "p", 1, "active")).await;
        assert!(result.is_err(), "невалидный template_id должен возвращать ошибку, не паниковать и не тихо игнорироваться");
    }
}
