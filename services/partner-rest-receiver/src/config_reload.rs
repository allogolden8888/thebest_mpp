//! `config.changes` consumer — BACKOFFICE_ROADMAP.md "Production
//! Readiness Review" P0 #4: partner config used to load exactly once from
//! `PARTNER_CONFIG_PATH` at startup and never again — a partner's
//! self-service change to their application/sender/webhook config never
//! reached live traffic without a manual restart. Same fix, same shape
//! as the already-real `config_reload.rs` in
//! destination-resolution-service/policy-service/pipeline-engine/
//! routing-service (`arc-swap` declared but never actually wired to a
//! Kafka consumer until those passes) — here for `entity_type=PARTNER`.
//!
//! Unlike those siblings' overlay-merge pattern (dynamic layer over a
//! separately-loaded static base), this service's "snapshot" IS the
//! partner map — no merge needed, so `handle_config_change` re-fetches
//! the touched partner_id from Configuration Redis and applies
//! upsert/remove directly to a cloned `PartnerSnapshot` before publishing
//! it via `ArcSwap::store`.
//!
//! Separate consumer group from the main `producer`-only wiring
//! (`kafka_io.rs` — this service has no other consumer) — `config.changes`
//! is a compacted topic with several entity_type in one topic (see
//! `platform-contracts/events/config_and_control.proto`); this service
//! filters to PARTNER only, everything else is committed and skipped.

use crate::config_source;
use crate::partner_config::PartnerSnapshot;
use crate::proto::common::ConfigEntityType;
use crate::proto::events::ConfigChangeEvent;
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use redis::aio::MultiplexedConnection;
use std::sync::Arc;

pub const TOPIC: &str = "config.changes";

/// `auto.offset.reset=earliest` + a per-process-unique `group_id` — same
/// reasoning as the sibling services' `config_reload.rs` (fullest writeup:
/// `services/policy-service/src/config_reload.rs`): `config.changes` is
/// compacted, and a freshly started process needs to replay the entire
/// current history of active entity_id, not just events from "now". A
/// fixed group_id surviving process restarts would make
/// `auto.offset.reset=earliest` only matter on the very first-ever start —
/// every later restart would resume from the previous process's committed
/// offset and never replay history into the fresh in-memory ArcSwap.
pub fn build_config_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    let unique_group_id = format!(
        "{group_id}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
    );
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", &unique_group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("не удалось создать Kafka consumer (config.changes)")
}

/// Ядро обработки одного `ConfigChangeEvent` — вынесено отдельно от Kafka-
/// цикла ради тестируемости без реального брокера (тот же принцип, что у
/// сиблингов). Re-fetches `event.entity_id` from Configuration Redis (не
/// доверяет `event.payload_json` напрямую — см. `config_source::fetch_partner`)
/// and applies the result to `live` via `ArcSwap::rcu` (atomically read-
/// copy-update: builds the new snapshot from whatever is currently live,
/// so concurrent Kafka-loop-only writers never lose an update to a race —
/// there's only one writer here in practice, but `rcu` is correct
/// regardless and costs nothing extra).
///
/// Returns `Ok(true)` if `live` was updated (entity_type was PARTNER and
/// re-fetch succeeded), `Ok(false)` if the event was for a different
/// entity_type (not our concern), and `Err` for Configuration Redis
/// failures — callers should treat `Err` as retriable (do not commit the
/// offset).
pub async fn handle_config_change(
    conn: &mut MultiplexedConnection,
    live: &ArcSwap<PartnerSnapshot>,
    event: &ConfigChangeEvent,
) -> Result<bool, config_source::ConfigSourceError> {
    if event.entity_type != ConfigEntityType::Partner as i32 {
        return Ok(false);
    }

    let partner_id = event.entity_id.clone();
    let fetched = config_source::fetch_partner(conn, &partner_id).await?;

    match fetched {
        Some(partner) if !partner.is_archived() => {
            let version = partner.version;
            let status = partner.status.clone();
            live.rcu(|current| {
                let mut next = (**current).clone();
                next.upsert(partner.clone());
                next
            });
            tracing::info!("config.changes: partner_id={partner_id} (version={version}, status={status}) обновлён в живом снапшоте");
        }
        found => {
            live.rcu(|current| {
                let mut next = (**current).clone();
                next.remove(&partner_id);
                next
            });
            tracing::info!("config.changes: partner_id={partner_id} удалён из живого снапшота (found={})", found.is_some());
        }
    }
    Ok(true)
}

pub async fn run_loop(consumer: StreamConsumer, mut conn: MultiplexedConnection, live: Arc<ArcSwap<PartnerSnapshot>>) {
    consumer.subscribe(&[TOPIC]).expect("не удалось подписаться на config.changes");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                    continue;
                };
                let event = match ConfigChangeEvent::decode(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::error!("config.changes: не удалось декодировать ConfigChangeEvent (poison message, пропущен): {e}");
                        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                        continue;
                    }
                };

                match handle_config_change(&mut conn, &live, &event).await {
                    Ok(_) => {
                        if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                            tracing::error!("не удалось закоммитить offset (config.changes): {e}");
                        }
                    }
                    Err(e) => {
                        // Retriable — Configuration Redis сбой/таймаут: НЕ
                        // коммитим, at-least-once, запись переобработается на
                        // следующем poll. Не разбираемся дальше по партициям
                        // (в отличие от config-cache-projector) — этот
                        // сервис однопоточно читает config.changes целиком в
                        // этом цикле, следующий recv() естественно
                        // переиграет тот же offset, раз он не закоммичен.
                        tracing::error!("config.changes: обработка entity_id={} провалилась, offset не коммитится: {e}", event.entity_id);
                    }
                }
            }
            Err(e) => tracing::error!("ошибка Kafka consumer (config.changes): {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partner_config::{Application, AuthConfig, Partner};
    use redis::AsyncCommands;

    // Тот же skip-паттерн, что уже используют idempotency.rs/msgctx.rs в
    // этом сервисе (нет Rust-эквивалента miniredis здесь) — реальный round-
    // trip против Redis, а не мок, но тесты не должны падать/висеть, если
    // песочница/CI не поднимает Redis на этот адрес.
    fn test_redis_url() -> String {
        std::env::var("PARTNER_REST_RECEIVER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
    }

    async fn test_conn() -> Option<MultiplexedConnection> {
        redis::Client::open(test_redis_url().as_str()).ok()?.get_multiplexed_async_connection().await.ok()
    }

    async fn cleanup(conn: &mut MultiplexedConnection, current_key: &str, version_key: &str) {
        let _: redis::RedisResult<()> = conn.del(current_key).await;
        let _: redis::RedisResult<()> = conn.del(version_key).await;
    }

    fn test_partner(id: &str, status: &str, version: u64) -> Partner {
        Partner {
            partner_id: id.to_string(),
            version,
            status: status.to_string(),
            applications: vec![Application {
                application_id: format!("{id}_app"),
                display_name: "Test".to_string(),
                auth: AuthConfig { auth_type: "API_KEY".to_string(), credential_ref: "cred".to_string() },
                ip_allowlist: vec![],
                rate_limit_tps: 10,
                allowed_channels: vec!["sms".to_string()],
            }],
        }
    }

    fn current_key(id: &str) -> String {
        format!("config:current:partner:{id}")
    }

    fn version_key(id: &str, version: u64) -> String {
        format!("config:version:partner:{id}:{version}")
    }

    async fn seed(conn: &mut MultiplexedConnection, partner: &Partner) {
        let payload = serde_json::to_vec(partner).unwrap();
        let _: () = conn.set(version_key(&partner.partner_id, partner.version), payload).await.unwrap();
        let _: () = conn.set(current_key(&partner.partner_id), partner.version as i64).await.unwrap();
    }

    fn change_event(partner_id: &str, version: i64) -> ConfigChangeEvent {
        ConfigChangeEvent {
            entity_type: ConfigEntityType::Partner as i32,
            entity_id: partner_id.to_string(),
            version,
            payload_json: vec![],
            status: "active".to_string(),
            created_at: None,
        }
    }

    fn unique_id(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4())
    }

    #[tokio::test]
    async fn ignores_events_for_other_entity_types() {
        let Some(mut conn) = test_conn().await else { return };
        let live = ArcSwap::from_pointee(PartnerSnapshot::default());
        let mut event = change_event("acme", 1);
        event.entity_type = ConfigEntityType::BillingTariff as i32;

        let updated = handle_config_change(&mut conn, &live, &event).await.expect("не должно ошибиться");
        assert!(!updated);
        assert!(live.load().is_empty());
    }

    #[tokio::test]
    async fn active_partner_is_fetched_from_redis_and_upserted() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("acme-active");
        let partner = test_partner(&id, "active", 1);
        seed(&mut conn, &partner).await;

        let live = ArcSwap::from_pointee(PartnerSnapshot::default());
        let updated = handle_config_change(&mut conn, &live, &change_event(&id, 1)).await.expect("fetch должен пройти");
        assert!(updated);
        assert!(live.load().application(&id, &format!("{id}_app")).is_some());

        cleanup(&mut conn, &current_key(&id), &version_key(&id, 1)).await;
    }

    #[tokio::test]
    async fn archived_partner_is_removed_from_live_snapshot() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("acme-archived");
        seed(&mut conn, &test_partner(&id, "archived", 2)).await;
        let mut initial = PartnerSnapshot::default();
        initial.upsert(test_partner(&id, "active", 1));
        let live = ArcSwap::from_pointee(initial);

        handle_config_change(&mut conn, &live, &change_event(&id, 2)).await.expect("fetch должен пройти");
        assert!(live.load().application(&id, &format!("{id}_app")).is_none());

        cleanup(&mut conn, &current_key(&id), &version_key(&id, 2)).await;
    }

    #[tokio::test]
    async fn partner_missing_from_configuration_redis_is_removed() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("ghost");
        // Ничего не засеяно — config:current:partner:{id} отсутствует.
        let mut initial = PartnerSnapshot::default();
        initial.upsert(test_partner(&id, "active", 1));
        let live = ArcSwap::from_pointee(initial);

        handle_config_change(&mut conn, &live, &change_event(&id, 1)).await.expect("Ok(None) не ошибка");
        assert!(live.load().application(&id, &format!("{id}_app")).is_none());
    }

    #[tokio::test]
    async fn inconsistent_projection_is_retriable_error() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("broken");
        let _: () = conn.set(current_key(&id), 5i64).await.unwrap();
        // config:version:partner:{id}:5 намеренно не записан.
        let live = ArcSwap::from_pointee(PartnerSnapshot::default());

        let result = handle_config_change(&mut conn, &live, &change_event(&id, 5)).await;
        assert!(result.is_err(), "несогласованная проекция должна быть retriable, не тихо проигнорирована");

        cleanup(&mut conn, &current_key(&id), &version_key(&id, 5)).await;
    }
}
