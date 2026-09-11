//! Bootstrap + on-demand read path against Configuration Redis —
//! BACKOFFICE_ROADMAP.md "Production Readiness Review" P0 #4, mirroring
//! the Go sibling
//! `services/partner-notification-service/internal/config/redis_source.go`
//! (same task, same key format) and reading the exact keys
//! `config-cache-projector`'s `WriteProjection`
//! (`services/config-cache-projector/internal/projector/projector.go`)
//! writes via Redis `MULTI`/`EXEC`:
//!
//!   `config:current:partner:{partner_id}`            STRING  active version number
//!   `config:version:partner:{partner_id}:{version}`  STRING  partner.schema.json payload
//!
//! This is the platform's live source of truth for partner config —
//! reading it directly (bootstrap SCAN + per-event GET on `config.changes`)
//! replaces the old load-once-from-static-file design without a new write
//! path or schema.

use crate::partner_config::{Partner, PartnerSnapshot};
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::fmt;

const PARTNER_CURRENT_PREFIX: &str = "config:current:partner:";

fn current_key(partner_id: &str) -> String {
    format!("{PARTNER_CURRENT_PREFIX}{partner_id}")
}

fn version_key(partner_id: &str, version: i64) -> String {
    format!("config:version:partner:{partner_id}:{version}")
}

#[derive(Debug)]
pub enum ConfigSourceError {
    Redis(String),
    /// `config:current` указывает на версию, для которой отсутствует
    /// `config:version` — несогласованность projector'а, должна дойти до
    /// вызывающей стороны как retriable ошибка, не тихо проигнорирована.
    MissingVersion { partner_id: String, version: i64 },
    Deserialize { partner_id: String, error: String },
}

impl fmt::Display for ConfigSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSourceError::Redis(e) => write!(f, "Configuration Redis error: {e}"),
            ConfigSourceError::MissingVersion { partner_id, version } => write!(
                f,
                "config:current:partner:{partner_id} указывает на версию {version}, но {} отсутствует в Configuration Redis (несогласованность projector'а)",
                version_key(partner_id, *version)
            ),
            ConfigSourceError::Deserialize { partner_id, error } => {
                write!(f, "partner.schema.json unmarshal (partner_id={partner_id}): {error}")
            }
        }
    }
}

impl std::error::Error for ConfigSourceError {}

/// Re-fetch a single partner_id's current version from Configuration
/// Redis. Used both by `load_all` (bootstrap) and by
/// `config_reload::handle_config_change` (live refresh on `config.changes`,
/// entity_type=PARTNER): re-reads from Redis rather than trusting the
/// Kafka event's own `payload_json`, because Redis (via
/// config-cache-projector's active-only gating on `config:current`) is
/// what actually decides which version is "current" — an event alone
/// doesn't tell us that an older, already-superseded active version isn't
/// still what `config:current` points to under reordering/replay.
///
/// `Ok(None)` means partner_id has no `config:current:partner:*` entry at
/// all (never projected, or config-cache-projector hasn't caught up yet)
/// — callers should treat this the same as an archived partner: not
/// eligible to be resolved for live traffic.
pub async fn fetch_partner(conn: &mut MultiplexedConnection, partner_id: &str) -> Result<Option<Partner>, ConfigSourceError> {
    let version: Option<i64> =
        conn.get(current_key(partner_id)).await.map_err(|e| ConfigSourceError::Redis(e.to_string()))?;
    let Some(version) = version else {
        return Ok(None);
    };

    let payload: Option<Vec<u8>> =
        conn.get(version_key(partner_id, version)).await.map_err(|e| ConfigSourceError::Redis(e.to_string()))?;
    let Some(payload) = payload else {
        return Err(ConfigSourceError::MissingVersion { partner_id: partner_id.to_string(), version });
    };

    let partner: Partner = serde_json::from_slice(&payload)
        .map_err(|e| ConfigSourceError::Deserialize { partner_id: partner_id.to_string(), error: e.to_string() })?;
    Ok(Some(partner))
}

/// Bootstrap read of every partner currently projected into Configuration
/// Redis, replacing the old load-once-from-static-file snapshot. SCANs
/// `config:current:partner:*` (cursor-based, non-blocking) and re-fetches
/// each via `fetch_partner`.
///
/// Archived partners are excluded from the bootstrap snapshot for the
/// same reason `config.changes`-driven updates remove them live (see
/// `Partner::is_archived` / `config_reload::handle_config_change`) — an
/// archived partner should not resolve to a delivery channel, whether
/// that's discovered at startup or via a later event.
pub async fn load_all(conn: &mut MultiplexedConnection) -> Result<PartnerSnapshot, ConfigSourceError> {
    let pattern = format!("{PARTNER_CURRENT_PREFIX}*");
    let mut partner_ids = Vec::new();
    {
        let mut iter: redis::AsyncIter<'_, String> =
            conn.scan_match(&pattern).await.map_err(|e| ConfigSourceError::Redis(e.to_string()))?;
        while let Some(key) = iter.next_item().await {
            if let Some(partner_id) = key.strip_prefix(PARTNER_CURRENT_PREFIX) {
                if !partner_id.is_empty() {
                    partner_ids.push(partner_id.to_string());
                }
            }
        }
    }

    let mut partners = Vec::new();
    for partner_id in partner_ids {
        match fetch_partner(conn, &partner_id).await? {
            Some(partner) if !partner.is_archived() => partners.push(partner),
            _ => {}
        }
    }
    Ok(PartnerSnapshot::from_partners(partners))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partner_config::{Application, AuthConfig};
    use redis::AsyncCommands;

    // Тот же skip-паттерн, что idempotency.rs/msgctx.rs/config_reload.rs —
    // реальный Redis round-trip, не мок; пропускается, если Redis
    // недоступен в песочнице/CI вместо непонятного зависания/паники.
    async fn test_conn() -> Option<MultiplexedConnection> {
        let url = std::env::var("PARTNER_REST_RECEIVER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string());
        redis::Client::open(url.as_str()).ok()?.get_multiplexed_async_connection().await.ok()
    }

    fn unique_id(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4())
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

    async fn seed(conn: &mut MultiplexedConnection, partner: &Partner) {
        let payload = serde_json::to_vec(partner).unwrap();
        let _: () = conn.set(version_key(&partner.partner_id, partner.version as i64), payload).await.unwrap();
        let _: () = conn.set(current_key(&partner.partner_id), partner.version as i64).await.unwrap();
    }

    async fn cleanup(conn: &mut MultiplexedConnection, partner_id: &str, version: i64) {
        let _: redis::RedisResult<()> = conn.del(current_key(partner_id)).await;
        let _: redis::RedisResult<()> = conn.del(version_key(partner_id, version)).await;
    }

    #[tokio::test]
    async fn fetch_partner_reads_current_version_payload() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("fetch-acme");
        seed(&mut conn, &test_partner(&id, "active", 3)).await;

        let partner = fetch_partner(&mut conn, &id).await.expect("fetch должен пройти").expect("ожидали Some");
        assert_eq!(partner.partner_id, id);
        assert_eq!(partner.version, 3);

        cleanup(&mut conn, &id, 3).await;
    }

    #[tokio::test]
    async fn fetch_partner_returns_none_when_no_current_pointer() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("fetch-ghost");

        let result = fetch_partner(&mut conn, &id).await.expect("Ok(None), не ошибка");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn fetch_partner_errors_on_inconsistent_projection() {
        let Some(mut conn) = test_conn().await else { return };
        let id = unique_id("fetch-broken");
        // config:current указывает на версию 5, config:version для неё не записан.
        let _: () = conn.set(current_key(&id), 5i64).await.unwrap();

        let result = fetch_partner(&mut conn, &id).await;
        assert!(result.is_err(), "несогласованная проекция должна быть ошибкой, не тихим None");

        cleanup(&mut conn, &id, 5).await;
    }

    #[tokio::test]
    async fn load_all_excludes_archived_partners() {
        let Some(mut conn) = test_conn().await else { return };
        let active_id = unique_id("load-active");
        let archived_id = unique_id("load-archived");
        seed(&mut conn, &test_partner(&active_id, "active", 1)).await;
        seed(&mut conn, &test_partner(&archived_id, "archived", 1)).await;

        let snapshot = load_all(&mut conn).await.expect("load_all должен пройти");
        assert!(snapshot.application(&active_id, &format!("{active_id}_app")).is_some());
        assert!(snapshot.application(&archived_id, &format!("{archived_id}_app")).is_none());

        cleanup(&mut conn, &active_id, 1).await;
        cleanup(&mut conn, &archived_id, 1).await;
    }
}
