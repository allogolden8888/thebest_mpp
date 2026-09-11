//! Партнёрский конфиг-снапшот — `config_schemas/partner.schema.json`.
//! Статический файл служит только bootstrap fallback, а production state
//! устанавливает полный replay `config.changes` и затем обновляет live.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    // Не читается этим сервисом сегодня — единственный поддерживаемый в этом
    // срезе `auth.rs::EnvAuthVerifier` не различает API_KEY/MTLS/SMPP_BIND,
    // всегда сравнивает `credential_ref`-производную переменную окружения.
    // Поле оставлено как честное зеркало `partner.schema.json` (полная
    // структура конфига, не urезанная под сегодняшние потребности) — когда
    // появится реальный multi-auth-type Vault-клиент, этому полю будет что
    // делать без повторного парсинга конфига.
    #[serde(rename = "type")]
    #[allow(dead_code)]
    pub auth_type: String,
    pub credential_ref: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Application {
    pub application_id: String,
    // Не читается — операционное/UI-поле (Backoffice), не участвует ни в
    // одной проверке `service_internal_methods.md` §1.1.
    #[allow(dead_code)]
    pub display_name: String,
    pub auth: AuthConfig,
    pub ip_allowlist: Vec<String>,
    pub rate_limit_tps: u32,
    pub allowed_channels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Partner {
    pub partner_id: String,
    // Версия внутри payload не является Kafka/Config Service version
    // (self-service ведёт её отдельно для читаемости), но остаётся частью
    // валидируемого partner document.
    pub version: u64,
    pub status: String,
    pub applications: Vec<Application>,
}

impl Partner {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }
}

#[derive(Debug, Clone)]
struct VersionedPartner {
    config_version: i64,
    // None — versioned archive tombstone. Версию необходимо хранить, иначе
    // поздняя доставка старого active события воскресит удалённого партнёра.
    partner: Option<Arc<Partner>>,
}

#[derive(Debug, Default)]
struct SnapshotState {
    ready: bool,
    partners: HashMap<String, VersionedPartner>,
}

/// Атомарный для читателя live snapshot всех PARTNER entity. Клон разделяет
/// одно состояние между Axum AppState и background Kafka consumer.
#[derive(Debug, Clone, Default)]
pub struct PartnerSnapshot {
    state: Arc<RwLock<SnapshotState>>,
}

impl PartnerSnapshot {
    /// Готовый snapshot для тестов/явного offline режима.
    pub fn from_partners(partners: Vec<Partner>) -> Self {
        Self::with_partners(partners, true)
    }

    /// Bootstrap-файл не открывает production ingress до полного replay
    /// config.changes. Он нужен только как диагностическая/локальная база.
    pub fn bootstrap_from_partners(partners: Vec<Partner>) -> Self {
        Self::with_partners(partners, false)
    }

    fn with_partners(partners: Vec<Partner>, ready: bool) -> Self {
        let entries = partners
            .into_iter()
            .map(|partner| {
                let version = partner.version as i64;
                (
                    partner.partner_id.clone(),
                    VersionedPartner {
                        config_version: version,
                        partner: Some(Arc::new(partner)),
                    },
                )
            })
            .collect();
        Self {
            state: Arc::new(RwLock::new(SnapshotState {
                ready,
                partners: entries,
            })),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, SnapshotState> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SnapshotState> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn is_ready(&self) -> bool {
        self.read().ready
    }

    pub fn get(&self, partner_id: &str) -> Option<Arc<Partner>> {
        let state = self.read();
        if !state.ready {
            return None;
        }
        state
            .partners
            .get(partner_id)
            .and_then(|entry| entry.partner.clone())
    }

    pub fn application(
        &self,
        partner_id: &str,
        application_id: &str,
    ) -> Option<(Arc<Partner>, Application)> {
        let partner = self.get(partner_id)?;
        let application = partner
            .applications
            .iter()
            .find(|a| a.application_id == application_id)?
            .clone();
        Some((partner, application))
    }

    pub(crate) fn install_bootstrap(&self, partners: HashMap<String, (i64, Option<Partner>)>) {
        let partners = partners
            .into_iter()
            .map(|(id, (config_version, partner))| {
                (
                    id,
                    VersionedPartner {
                        config_version,
                        partner: partner.map(Arc::new),
                    },
                )
            })
            .collect();
        let mut state = self.write();
        state.partners = partners;
        state.ready = true;
    }

    pub(crate) fn apply_active(&self, config_version: i64, partner: Partner) {
        let mut state = self.write();
        let should_apply = state
            .partners
            .get(&partner.partner_id)
            .is_none_or(|current| config_version > current.config_version);
        if should_apply {
            state.partners.insert(
                partner.partner_id.clone(),
                VersionedPartner {
                    config_version,
                    partner: Some(Arc::new(partner)),
                },
            );
        }
    }

    pub(crate) fn apply_archived(&self, config_version: i64, partner_id: &str) {
        let mut state = self.write();
        let should_apply = state
            .partners
            .get(partner_id)
            .is_none_or(|current| config_version > current.config_version);
        if should_apply {
            state.partners.insert(
                partner_id.to_string(),
                VersionedPartner {
                    config_version,
                    partner: None,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_partner() -> Partner {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config_schemas/examples/partner.valid.json");
        let json_str = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("не удалось прочитать {}: {e}", path.display()));
        serde_json::from_str(&json_str).expect("partner.schema.json форма")
    }

    #[test]
    fn loads_real_partner_fixture() {
        let partner = real_partner();
        assert_eq!(partner.partner_id, "click_uz");
        assert!(partner.is_active());
        assert_eq!(partner.applications.len(), 2);
    }

    #[test]
    fn snapshot_looks_up_application_by_partner_and_application_id() {
        let snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        let (partner, app) = snapshot
            .application("click_uz", "click_uz_main")
            .expect("должно найтись");
        assert_eq!(partner.partner_id, "click_uz");
        // 2000, не 300 — фикстура поднята веткой main (1500 TPS load-test push)
        // для нагрузочного тестирования, слияние веток сохранило это значение
        // как актуальное (subagent-1 эту строку вообще не трогал).
        assert_eq!(app.rate_limit_tps, 2000);
    }

    #[test]
    fn unknown_partner_or_application_returns_none() {
        let snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        assert!(snapshot.application("unknown", "click_uz_main").is_none());
        assert!(snapshot.application("click_uz", "unknown_app").is_none());
    }

    #[test]
    fn bootstrap_snapshot_is_fail_closed_until_kafka_replay_installed() {
        let snapshot = PartnerSnapshot::bootstrap_from_partners(vec![real_partner()]);
        assert!(!snapshot.is_ready());
        assert!(snapshot.get("click_uz").is_none());

        let partner = real_partner();
        snapshot.install_bootstrap(HashMap::from([(
            partner.partner_id.clone(),
            (42, Some(partner)),
        )]));
        assert!(snapshot.is_ready());
        assert!(snapshot.get("click_uz").is_some());
    }

    #[test]
    fn stale_config_version_cannot_overwrite_or_archive_newer_partner() {
        let snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        let mut newer = real_partner();
        newer.status = "suspended".into();
        snapshot.apply_active(10, newer);
        snapshot.apply_active(9, real_partner());
        assert_eq!(snapshot.get("click_uz").unwrap().status, "suspended");

        snapshot.apply_archived(9, "click_uz");
        assert!(snapshot.get("click_uz").is_some());
        snapshot.apply_archived(11, "click_uz");
        assert!(snapshot.get("click_uz").is_none());
    }
}
