//! Партнёрский конфиг-снапшот — `config_schemas/partner.schema.json`.
//!
//! BACKOFFICE_ROADMAP.md "Production Readiness Review" P0 #4: раньше
//! снапшот грузился РОВНО ОДИН РАЗ при старте из статического файла
//! (`PARTNER_CONFIG_PATH`) и никогда не обновлялся. Теперь (см.
//! `config_source.rs`/`config_reload.rs`) снапшот либо грузится один раз
//! из статического файла (`PARTNER_CONFIG_PATH`, явно заданный — local dev
//! без Configuration Redis), либо строится bootstrap-чтением из
//! Configuration Redis + живьём обновляется через `config.changes`-
//! консьюмер — тот же
//! `config:current:partner:{id}`/`config:version:partner:{id}:{version}`,
//! который уже пишет `config-cache-projector`, реально работающий в
//! проде. Тот же дизайн, что Go-сиблинг
//! `services/partner-notification-service/internal/config` (тот же P0 #4
//! пункт, оба сервиса читают тот же ключевой формат).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Serialize (в дополнение к Deserialize) — нужен только config_reload.rs'
// тестам, которые сами засеивают Configuration Redis реальным JSON перед
// чтением через config_source::fetch_partner (round-trip тест, не мок).
// Не используется на настоящем пути записи — payload_json туда пишет
// configuration-service (не этот сервис).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub partner_id: String,
    // Читается config_reload.rs для логирования (какая версия только что
    // стала живой) — не участвует в сравнении/дедупликации версий здесь:
    // Configuration Redis (config:current:partner:{id}) уже решает, какая
    // версия "текущая", это поле просто отражает то, что вернул fetch_partner.
    pub version: u64,
    pub status: String,
    pub applications: Vec<Application>,
}

impl Partner {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }

    /// `IsArchived` (Go-сиблинг, `internal/config/partner.go`): партнёр,
    /// снятый с обслуживания целиком — не `"suspended"` (тот
    /// временный/обратимый, partner_id остаётся резолвимым, чтобы запросы
    /// получали осмысленный отказ через обычную auth/rate-limit цепочку,
    /// а не "неизвестный партнёр"). Только `archived` означает "удалить
    /// из живого снапшота", см. `config_reload.rs::handle_config_change`.
    pub fn is_archived(&self) -> bool {
        self.status == "archived"
    }
}

/// Снапшот по всем партнёрам — в этом срезе строится из одного файла с одним
/// партнёром (Фаза 2.2, тот же паттерн упрощения, что у Routing/Policy),
/// но структура (`HashMap` по `partner_id`) уже общая, рассчитана на
/// множество партнёров.
///
/// `Clone` — нужен для `ArcSwap`-паттерна (`config_reload.rs`): каждое
/// живое обновление публикует НОВЫЙ `Arc<PartnerSnapshot>` (copy-on-write
/// на уровне всей структуры, `HashMap::clone` — партнёров в этом срезе
/// мало, полное клонирование на upsert/remove проще и безопаснее, чем
/// частичная мутация, и не на хот-пасе запроса).
#[derive(Debug, Default, Clone)]
pub struct PartnerSnapshot {
    partners: HashMap<String, Partner>,
}

impl PartnerSnapshot {
    pub fn from_partners(partners: Vec<Partner>) -> Self {
        Self { partners: partners.into_iter().map(|p| (p.partner_id.clone(), p)).collect() }
    }

    pub fn get(&self, partner_id: &str) -> Option<&Partner> {
        self.partners.get(partner_id)
    }

    pub fn application<'a>(&'a self, partner_id: &str, application_id: &str) -> Option<(&'a Partner, &'a Application)> {
        let partner = self.get(partner_id)?;
        let application = partner.applications.iter().find(|a| a.application_id == application_id)?;
        Some((partner, application))
    }

    pub fn len(&self) -> usize {
        self.partners.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partners.is_empty()
    }

    /// Публикует/заменяет одного партнёра в снапшоте (add-or-replace by
    /// `partner_id`) — используется по `config_reload.rs` для
    /// active/suspended-обновлений с `config.changes`.
    pub fn upsert(&mut self, partner: Partner) {
        self.partners.insert(partner.partner_id.clone(), partner);
    }

    /// Убирает партнёра из снапшота целиком — archived-статус или
    /// отсутствие в Configuration Redis (см. `config_reload.rs`). No-op,
    /// если `partner_id` уже отсутствует.
    pub fn remove(&mut self, partner_id: &str) {
        self.partners.remove(partner_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_partner() -> Partner {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config_schemas/examples/partner.valid.json");
        let json_str = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("не удалось прочитать {}: {e}", path.display()));
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
        let (partner, app) = snapshot.application("click_uz", "click_uz_main").expect("должно найтись");
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
    fn archived_status_is_distinct_from_active_and_suspended() {
        let mut partner = real_partner();
        assert!(!partner.is_archived());
        partner.status = "suspended".to_string();
        assert!(!partner.is_archived(), "suspended остаётся резолвимым, не archived");
        partner.status = "archived".to_string();
        assert!(partner.is_archived());
        assert!(!partner.is_active());
    }

    #[test]
    fn upsert_adds_new_partner_without_affecting_others() {
        let mut snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        let mut second = real_partner();
        second.partner_id = "beta".to_string();
        second.applications[0].application_id = "beta_app".to_string();

        snapshot.upsert(second);

        assert!(snapshot.application("click_uz", "click_uz_main").is_some());
        assert!(snapshot.application("beta", "beta_app").is_some());
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn upsert_replaces_existing_partner_version() {
        let mut snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        let mut updated = real_partner();
        updated.version = 99;
        updated.applications[0].rate_limit_tps = 1;

        snapshot.upsert(updated);

        let (partner, app) = snapshot.application("click_uz", "click_uz_main").expect("должно найтись");
        assert_eq!(partner.version, 99);
        assert_eq!(app.rate_limit_tps, 1);
        assert_eq!(snapshot.len(), 1, "upsert существующего partner_id должен заменить, не задублировать");
    }

    #[test]
    fn remove_drops_partner_from_live_map() {
        let mut snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        snapshot.remove("click_uz");
        assert!(snapshot.application("click_uz", "click_uz_main").is_none());
        assert!(snapshot.is_empty());
    }

    #[test]
    fn remove_unknown_partner_is_noop() {
        let mut snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        snapshot.remove("unknown");
        assert_eq!(snapshot.len(), 1);
    }
}
