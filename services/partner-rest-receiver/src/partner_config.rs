//! Партнёрский конфиг-снапшот — `config_schemas/partner.schema.json`. В проде
//! это `config.changes` (entity_type=PARTNER), спроецированный в локальный
//! in-process snapshot (HLD §16); здесь, как и у остальных сервисов этого
//! среза (Routing/Policy — их README), снапшот грузится один раз при
//! старте из статического файла, hot-reload не реализован.

use serde::Deserialize;
use std::collections::HashMap;

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
    // Не читается — конфиг грузится один раз из статического файла в этом
    // срезе (hot-reload/дедупликация версий из `config.changes` не
    // реализованы, см. README), некому сравнивать версии.
    #[allow(dead_code)]
    pub version: u64,
    pub status: String,
    pub applications: Vec<Application>,
}

impl Partner {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }
}

/// Снапшот по всем партнёрам — в этом срезе строится из одного файла с одним
/// партнёром (Фаза 2.2, тот же паттерн упрощения, что у Routing/Policy),
/// но структура (`HashMap` по `partner_id`) уже общая, рассчитана на
/// множество партнёров.
#[derive(Debug, Default)]
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
        assert_eq!(app.rate_limit_tps, 300);
    }

    #[test]
    fn unknown_partner_or_application_returns_none() {
        let snapshot = PartnerSnapshot::from_partners(vec![real_partner()]);
        assert!(snapshot.application("unknown", "click_uz_main").is_none());
        assert!(snapshot.application("click_uz", "unknown_app").is_none());
    }
}
