//! `config.changes` consumer — реальный hot-reload вместо статичных файлов,
//! запечённых в образ (см. destination-resolution-service/src/config_reload.rs
//! для полного обоснования паттерна). Отдельная consumer group от
//! `stage.policy` — независимые офсеты/партиционирование.
//!
//! Два entity_type в одном топике:
//! - POLICY_RULESET — единый глобальный слот (last-active-wins), не
//!   entity_id-keyed overlay: этот сервис не резолвит ruleset по партнёру/
//!   оператору (та же упрощённая семантика, что уже была у статического
//!   файла — см. `policy_engine::RawPolicyRulesetConfigChange`).
//! - POLICY_TEMPLATE — entity_id-keyed overlay (template_id), много записей
//!   (V022: 6323 реальных шаблонов) — тот же паттерн, что NUMBER_RANGE в
//!   destination-resolution-service.

use crate::banwords::BanwordChecker;
use crate::policy_engine::PolicyRulesetConfig;
use crate::proto::ConfigEntityType;
use crate::proto::events_v1::ConfigChangeEvent;
use crate::template_matching::{CompiledRuleset, Template};
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub const TOPIC: &str = "config.changes";

/// Всё, что нужно `handle_command` за один атомарный `ArcSwap::load` —
/// ruleset/templates/banwords меняются вместе (banwords производный от
/// ruleset.banwords, templates — от отдельного entity_type), поэтому один
/// снапшот на троих, не три независимых ArcSwap (иначе одно сообщение могло
/// бы увидеть новый ruleset, но ещё старые banwords в разных потоках).
pub struct PolicyLiveState {
    pub ruleset: PolicyRulesetConfig,
    pub templates: CompiledRuleset,
    pub banwords: BanwordChecker,
}

/// Одна запись `config.changes` (entity_type=POLICY_TEMPLATE) —
/// `config_schemas/policy_template.schema.json`, одна строка
/// `policy.policy_template`. `partner_id`/`operator_id`/`channel`/`version`
/// сознательно проигнорированы — этот сервис держит один общий реестр
/// шаблонов, не резолвит по партнёру/каналу (та же упрощённая семантика,
/// что уже была у статического файла). `sender_id` — ИСКЛЮЧЕНИЕ из этого
/// правила (Фаза 2 плана закрытия API-пробелов): в отличие от остальных
/// игнорируемых полей, он напрямую участвует в матчинге
/// (`Template::find_match`), не просто метаданные — `#[serde(default)]`,
/// т.к. это поле новее самого формата payload'а (nullable в схеме).
#[derive(Debug, Deserialize)]
struct TemplateConfigPayload {
    template_id: String,
    category: String,
    pattern: String,
    status: String, // "active" | "archived"
    #[serde(default)]
    sender_id: Option<String>,
}

/// Живое состояние policy-конфига поверх статических bootstrap-файлов —
/// arc-swap был объявлен в Cargo.toml, но никогда не подключался к
/// реальному Kafka-консьюмеру.
pub struct ConfigOverlay {
    base_ruleset: PolicyRulesetConfig,
    base_templates: Vec<Template>,
    ruleset_overlay: Mutex<Option<PolicyRulesetConfig>>,
    template_overlay: Mutex<HashMap<String, Template>>,
}

impl ConfigOverlay {
    pub fn new(base_ruleset: PolicyRulesetConfig, base_templates: Vec<Template>) -> Self {
        ConfigOverlay {
            base_ruleset,
            base_templates,
            ruleset_overlay: Mutex::new(None),
            template_overlay: Mutex::new(HashMap::new()),
        }
    }

    /// `status=archived` возвращает к статическому base ruleset — тот же
    /// принцип, что архивация одной записи overlay в других сервисах, только
    /// здесь "запись" одна на весь процесс.
    pub fn apply_ruleset(&self, payload_json: &[u8]) -> Result<(), serde_json::Error> {
        let (ruleset, status) = PolicyRulesetConfig::try_from_config_change_payload(payload_json)?;
        let mut overlay = self.ruleset_overlay.lock().expect("ruleset overlay mutex poisoned");
        *overlay = if status == "archived" { None } else { Some(ruleset) };
        Ok(())
    }

    fn apply_template(&self, entity_id: &str, payload: &TemplateConfigPayload) {
        let mut overlay = self.template_overlay.lock().expect("template overlay mutex poisoned");
        if payload.status == "archived" {
            overlay.remove(entity_id);
        } else {
            overlay.insert(
                entity_id.to_string(),
                Template {
                    template_id: payload.template_id.clone(),
                    pattern: payload.pattern.clone(),
                    category: payload.category.clone(),
                    sender_id: payload.sender_id.clone(),
                },
            );
        }
    }

    pub fn build_live_state(&self) -> PolicyLiveState {
        let ruleset = self
            .ruleset_overlay
            .lock()
            .expect("ruleset overlay mutex poisoned")
            .clone()
            .unwrap_or_else(|| self.base_ruleset.clone());
        let banwords = BanwordChecker::new(&ruleset.banwords);

        let template_overlay = self.template_overlay.lock().expect("template overlay mutex poisoned");
        let mut templates: Vec<Template> = template_overlay.values().cloned().collect();
        templates.extend(self.base_templates.iter().cloned());
        let templates = CompiledRuleset::new(templates);

        PolicyLiveState { ruleset, templates, banwords }
    }
}

/// `auto.offset.reset=earliest` — compacted-топик, на каждом свежем старте
/// нужна вся история активных entity_id, не только новые события "с этого
/// момента".
/// Реальная находка (нагрузочный прогон + прямая проверка живого процесса,
/// не гипотетическая): раньше `group_id` был ФИКСИРОВАННЫМ и переживал
/// рестарт процесса — Kafka хранит committed offset за consumer group
/// независимо от того, жив ли сам процесс. `auto.offset.reset=earliest`
/// применяется ТОЛЬКО если для группы ещё нет закоммиченного offset —
/// на любом рестарте ПОСЛЕ первого свежий процесс просто резюмировал с уже
/// прочитанной (закоммиченной прошлым процессом) позиции, никогда не
/// переигрывая историю `config.changes` заново. In-memory `ConfigOverlay`
/// при этом каждый раз стартует ПУСТЫМ (это не персистентное состояние) —
/// итог: hot-reload конфиг (например `tpl-hotreload-test`) применялся РОВНО
/// один раз, при самом первом запуске, и молча терялся на любом следующем
/// рестарте — offset коммитился нормально (lag=0 в `kafka-consumer-groups`,
/// никаких ошибок в логах), событие просто больше никогда не доставлялось
/// заново. Единственный корректный способ гарантированно переиграть ВЕСЬ
/// compacted-топик на каждом старте — уникальный `group_id` на каждый
/// процесс, чтобы `auto.offset.reset=earliest` реально срабатывал каждый раз.
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

/// Ядро обработки одного `ConfigChangeEvent` — тестируется без брокера.
/// Возвращает `true`, если событие относилось к POLICY_RULESET/POLICY_TEMPLATE
/// и применилось успешно (снапшот нужно пересобрать).
pub fn handle_config_change(overlay: &ConfigOverlay, event: &ConfigChangeEvent) -> bool {
    if event.entity_type == ConfigEntityType::PolicyRuleset as i32 {
        match overlay.apply_ruleset(&event.payload_json) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(
                    "config.changes: entity_id={} payload_json не парсится как policy_ruleset: {e}",
                    event.entity_id
                );
                false
            }
        }
    } else if event.entity_type == ConfigEntityType::PolicyTemplate as i32 {
        match serde_json::from_slice::<TemplateConfigPayload>(&event.payload_json) {
            Ok(payload) => {
                overlay.apply_template(&event.entity_id, &payload);
                true
            }
            Err(e) => {
                tracing::error!(
                    "config.changes: entity_id={} payload_json не парсится как policy_template: {e}",
                    event.entity_id
                );
                false
            }
        }
    } else {
        false
    }
}

pub async fn run_loop(consumer: StreamConsumer, overlay: Arc<ConfigOverlay>, live: Arc<ArcSwap<PolicyLiveState>>) {
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
                        tracing::error!("не удалось декодировать ConfigChangeEvent: {e}");
                        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                        continue;
                    }
                };

                if handle_config_change(&overlay, &event) {
                    live.store(Arc::new(overlay.build_live_state()));
                    tracing::info!(
                        "config.changes: policy обновлён (entity_type={}, entity_id={}, status={}) — снапшот пересобран",
                        event.entity_type,
                        event.entity_id,
                        event.status
                    );
                }

                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset (config.changes): {e}");
                }
            }
            Err(e) => tracing::error!("ошибка Kafka consumer (config.changes): {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_ruleset() -> PolicyRulesetConfig {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config_schemas/examples/policy_ruleset.valid.json");
        let json_str = std::fs::read_to_string(&path).unwrap();
        PolicyRulesetConfig::from_config_schema_json(&json_str)
    }

    fn base_template() -> Template {
        Template {
            template_id: "tpl-contract-payment".to_string(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".to_string(),
            category: "TRANSACTION".to_string(),
            sender_id: None,
        }
    }

    fn ruleset_event(status: &str) -> ConfigChangeEvent {
        let payload = serde_json::json!({
            "partner_id": null,
            "operator_id": null,
            "version": 2,
            "status": status,
            "unmatched_template_behavior": "REJECT",
            "anti_spam": {"max_messages": 1, "window_seconds": 60, "scope": "PER_MSISDN"},
            "time_of_day": [],
            "sender_validation": {"allowed_sender_ids": ["OnlyThis"]},
            "banwords": {"words": [], "normalization": {"nfkc": true, "homoglyph_fold": true, "strip_separators": true}},
        });
        ConfigChangeEvent {
            entity_type: ConfigEntityType::PolicyRuleset as i32,
            entity_id: "global".to_string(),
            version: 2,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            status: status.to_string(),
            created_at: None,
        }
    }

    fn template_event(entity_id: &str, category: &str, pattern: &str, status: &str) -> ConfigChangeEvent {
        let payload = serde_json::json!({
            "template_id": entity_id,
            "partner_id": "demo_partner",
            "operator_id": null,
            "channel": "SMS",
            "category": category,
            "pattern": pattern,
            "version": 1,
            "status": status,
        });
        ConfigChangeEvent {
            entity_type: ConfigEntityType::PolicyTemplate as i32,
            entity_id: entity_id.to_string(),
            version: 1,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            status: status.to_string(),
            created_at: None,
        }
    }

    fn overlay() -> ConfigOverlay {
        ConfigOverlay::new(base_ruleset(), vec![base_template()])
    }

    #[test]
    fn starts_out_matching_the_static_baseline() {
        let overlay = overlay();
        let state = overlay.build_live_state();
        assert!(state.templates.find_match("Hello shartnoma bo'yicha 123 so'm to'lovni bugun amalga oshiring", "any-sender").is_some());
    }

    #[test]
    fn ruleset_event_replaces_the_active_ruleset() {
        let overlay = overlay();
        assert!(handle_config_change(&overlay, &ruleset_event("active")));
        let state = overlay.build_live_state();
        assert_eq!(state.ruleset.allowed_sender_ids.len(), 1);
        assert!(state.ruleset.allowed_sender_ids.contains("OnlyThis"));
    }

    #[test]
    fn ruleset_event_archived_reverts_to_static_baseline() {
        let overlay = overlay();
        handle_config_change(&overlay, &ruleset_event("active"));
        assert!(overlay.build_live_state().ruleset.allowed_sender_ids.contains("OnlyThis"));

        handle_config_change(&overlay, &ruleset_event("archived"));
        let state = overlay.build_live_state();
        assert!(!state.ruleset.allowed_sender_ids.contains("OnlyThis"), "archived должен вернуть статический base ruleset");
    }

    #[test]
    fn template_event_adds_a_new_matchable_template() {
        let overlay = overlay();
        let event = template_event("tpl-new", "SERVICE", "%w promo kod faollashtirildi", "active");
        assert!(handle_config_change(&overlay, &event));
        let state = overlay.build_live_state();
        let matched = state.templates.find_match("Hello promo kod faollashtirildi", "any-sender").unwrap();
        assert_eq!(matched.category, "SERVICE");
    }

    /// Реальная воспроизводящая проверка (нагрузочный прогон 1000 msg/s):
    /// байт-в-байт то же `ConfigChangeEvent`, что реально лежит в
    /// `config.changes` (снято через `examples/decode_config.rs`), плюс
    /// РЕАЛЬНЫЙ msgctx.body одного из реально провалившихся сообщений
    /// (взят напрямую из Runtime Redis) — если это пройдёт, баг не в
    /// алгоритме матчинга/оверлея самом по себе, а где-то в реальном
    /// процессе (порядок обработки config.changes vs stage.policy,
    /// множественный экземпляр ConfigOverlay, и т.п.).
    #[test]
    fn reproduces_real_kafka_event_against_real_failing_message_body() {
        let overlay = overlay();
        let real_event = ConfigChangeEvent {
            entity_type: 3, // CONFIG_ENTITY_TYPE_POLICY_TEMPLATE — подтверждено examples/decode_config.rs
            entity_id: "tpl-hotreload-test".to_string(),
            version: 0,
            payload_json: br#"{"status": "active", "channel": "SMS", "pattern": "%w promo kod faollashtirildi", "version": 1, "category": "SERVICE", "partner_id": "click_uz", "operator_id": null, "template_id": "tpl-hotreload-test"}"#.to_vec(),
            status: "active".to_string(),
            created_at: None,
        };
        assert!(handle_config_change(&overlay, &real_event), "handle_config_change должен вернуть true для реального события");
        let state = overlay.build_live_state();
        let real_body = "Promo5789 promo kod faollashtirildi"; // реально из msgctx:38dbc4b4-... в Runtime Redis
        let matched = state.templates.find_match(real_body, "any-sender");
        assert!(matched.is_some(), "реальное тело реально провалившегося сообщения должно матчиться, получили {matched:?}");
        assert_eq!(matched.unwrap().category, "SERVICE");
    }

    #[test]
    fn template_event_archived_removes_it_but_keeps_base_template() {
        let overlay = overlay();
        let event = template_event("tpl-new", "SERVICE", "%w promo kod faollashtirildi", "active");
        handle_config_change(&overlay, &event);
        assert!(overlay.build_live_state().templates.find_match("Hello promo kod faollashtirildi", "any-sender").is_some());

        handle_config_change(&overlay, &template_event("tpl-new", "SERVICE", "%w promo kod faollashtirildi", "archived"));
        let state = overlay.build_live_state();
        assert!(state.templates.find_match("Hello promo kod faollashtirildi", "any-sender").is_none());
        // Базовый (статический) шаблон остаётся нетронутым.
        assert!(state.templates.find_match("Hello shartnoma bo'yicha 123 so'm to'lovni bugun amalga oshiring", "any-sender").is_some());
    }

    #[test]
    fn ignores_events_for_other_entity_types() {
        let overlay = overlay();
        let mut event = ruleset_event("active");
        event.entity_type = ConfigEntityType::NumberRange as i32;
        assert!(!handle_config_change(&overlay, &event));
    }

    #[test]
    fn malformed_ruleset_payload_is_rejected_not_panicking() {
        let overlay = overlay();
        let mut event = ruleset_event("active");
        event.payload_json = b"{not valid json".to_vec();
        assert!(!handle_config_change(&overlay, &event));
    }

    #[test]
    fn malformed_template_payload_is_rejected_not_panicking() {
        let overlay = overlay();
        let mut event = template_event("tpl-new", "SERVICE", "%w x", "active");
        event.payload_json = b"{not valid json".to_vec();
        assert!(!handle_config_change(&overlay, &event));
    }
}
