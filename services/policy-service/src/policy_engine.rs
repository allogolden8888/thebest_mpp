//! Порт `policy_matching/policy_engine.py` — оркестрация всех восьми
//! проверок в том же порядке зависимостей (`service_internal_methods.md`
//! §1.5): 7 validate_sender -> 6 check_sender_blacklist -> 1 match_template/
//! resolve_unmatched_behavior -> 5 check_banwords -> 4 check_category_blacklist
//! -> 3 check_time_of_day -> 2 check_spam_frequency (increment только если
//! сообщение реально прошло).
//!
//! `RuntimeState` — та же заглушка Runtime Redis, что в Python-версии;
//! `PolicyRulesetConfig::from_config_schema_json` парсит ровно ту форму,
//! что в `config_schemas/policy_ruleset.schema.json`, тесты грузят тот же
//! реальный `config_schemas/examples/policy_ruleset.valid.json` — та же
//! кросс-артефактная проверка согласованности, что в Python-оркестраторе.

use crate::banwords::BanwordChecker;
use crate::template_matching::CompiledRuleset;
use chrono::{NaiveDateTime, NaiveTime};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct MessageContext {
    pub msisdn: String,
    pub sender_id: String,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum UnmatchedTemplateBehavior {
    CategorizeAsUntemplated,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AntiSpamScope {
    PerMsisdn,
    PerMsisdnPerCategory,
}

#[derive(Debug, Clone)]
pub struct PolicyRulesetConfig {
    pub unmatched_template_behavior: UnmatchedTemplateBehavior,
    pub anti_spam_max_messages: usize,
    pub anti_spam_window_seconds: i64,
    pub anti_spam_scope: AntiSpamScope,
    pub time_of_day: HashMap<String, (NaiveTime, NaiveTime)>,
    pub allowed_sender_ids: HashSet<String>,
    pub banwords: Vec<String>,
}

#[derive(Deserialize)]
struct RawAntiSpam {
    max_messages: usize,
    window_seconds: i64,
    scope: String,
}
#[derive(Deserialize)]
struct RawTimeOfDayEntry {
    category: String,
    allowed_from: String,
    allowed_to: String,
}
#[derive(Deserialize)]
struct RawSenderValidation {
    allowed_sender_ids: Vec<String>,
}
#[derive(Deserialize)]
struct RawBanwords {
    words: Vec<String>,
}
#[derive(Deserialize)]
struct RawPolicyRuleset {
    unmatched_template_behavior: String,
    anti_spam: RawAntiSpam,
    time_of_day: Vec<RawTimeOfDayEntry>,
    sender_validation: RawSenderValidation,
    banwords: RawBanwords,
}

/// Форма ровно `config_schemas/policy_ruleset.schema.json` целиком — в
/// отличие от [`RawPolicyRuleset`] (только поля, нужные статическому
/// bootstrap-файлу), несёт `status`, нужный, чтобы отличить `archived` от
/// `active` при живом `config.changes`-событии. `partner_id`/`operator_id`
/// сознательно проигнорированы (`#[serde(default)]`, не читаются) — этот
/// сервис не резолвит ruleset по партнёру/оператору, тот же единый
/// глобальный ruleset, что уже был у статического файла (см. README) —
/// hot-reload не расширяет эту упрощённую семантику, только подключает её
/// к реальному Kafka вместо запечённого в образ файла.
#[derive(Deserialize)]
struct RawPolicyRulesetConfigChange {
    #[serde(default)]
    #[allow(dead_code)]
    partner_id: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    operator_id: Option<String>,
    status: String,
    unmatched_template_behavior: String,
    anti_spam: RawAntiSpam,
    time_of_day: Vec<RawTimeOfDayEntry>,
    sender_validation: RawSenderValidation,
    banwords: RawBanwords,
}

fn convert_time_of_day(entries: Vec<RawTimeOfDayEntry>) -> HashMap<String, (NaiveTime, NaiveTime)> {
    entries
        .into_iter()
        .map(|e| {
            let parse = |s: &str| NaiveTime::parse_from_str(s, "%H:%M").expect("HH:MM");
            (e.category, (parse(&e.allowed_from), parse(&e.allowed_to)))
        })
        .collect()
}

fn convert_unmatched_template_behavior(s: &str) -> UnmatchedTemplateBehavior {
    match s {
        "CATEGORIZE_AS_UNTEMPLATED" => UnmatchedTemplateBehavior::CategorizeAsUntemplated,
        "REJECT" => UnmatchedTemplateBehavior::Reject,
        other => panic!("неизвестный unmatched_template_behavior: {other}"),
    }
}

fn convert_anti_spam_scope(s: &str) -> AntiSpamScope {
    match s {
        "PER_MSISDN" => AntiSpamScope::PerMsisdn,
        "PER_MSISDN_PER_CATEGORY" => AntiSpamScope::PerMsisdnPerCategory,
        other => panic!("неизвестный anti_spam.scope: {other}"),
    }
}

impl PolicyRulesetConfig {
    pub fn from_config_schema_json(json_str: &str) -> Self {
        let raw: RawPolicyRuleset = serde_json::from_str(json_str).expect("policy_ruleset.schema.json форма");
        Self {
            unmatched_template_behavior: convert_unmatched_template_behavior(&raw.unmatched_template_behavior),
            anti_spam_max_messages: raw.anti_spam.max_messages,
            anti_spam_window_seconds: raw.anti_spam.window_seconds,
            anti_spam_scope: convert_anti_spam_scope(&raw.anti_spam.scope),
            time_of_day: convert_time_of_day(raw.time_of_day),
            allowed_sender_ids: raw.sender_validation.allowed_sender_ids.into_iter().collect(),
            banwords: raw.banwords.words,
        }
    }

    /// `config.changes` (entity_type=POLICY_RULESET) — в отличие от
    /// [`Self::from_config_schema_json`] (bootstrap, `panic!` на кривом
    /// файле — оправдано, сервис не должен стартовать с плохим конфигом),
    /// здесь `Result`: одно кривое Kafka-сообщение не должно ронять уже
    /// работающий процесс. Возвращает `(config, status)` — вызывающая
    /// сторона (`config_reload.rs`) решает, что делать с `archived`.
    pub fn try_from_config_change_payload(payload_json: &[u8]) -> Result<(Self, String), serde_json::Error> {
        let raw: RawPolicyRulesetConfigChange = serde_json::from_slice(payload_json)?;
        Ok((
            Self {
                unmatched_template_behavior: convert_unmatched_template_behavior(&raw.unmatched_template_behavior),
                anti_spam_max_messages: raw.anti_spam.max_messages,
                anti_spam_window_seconds: raw.anti_spam.window_seconds,
                anti_spam_scope: convert_anti_spam_scope(&raw.anti_spam.scope),
                time_of_day: convert_time_of_day(raw.time_of_day),
                allowed_sender_ids: raw.sender_validation.allowed_sender_ids.into_iter().collect(),
                banwords: raw.banwords.words,
            },
            raw.status,
        ))
    }
}

/// spam_key — (msisdn, Option<category>): None для PER_MSISDN, Some для PER_MSISDN_PER_CATEGORY.
type SpamKey = (String, Option<String>);

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub sender_blacklist: HashSet<(String, String)>,
    pub category_blacklist: HashSet<(String, String)>,
    pub spam_history: HashMap<SpamKey, Vec<NaiveDateTime>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyOutcome {
    pub outcome: &'static str, // "SUCCEEDED" | "REJECTED"
    pub category: String,
    pub reason_code: Option<&'static str>,
}

pub fn evaluate_policy(
    ctx: &MessageContext,
    ruleset: &PolicyRulesetConfig,
    templates: &CompiledRuleset,
    banword_checker: &BanwordChecker,
    runtime: &mut RuntimeState,
    now: NaiveDateTime,
) -> PolicyOutcome {
    // Требование 7 — validate_sender
    if !ruleset.allowed_sender_ids.is_empty() && !ruleset.allowed_sender_ids.contains(&ctx.sender_id) {
        return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("INVALID_SENDER") };
    }

    // Требование 6 — check_sender_blacklist
    if runtime.sender_blacklist.contains(&(ctx.msisdn.clone(), ctx.sender_id.clone())) {
        return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("SENDER_BLACKLISTED") };
    }

    // Требование 1 — match_template / resolve_unmatched_behavior
    let category = match templates.find_match(&ctx.body, &ctx.sender_id) {
        Some(matched) => matched.category,
        None => match ruleset.unmatched_template_behavior {
            UnmatchedTemplateBehavior::Reject => {
                return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("NO_TEMPLATE_MATCH") };
            }
            UnmatchedTemplateBehavior::CategorizeAsUntemplated => "UNTEMPLATED".to_string(),
        },
    };

    // Требование 5 — check_banwords. После определения category, но НЕ зависит
    // от результата match_template — %w пермиссивен структурно, не по содержимому.
    if !banword_checker.find_hits(&ctx.body).is_empty() {
        return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("BANWORD_DETECTED") };
    }

    // Требование 4 — check_category_blacklist
    if runtime.category_blacklist.contains(&(ctx.msisdn.clone(), category.clone())) {
        return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("CATEGORY_BLACKLISTED") };
    }

    // Требование 3 — check_time_of_day
    if let Some((start, end)) = ruleset.time_of_day.get(&category) {
        let t = now.time();
        if !(*start <= t && t <= *end) {
            return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("OUTSIDE_TIME_WINDOW") };
        }
    }

    // Требование 2 — check_spam_frequency
    let spam_key: SpamKey = match ruleset.anti_spam_scope {
        AntiSpamScope::PerMsisdn => (ctx.msisdn.clone(), None),
        AntiSpamScope::PerMsisdnPerCategory => (ctx.msisdn.clone(), Some(category.clone())),
    };
    let window_start = now - chrono::Duration::seconds(ruleset.anti_spam_window_seconds);
    let mut recent: Vec<NaiveDateTime> =
        runtime.spam_history.get(&spam_key).map(|v| v.iter().copied().filter(|t| *t >= window_start).collect()).unwrap_or_default();
    if recent.len() >= ruleset.anti_spam_max_messages {
        return PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("SPAM_THROTTLED") };
    }

    // increment_spam_counter — только для сообщений, реально прошедших все проверки.
    recent.push(now);
    runtime.spam_history.insert(spam_key, recent);

    PolicyOutcome { outcome: "SUCCEEDED", category, reason_code: None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template_matching::Template;
    use chrono::NaiveDate;
    use std::path::Path;

    fn real_template() -> Template {
        Template {
            template_id: "tpl-contract-payment".to_string(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".to_string(),
            category: "TRANSACTION".to_string(),
            sender_id: None,
        }
    }

    fn load_real_ruleset() -> PolicyRulesetConfig {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config_schemas/examples/policy_ruleset.valid.json");
        let json_str = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("не удалось прочитать {}: {e}", path.display()));
        PolicyRulesetConfig::from_config_schema_json(&json_str)
    }

    fn fresh_env() -> (PolicyRulesetConfig, CompiledRuleset, BanwordChecker, RuntimeState) {
        let ruleset = load_real_ruleset();
        let templates = CompiledRuleset::new(vec![real_template()]);
        let banwords = BanwordChecker::new(&ruleset.banwords);
        let runtime = RuntimeState::default();
        (ruleset, templates, banwords, runtime)
    }

    fn dt(h: u32, m: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 7, 22).unwrap().and_hms_opt(h, m, 0).unwrap()
    }

    fn ctx(sender_id: &str, body: &str) -> MessageContext {
        MessageContext { msisdn: "998901331835".to_string(), sender_id: sender_id.to_string(), body: body.to_string() }
    }

    const TPL_BODY: &str = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring";

    #[test]
    fn happy_path_matched_template() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        let result = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "SUCCEEDED", category: "TRANSACTION".into(), reason_code: None });
    }

    #[test]
    fn invalid_sender_rejected_before_anything_else() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        let result = evaluate_policy(&ctx("NotClick", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("INVALID_SENDER") });
    }

    #[test]
    fn sender_blacklisted() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        runtime.sender_blacklist.insert(("998901331835".to_string(), "Click".to_string()));
        let result = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("SENDER_BLACKLISTED") });
    }

    #[test]
    fn unmatched_template_categorized_as_untemplated() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        let result = evaluate_policy(&ctx("Click", "совершенно другой текст без шаблона"), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "SUCCEEDED", category: "UNTEMPLATED".into(), reason_code: None });
    }

    #[test]
    fn banword_blocks_even_though_template_matches() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        let body = "idiot shartnoma bo'yicha 123456 so'm to'lovni bugun amalga oshiring";
        let result = evaluate_policy(&ctx("Click", body), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("BANWORD_DETECTED") });
    }

    #[test]
    fn category_blacklisted() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        runtime.category_blacklist.insert(("998901331835".to_string(), "TRANSACTION".to_string()));
        let result = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, dt(12, 0));
        assert_eq!(result, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("CATEGORY_BLACKLISTED") });
    }

    #[test]
    fn outside_time_window() {
        let (ruleset, _, banwords, mut runtime) = fresh_env();
        let ad_template = Template { template_id: "tpl-ads".into(), pattern: "%w reklama".into(), category: "ADVERTISING".into(), sender_id: None };
        let templates_with_ads = CompiledRuleset::new(vec![real_template(), ad_template]);
        let advert_ctx = ctx("Click", "SuperSale reklama");

        // policy_ruleset.valid.json: ADVERTISING разрешена 09:00-20:00 Asia/Tashkent.
        let result_night = evaluate_policy(&advert_ctx, &ruleset, &templates_with_ads, &banwords, &mut runtime, dt(23, 0));
        assert_eq!(result_night, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("OUTSIDE_TIME_WINDOW") });

        let result_day = evaluate_policy(&advert_ctx, &ruleset, &templates_with_ads, &banwords, &mut runtime, dt(10, 0));
        assert_eq!(result_day, PolicyOutcome { outcome: "SUCCEEDED", category: "ADVERTISING".into(), reason_code: None });
    }

    #[test]
    fn spam_throttled_after_limit_and_counter_not_incremented_on_rejects() {
        let (ruleset, templates, banwords, mut runtime) = fresh_env();
        let base = dt(12, 0);

        // policy_ruleset.valid.json: max_messages=3, window_seconds=60, PER_MSISDN_PER_CATEGORY.
        for i in 0..3 {
            let now = base + chrono::Duration::seconds(i);
            let result = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, now);
            assert_eq!(result.outcome, "SUCCEEDED", "сообщение {i} должно пройти (в пределах лимита 3)");
        }

        let fourth = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, base + chrono::Duration::seconds(3));
        assert_eq!(fourth, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("SPAM_THROTTLED") });

        // Заблокированный отправитель не должен занимать место в spam-окне.
        runtime.sender_blacklist.insert(("998901331835".to_string(), "Click".to_string()));
        let rejected_by_sender = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, base + chrono::Duration::seconds(4));
        assert_eq!(rejected_by_sender, PolicyOutcome { outcome: "REJECTED", category: "BLOCKED".into(), reason_code: Some("SENDER_BLACKLISTED") });
        runtime.sender_blacklist.clear();

        // После истечения окна (60с) лимит должен сброситься.
        let after_window = evaluate_policy(&ctx("Click", TPL_BODY), &ruleset, &templates, &banwords, &mut runtime, base + chrono::Duration::seconds(61));
        assert_eq!(after_window.outcome, "SUCCEEDED", "окно анти-спама истекло — счётчик должен сброситься");
    }
}
