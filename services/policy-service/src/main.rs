mod banwords;
mod config_reload;
mod health;
mod kafka_io;
mod offset_tracker;
mod policy_engine;
mod proto;
mod redis_url;
mod template_check;
mod template_matching;

use arc_swap::ArcSwap;
use config_reload::ConfigOverlay;
use health::HealthState;
use kafka_io::{MessageContextStore, RedisMessageContextStore};
use policy_engine::PolicyRulesetConfig;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use template_matching::Template;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let health_state = Arc::new(HealthState::default());
    let health_router = health::router(health_state.clone()).merge(template_check::router());
    let health_listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .expect("не удалось забиндить health-порт 9090");
    tokio::spawn(async move {
        axum::serve(health_listener, health_router).await.expect("health-сервер упал");
    });

    // В проде — ruleset/шаблоны строятся из config.changes (entity_type=policy_ruleset,
    // policy_template), не из локальных файлов; здесь заглушка на ту же форму,
    // что уже провалидирована в config_schemas/.
    let ruleset_path = std::env::var("POLICY_RULESET_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/policy_ruleset.valid.json".to_string());
    let ruleset_json = std::fs::read_to_string(&ruleset_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать ruleset {ruleset_path}: {e}"));
    let ruleset = PolicyRulesetConfig::from_config_schema_json(&ruleset_json);

    let template_path = std::env::var("POLICY_TEMPLATE_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/policy_template.valid.json".to_string());
    let template_json = std::fs::read_to_string(&template_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать template {template_path}: {e}"));
    let template_raw: serde_json::Value = serde_json::from_str(&template_json).expect("policy_template.schema.json форма");
    let template = Template {
        template_id: template_raw["template_id"].as_str().unwrap().to_string(),
        pattern: template_raw["pattern"].as_str().unwrap().to_string(),
        category: template_raw["category"].as_str().unwrap().to_string(),
        // Фаза 2 — `sender_id` не обязателен в policy_template.schema.json,
        // отсутствие/null означает "все отправители партнёра" (см. Template::sender_id).
        sender_id: template_raw["sender_id"].as_str().map(String::from),
    };
    // Статические файлы — только bootstrap-нулевая точка; живое состояние
    // дальше ведёт config_reload.rs (config.changes, entity_type=POLICY_RULESET
    // и POLICY_TEMPLATE) — то, что раньше было объявленным, но никогда не
    // подключённым arc-swap (см. README "Что НЕ реализовано").
    let overlay = Arc::new(ConfigOverlay::new(ruleset, vec![template]));
    let live_policy = Arc::new(ArcSwap::from_pointee(overlay.build_live_state()));

    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let consumer = kafka_io::build_consumer(&bootstrap_servers, "policy-service");
    let producer = kafka_io::build_producer(&bootstrap_servers);
    let config_consumer = config_reload::build_config_consumer(&bootstrap_servers, "policy-service-config");
    tokio::spawn(config_reload::run_loop(config_consumer, overlay, live_policy.clone()));

    let redis_runtime_url = redis_url::build_redis_runtime_url();
    let context_store: Arc<dyn MessageContextStore> = Arc::new(RedisMessageContextStore::new(&redis_runtime_url));

    kafka_io::run_loop(consumer, producer, context_store, live_policy).await;
}
