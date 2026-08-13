mod build_stage_execute;
mod config_reload;
mod execution_state;
mod health;
mod kafka_io;
mod offset_tracker;
mod pipeline_graph;
mod proto;
mod redis_cas;
mod redis_url;

use arc_swap::ArcSwap;
use health::HealthState;
use pipeline_graph::{ConfigOverlay, PipelineDefinition};
use redis_cas::RedisStateStore;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let health_state = Arc::new(HealthState::default());
    let health_router = health::router(health_state.clone());
    let health_listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .expect("не удалось забиндить health-порт 9090");
    tokio::spawn(async move {
        axum::serve(health_listener, health_router).await.expect("health-сервер упал");
    });

    // В проде — resolve_pipeline_version(partner_id, application_id, snapshot) из
    // config.changes; здесь один тестовый пайплайн (Фаза 2.2), тот же файл, что
    // уже провалидирован в config_schemas/.
    let pipeline_path = std::env::var("PIPELINE_DEFINITION_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/pipeline.valid.json".to_string());
    let json = std::fs::read_to_string(&pipeline_path).unwrap_or_else(|e| panic!("не удалось прочитать {pipeline_path}: {e}"));
    let pipeline: PipelineDefinition = serde_json::from_str(&json).expect("pipeline.schema.json форма");

    // Статический файл — только bootstrap-нулевая точка; живое состояние
    // дальше ведёт config_reload.rs (config.changes, entity_type=PIPELINE)
    // — то, что раньше было даже не объявленной Cargo.toml-зависимостью, не
    // то что подключённым arc-swap (см. README "Что НЕ реализовано").
    let overlay = Arc::new(ConfigOverlay::new(pipeline));
    let live_pipeline = Arc::new(ArcSwap::from_pointee(overlay.current()));

    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let producer = kafka_io::build_producer(&bootstrap_servers);

    // development_plan.md 4.2 — Runtime Redis CAS заменяет in-memory
    // Arc<Mutex<HashMap>>, снимает блокер "не работает с более чем одной
    // репликой", см. src/redis_cas.rs.
    let store = Arc::new(RedisStateStore::new(&redis_url::build_redis_runtime_url()).expect("не удалось создать Runtime Redis клиент"));

    let incoming_consumer = kafka_io::build_consumer(&bootstrap_servers, "pipeline-engine", kafka_io::INCOMING_TOPIC);
    let completed_consumer = kafka_io::build_consumer(&bootstrap_servers, "pipeline-engine", kafka_io::COMPLETED_TOPIC);
    // Отдельная consumer group от incoming/completed — независимые офсеты,
    // редкий (только при реальном retry) трафик не должен делить партиции/
    // ребалансы с основными двумя циклами.
    let retry_trigger_consumer =
        kafka_io::build_consumer(&bootstrap_servers, "pipeline-engine-retry-trigger", kafka_io::PIPELINE_RETRY_TRIGGERS_TOPIC);
    let config_consumer = config_reload::build_config_consumer(&bootstrap_servers, "pipeline-engine-config");
    tokio::spawn(config_reload::run_loop(config_consumer, overlay, live_pipeline.clone()));

    let incoming_loop = kafka_io::run_incoming_loop(incoming_consumer, producer.clone(), live_pipeline.clone(), store.clone());
    let completed_loop = kafka_io::run_completed_loop(completed_consumer, producer.clone(), live_pipeline.clone(), store.clone());
    let retry_trigger_loop = kafka_io::run_retry_trigger_loop(retry_trigger_consumer, producer, live_pipeline, store);

    tokio::join!(incoming_loop, completed_loop, retry_trigger_loop);
}
