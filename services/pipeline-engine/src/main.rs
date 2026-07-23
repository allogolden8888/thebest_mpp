mod build_stage_execute;
mod execution_state;
mod health;
mod kafka_io;
mod pipeline_graph;
mod proto;

use health::HealthState;
use pipeline_graph::PipelineDefinition;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

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
    let pipeline: Arc<PipelineDefinition> = Arc::new(serde_json::from_str(&json).expect("pipeline.schema.json форма"));

    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let producer = kafka_io::build_producer(&bootstrap_servers);

    let store: kafka_io::StateStore = Arc::new(Mutex::new(HashMap::new()));
    let destinations: kafka_io::DestinationStore = Arc::new(Mutex::new(HashMap::new()));

    let incoming_consumer = kafka_io::build_consumer(&bootstrap_servers, "pipeline-engine", kafka_io::INCOMING_TOPIC);
    let completed_consumer = kafka_io::build_consumer(&bootstrap_servers, "pipeline-engine", kafka_io::COMPLETED_TOPIC);

    let incoming_loop = kafka_io::run_incoming_loop(incoming_consumer, producer.clone(), pipeline.clone(), store.clone(), destinations.clone());
    let completed_loop = kafka_io::run_completed_loop(completed_consumer, producer, pipeline, store, destinations);

    tokio::join!(incoming_loop, completed_loop);
}
