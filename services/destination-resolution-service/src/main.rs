mod config_reload;
mod health;
mod kafka_io;
mod offset_tracker;
mod proto;
mod resolver;

use arc_swap::ArcSwap;
use health::HealthState;
use resolver::{ConfigOverlay, Snapshot};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let health_state = Arc::new(HealthState::default());

    // Health-сервер стартует немедленно — readyz=503 до загрузки снапшота
    // (k8s readinessProbe не пускает трафик на под, пока не готов).
    let health_router = health::router(health_state.clone());
    let health_listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .expect("не удалось забиндить health-порт 9090");
    tokio::spawn(async move {
        axum::serve(health_listener, health_router).await.expect("health-сервер упал");
    });

    // Статический файл — только bootstrap-нулевая точка (быстрый старт без
    // ожидания полного replay compacted-топика). Живое состояние дальше
    // ведётся config_reload.rs, реально подключённым к config.changes
    // (entity_type=NUMBER_RANGE) — то, что раньше было объявленным, но
    // никогда не подключённым arc-swap (см. README "Что НЕ реализовано").
    // number_portability_override НЕ входит в hot-reload контур — у него
    // нет своего ConfigEntityType в common/enums.proto, остаётся только
    // статическим (см. resolver::ConfigOverlay).
    let snapshot_path = std::env::var("SNAPSHOT_PATH")
        .unwrap_or_else(|_| "data/number_range_snapshot.json".to_string());
    let snapshot_json = std::fs::read_to_string(&snapshot_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать снапшот {snapshot_path}: {e}"));
    let snapshot = Snapshot::from_json_str(&snapshot_json).expect("снапшот должен парситься как JSON");

    let overlay = Arc::new(ConfigOverlay::new(snapshot.clone()));
    let live_snapshot = Arc::new(ArcSwap::from_pointee(snapshot));
    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let consumer = kafka_io::build_consumer(&bootstrap_servers, "destination-resolution-service");
    let producer = kafka_io::build_producer(&bootstrap_servers);
    // Отдельная consumer group от stage.destination-resolution — независимые
    // офсеты/партиционирование, сбой одного цикла не должен блокировать другой.
    let config_consumer =
        config_reload::build_config_consumer(&bootstrap_servers, "destination-resolution-service-config");
    tokio::spawn(config_reload::run_loop(config_consumer, overlay, live_snapshot.clone()));

    kafka_io::run_loop(consumer, producer, live_snapshot).await;
}
