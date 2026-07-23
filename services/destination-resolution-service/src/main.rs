mod health;
mod kafka_io;
mod proto;
mod resolver;

use health::HealthState;
use resolver::Snapshot;
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

    // В проде — снапшот строится из config.changes (entity_type=number_range,
    // entity_type=number_portability_override), не из локального файла;
    // здесь заглушка на тот же формат, что уже провалидирован в
    // config_schemas/number_range.schema.json.
    let snapshot_path = std::env::var("SNAPSHOT_PATH")
        .unwrap_or_else(|_| "data/number_range_snapshot.json".to_string());
    let snapshot_json = std::fs::read_to_string(&snapshot_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать снапшот {snapshot_path}: {e}"));
    let snapshot = Snapshot::from_json_str(&snapshot_json).expect("снапшот должен парситься как JSON");
    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let consumer = kafka_io::build_consumer(&bootstrap_servers, "destination-resolution-service");
    let producer = kafka_io::build_producer(&bootstrap_servers);

    kafka_io::run_loop(consumer, producer, snapshot).await;
}
