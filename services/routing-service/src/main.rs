mod health;
mod kafka_io;
mod proto;
mod route_table;
mod routing;

use health::HealthState;
use route_table::{RouteTable, RouteTableSnapshot};
use routing::ControlSnapshot;
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

    // В проде — снапшот из config.changes (entity_type=routing_table) по каждому
    // оператору; здесь заглушка на тот же формат, один тестовый оператор (Фаза 2.2).
    let route_table_path = std::env::var("ROUTE_TABLE_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/routing_table.valid.json".to_string());
    let json = std::fs::read_to_string(&route_table_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать {route_table_path}: {e}"));
    let table: RouteTable = serde_json::from_str(&json).expect("routing_table.schema.json форма");
    let snapshot = RouteTableSnapshot::from_tables(vec![table]);

    // Execution Control snapshot (scope=OPERATOR_ROUTE) — в проде проецируется
    // из execution.control (compacted) в локальный in-process snapshot; здесь
    // пустой (все маршруты ACTIVE по fail-open) до реализации Фазы 3.1.
    let control = ControlSnapshot::new();

    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let consumer = kafka_io::build_consumer(&bootstrap_servers, "routing-service");
    let producer = kafka_io::build_producer(&bootstrap_servers);

    kafka_io::run_loop(consumer, producer, snapshot, control).await;
}
