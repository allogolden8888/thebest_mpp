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
    // оператору; здесь заглушка на тот же формат — по умолчанию все три реально
    // задокументированных оператора (development_plan.md 5.5), не один тестовый,
    // как было раньше (Фаза 2.2). ROUTE_TABLE_PATH — основной файл (обязателен,
    // как раньше), ROUTE_TABLE_EXTRA_PATHS — запятая-разделённый список
    // дополнительных routing_table-файлов, каждый мержится в тот же снапшот
    // (RouteTableSnapshot::from_tables уже строит HashMap по operator_id —
    // несколько файлов с разными operator_id просто дают несколько записей).
    let route_table_path = std::env::var("ROUTE_TABLE_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/routing_table.valid.json".to_string());
    let extra_route_table_paths = std::env::var("ROUTE_TABLE_EXTRA_PATHS").unwrap_or_else(|_| {
        "../../config_schemas/examples/routing_table.ucell_uz.valid.json,\
         ../../config_schemas/examples/routing_table.uzmobile_uz.valid.json"
            .to_string()
    });

    let load_table = |path: &str| -> RouteTable {
        let json = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("не удалось прочитать {path}: {e}"));
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("{path}: routing_table.schema.json форма: {e}"))
    };

    let mut tables = vec![load_table(&route_table_path)];
    tables.extend(
        extra_route_table_paths
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(load_table),
    );
    let snapshot = RouteTableSnapshot::from_tables(tables);

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
