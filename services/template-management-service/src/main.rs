mod bulk;
mod health;
mod list;
mod preview;
mod projector;
mod proto;
mod store;
mod template_matching;

use health::HealthState;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use store::Store;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn build_database_url() -> String {
    let host = env("POSTGRES_HOST", "localhost");
    let port = env("POSTGRES_PORT", "5432");
    let db = env("POSTGRES_DB", "mpp");
    let user = env("POSTGRES_USER", "");
    let password = env("POSTGRES_PASSWORD", "");
    format!("postgres://{user}:{password}@{host}:{port}/{db}")
}

/// Единственная точка интеграции трёх независимо реализованных фич —
/// list/search, preview, bulk import/export строились параллельно, каждая
/// в своём модуле, ни один не трогал main.rs.
fn app_router(store: Store) -> axum::Router {
    axum::Router::new()
        .merge(list::router(store.clone()))
        .merge(preview::router(store.clone()))
        .merge(bulk::router(store))
}

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

    let store = Store::connect(&build_database_url())
        .await
        .unwrap_or_else(|e| panic!("не удалось подключиться к Postgres: {e}"));
    store.ping().await.unwrap_or_else(|e| panic!("Postgres ping не прошёл: {e}"));
    health_state.ready.store(true, Ordering::Relaxed);

    let bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
    let projector_consumer = projector::build_consumer(&bootstrap_servers, "template-management-service");
    tokio::spawn(projector::run_loop(projector_consumer, store.clone()));

    let app = app_router(store);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("не удалось забиндить бизнес-порт 8080");
    axum::serve(listener, app).await.expect("HTTP-сервер упал");
}
