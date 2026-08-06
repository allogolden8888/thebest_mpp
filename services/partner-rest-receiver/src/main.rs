mod admission;
mod auth;
mod build_incoming;
mod health;
mod http;
mod idempotency;
mod ip_allowlist;
mod kafka_io;
mod msgctx;
mod partner_config;
mod proto;
mod rate_limit;
mod redis_sync;
mod redis_url;
mod request;
mod segmentation;

use admission::AlwaysAdmit;
use auth::EnvAuthVerifier;
use health::HealthState;
use partner_config::{Partner, PartnerSnapshot};
use rate_limit::RateLimiter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

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

    // В проде — снапшот из config.changes (entity_type=PARTNER); здесь заглушка
    // на тот же формат, один тестовый партнёр (Фаза 2.2, тот же паттерн
    // упрощения, что у Routing/Policy — см. README).
    let partner_path = std::env::var("PARTNER_CONFIG_PATH")
        .unwrap_or_else(|_| "../../config_schemas/examples/partner.valid.json".to_string());
    let partner_json = std::fs::read_to_string(&partner_path)
        .unwrap_or_else(|e| panic!("не удалось прочитать {partner_path}: {e}"));
    let partner: Partner = serde_json::from_str(&partner_json).expect("partner.schema.json форма");
    let partner_snapshot = PartnerSnapshot::from_partners(vec![partner]);

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let producer = kafka_io::build_producer(&bootstrap_servers);

    let redis_runtime_url = redis_url::build_redis_runtime_url();

    let rate_limiter = RateLimiter::default();

    let state = Arc::new(http::AppState {
        partner_snapshot,
        auth_verifier: Box::new(EnvAuthVerifier),
        admission_gate: Box::new(AlwaysAdmit),
        rate_limiter,
        producer,
        redis_runtime_url: redis_runtime_url.clone(),
        concurrency_limit: Arc::new(tokio::sync::Semaphore::new(http::MAX_CONCURRENT_REQUESTS)),
    });

    // `sync_rate_limit_counters` — таймер ~1с, не Redis round-trip на каждое
    // сообщение (service_internal_methods.md §1.1).
    {
        let state = state.clone();
        let redis_runtime_url = redis_runtime_url.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                redis_sync::sync_rate_limit_counters(&redis_runtime_url, &state.rate_limiter).await;
            }
        });
    }

    health_state.ready.store(true, Ordering::Relaxed);

    let api_router = http::router(state);
    let api_listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.expect("не удалось забиндить REST-порт 8080");
    axum::serve(api_listener, api_router.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .expect("REST-сервер упал");
}
