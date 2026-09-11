mod admission;
mod auth;
mod build_incoming;
mod config_reload;
mod config_source;
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
mod vault_auth;

use admission::AlwaysAdmit;
use arc_swap::ArcSwap;
use auth::{AuthVerifier, EnvAuthVerifier};
use health::HealthState;
use partner_config::{Partner, PartnerSnapshot};
use rate_limit::RateLimiter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use vault_auth::{VaultAuthVerifier, VaultClient};

/// `AUTH_VERIFIER_MODE` — `"vault"` (дефолт) использует `VaultAuthVerifier`
/// (реальный Vault read с TTL-кешем, см. `vault_auth.rs`); `"env"` явно
/// откатывается на `EnvAuthVerifier` (bootstrap/break-glass — старый
/// статический `PARTNER_CRED_*` механизм, НЕ удалён, остаётся доступным).
/// Неизвестное значение — предупреждение в лог + дефолт на `vault` (более
/// безопасное направление: явная опечатка в конфиге не должна тихо откатить
/// сервис на менее живую проверку credentials).
fn build_auth_verifier(health_state: &Arc<HealthState>) -> Box<dyn AuthVerifier> {
    let mode = std::env::var("AUTH_VERIFIER_MODE").unwrap_or_else(|_| "vault".to_string());
    match mode.as_str() {
        "env" => {
            tracing::warn!("AUTH_VERIFIER_MODE=env — используется EnvAuthVerifier (bootstrap/break-glass), не VaultAuthVerifier");
            Box::new(EnvAuthVerifier)
        }
        other => {
            if other != "vault" {
                tracing::warn!("AUTH_VERIFIER_MODE={other:?} не распознан — используется дефолт vault (VaultAuthVerifier)");
            }
            let vault_client = VaultClient::from_env();

            // Периодический health-poll на ТОМ ЖЕ клиенте/token source, что
            // реально используется на пути аутентификации — не отдельно
            // сконфигурированный клиент. /readyz должен отражать реальную
            // достижимость Vault, не статический флаг, выставленный один раз
            // при старте (см. health.rs).
            {
                let health_state = health_state.clone();
                let ping_client = vault_client.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(15));
                    loop {
                        interval.tick().await;
                        let healthy = ping_client.ping().await.is_ok();
                        if !healthy {
                            tracing::error!("Vault health-check (/v1/sys/health) не прошёл — /readyz теперь отражает недоступность");
                        }
                        health_state.vault_healthy.store(healthy, Ordering::Relaxed);
                    }
                });
            }

            Box::new(VaultAuthVerifier::new(vault_client))
        }
    }
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

    // BACKOFFICE_ROADMAP.md "Production Readiness Review" P0 #4: partner
    // config used to load exactly once from PARTNER_CONFIG_PATH at startup
    // and never again — a partner's self-service config change never
    // reached live traffic without a restart. PARTNER_CONFIG_PATH set
    // explicitly is now an opt-in fallback (static file, local dev without
    // Configuration Redis, no config.changes subscription); the default
    // path bootstraps from Configuration Redis (the same store
    // config-cache-projector already projects into) and stays live via a
    // config.changes consumer (config_reload.rs). Same design as the Go
    // sibling, services/partner-notification-service/cmd/.../main.go.
    let explicit_partner_path = std::env::var("PARTNER_CONFIG_PATH").ok().filter(|v| !v.is_empty());
    let mut config_redis_conn: Option<redis::aio::MultiplexedConnection> = None;
    let partner_snapshot = match &explicit_partner_path {
        Some(partner_path) => {
            tracing::warn!(
                "PARTNER_CONFIG_PATH={partner_path} задан явно — партнёрский снапшот грузится один раз из статического файла (local dev без Configuration Redis), config.changes НЕ отслеживается"
            );
            let partner_json =
                std::fs::read_to_string(partner_path).unwrap_or_else(|e| panic!("не удалось прочитать {partner_path}: {e}"));
            let partner: Partner = serde_json::from_str(&partner_json).expect("partner.schema.json форма");
            PartnerSnapshot::from_partners(vec![partner])
        }
        None => {
            let redis_configuration_url = redis_url::build_redis_configuration_url();
            let mut conn = redis::Client::open(redis_configuration_url.as_str())
                .expect("невалидный REDIS_CONFIGURATION_URL")
                .get_multiplexed_async_connection()
                .await
                .expect("не удалось подключиться к Configuration Redis при старте");
            let snapshot = config_source::load_all(&mut conn)
                .await
                .unwrap_or_else(|e| panic!("bootstrap-загрузка партнёров из Configuration Redis: {e}"));
            tracing::info!("bootstrap: партнёрский снапшот загружен из Configuration Redis ({} партнёров)", snapshot.len());
            config_redis_conn = Some(conn);
            snapshot
        }
    };
    let live_partner_snapshot = Arc::new(ArcSwap::from_pointee(partner_snapshot));

    let bootstrap_servers = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "kafka-bootstrap.mpp.svc:9092".to_string());
    let producer = kafka_io::build_producer(&bootstrap_servers);

    // Только когда bootstrap реально пришёл из Configuration Redis (не
    // static-file fallback) — отдельная consumer group от любых будущих
    // consumer'ов этого сервиса (сегодня их нет, этот процесс раньше был
    // producer-only), сбой этого цикла не должен ничего останавливать
    // кроме себя.
    if let Some(conn) = config_redis_conn {
        let config_consumer = config_reload::build_config_consumer(&bootstrap_servers, "partner-rest-receiver-config");
        tokio::spawn(config_reload::run_loop(config_consumer, conn, live_partner_snapshot.clone()));
    }

    let redis_runtime_url = redis_url::build_redis_runtime_url();
    // Один `MultiplexedConnection` на весь процесс — клонируется (дёшево, тот
    // же TCP-хендл) на каждый запрос вместо нового `Client::open` +
    // `get_multiplexed_async_connection` на каждый вызов. Реальная находка
    // нагрузочного прогона (1500 TPS push, `strace -c` под нагрузкой): старый
    // паттерн тратил ~90% времени сервиса в syscall'ах socket/connect/close —
    // TCP-хендшейк + Redis AUTH на КАЖДЫЙ HTTP-запрос, не в бизнес-логике.
    // Как и `producer` ниже (создан один раз, тот же общий паттерн) — до
    // этого фикса Redis был единственным исключением из него в этом файле.
    let redis_conn = redis::Client::open(redis_runtime_url.as_str())
        .expect("невалидный REDIS_RUNTIME_URL")
        .get_multiplexed_async_connection()
        .await
        .expect("не удалось подключиться к Runtime Redis при старте");

    let rate_limiter = RateLimiter::default();

    let auth_verifier = build_auth_verifier(&health_state);

    let state = Arc::new(http::AppState {
        partner_snapshot: live_partner_snapshot,
        auth_verifier,
        admission_gate: Box::new(AlwaysAdmit),
        rate_limiter,
        producer,
        redis_conn,
        concurrency_limit: Arc::new(tokio::sync::Semaphore::new(http::MAX_CONCURRENT_REQUESTS)),
    });

    // `sync_rate_limit_counters` — таймер ~1с, не Redis round-trip на каждое
    // сообщение (service_internal_methods.md §1.1).
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                redis_sync::sync_rate_limit_counters(state.redis_conn.clone(), &state.rate_limiter).await;
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
