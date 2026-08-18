//! `/healthz`/`/readyz`/`/metrics` на 9090 — та же конвенция, что у остальных
//! сервисов этого среза (см. destination-resolution-service/src/health.rs).
//! Бизнес-REST API (`/v1/messages`) слушает отдельный порт — см. `http.rs`/`main.rs`.

use axum::{Router, routing::get};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct HealthState {
    pub ready: AtomicBool,
    /// `true` по умолчанию — сервисы этого среза все используют один
    /// `AtomicBool`-паттерн (не Go-стиль `SetDependencyChecks` с картой
    /// именованных проверок), так что вместо обобщённого механизма
    /// зависимостей здесь один явный флаг под единственную реальную внешнюю
    /// зависимость, которая теперь есть у `/readyz` — Vault. Остаётся `true`,
    /// если `AUTH_VERIFIER_MODE=env` (Vault вообще не используется, см.
    /// `main.rs`) — никто не переключает его в этом режиме, значит `/readyz`
    /// не деградирует из-за недоступности зависимости, которая не
    /// задействована. Когда используется `VaultAuthVerifier`, `main.rs`
    /// спавнит периодический `VaultClient::ping` и обновляет этот флаг.
    pub vault_healthy: AtomicBool,
}

impl Default for HealthState {
    fn default() -> Self {
        Self { ready: AtomicBool::new(false), vault_healthy: AtomicBool::new(true) }
    }
}

pub fn router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get({
                let state = state.clone();
                move || {
                    let state = state.clone();
                    async move {
                        if !state.ready.load(Ordering::Relaxed) {
                            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "partner snapshot not loaded")
                        } else if !state.vault_healthy.load(Ordering::Relaxed) {
                            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "vault unreachable")
                        } else {
                            (axum::http::StatusCode::OK, "ready")
                        }
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(|| async {
                "# HELP partner_rest_receiver_up Service liveness placeholder\n# TYPE partner_rest_receiver_up gauge\npartner_rest_receiver_up 1\n"
            }),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn healthz_always_ok() {
        let state = Arc::new(HealthState::default());
        let app = router(state);
        let response = app.oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_503_until_loaded() {
        let state = Arc::new(HealthState::default());
        let app = router(state.clone());
        let response = app.oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.ready.store(true, Ordering::Relaxed);
        let app = router(state);
        let response = app.oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_defaults_to_vault_healthy_true_env_mode_never_touches_it() {
        // AUTH_VERIFIER_MODE=env не использует Vault вообще — vault_healthy
        // остаётся true по умолчанию, /readyz не должен деградировать из-за
        // зависимости, которая не задействована в этом режиме.
        let state = Arc::new(HealthState::default());
        state.ready.store(true, Ordering::Relaxed);
        let app = router(state);
        let response = app.oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_503_when_vault_unhealthy_even_if_ready() {
        let state = Arc::new(HealthState::default());
        state.ready.store(true, Ordering::Relaxed);
        state.vault_healthy.store(false, Ordering::Relaxed);
        let app = router(state.clone());
        let response = app.oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.vault_healthy.store(true, Ordering::Relaxed);
        let app = router(state);
        let response = app.oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
