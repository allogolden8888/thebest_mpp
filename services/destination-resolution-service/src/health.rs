//! `/healthz`, `/readyz`, `/metrics` на HEALTH_PORT=9090 — конвенция,
//! введённая в k8s/generate_manifests.py (readiness/liveness probes,
//! PodMonitor scrape target). Реализована здесь как первый сервис,
//! реально предоставляющий эти три пути — раньше конвенция существовала
//! только как ожидание в манифестах, ни один сервис её не реализовывал.

use axum::{Router, routing::get};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct HealthState {
    /// false до тех пор, пока snapshot не загружен — readyz не должен
    /// отвечать 200, пока resolver ещё не может обслуживать запросы.
    pub ready: AtomicBool,
}

impl Default for HealthState {
    fn default() -> Self {
        Self { ready: AtomicBool::new(false) }
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
                        if state.ready.load(Ordering::Relaxed) {
                            (axum::http::StatusCode::OK, "ready")
                        } else {
                            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "snapshot not loaded")
                        }
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(|| async {
                // Плейсхолдер Prometheus exposition format — реальные счётчики
                // (resolved_total/not_found_total и т.д.) вне скоупа этого шага,
                // PodMonitor должен получать хотя бы валидный пустой ответ 200,
                // не 404, чтобы отличать "сервис жив, метрик пока нет" от
                // "эндпоинт не существует".
                "# HELP destination_resolution_up Service liveness placeholder\n# TYPE destination_resolution_up gauge\ndestination_resolution_up 1\n"
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
        let response = app
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_503_until_snapshot_loaded() {
        let state = Arc::new(HealthState::default());
        let app = router(state.clone());
        let response = app
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.ready.store(true, Ordering::Relaxed);
        let app = router(state);
        let response = app
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
