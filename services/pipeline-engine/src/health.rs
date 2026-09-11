//! `/healthz`/`/readyz`/`/metrics` на 9090 — та же конвенция, что у
//! остальных сервисов этого среза.

use axum::{Router, routing::get};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct HealthState {
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
                            (axum::http::StatusCode::SERVICE_UNAVAILABLE, "pipeline bootstrap not complete")
                        }
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(|| async {
                "# HELP pipeline_engine_up Service liveness placeholder\n# TYPE pipeline_engine_up gauge\npipeline_engine_up 1\n"
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
}
