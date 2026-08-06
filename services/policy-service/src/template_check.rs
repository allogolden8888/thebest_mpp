//! `POST /template-check` — pre-flight-проверка одного pattern перед
//! регистрацией шаблона в конфиге (`policy_template`). НЕ валидирует
//! структуру JSON самого config-документа — это делает
//! `config_schemas/policy_template.schema.json` + `config_schemas/validate_all.py`.
//! Только даёт предупреждения о селективности литеральных фрагментов, см.
//! `template_matching::check_pattern_selectivity` — всегда `200 OK` с
//! (возможно пустым) списком предупреждений, никогда не блокирует запрос.
//! Решение "заблокировать/подтвердить/проигнорировать" остаётся на
//! вызывающей стороне (в этой ветке (`main`) её пока нет — реальный
//! config-API с RBAC/UI существует только в ветке `subagent-1`, см.
//! CODE_REVIEW.md). Размещено здесь, а не там: это единственный процесс в
//! `main`, который реально знает алгоритм матчинга
//! (`parse_pattern`/`literal_fragments`), и он уже держит HTTP-сервер для
//! health-эндпоинтов на том же порту (`health.rs`) — не нужен ни новый
//! сервис, ни дублирование логики парсинга паттерна где-то ещё.

use crate::template_matching::check_pattern_selectivity;
use axum::{Json, Router, routing::post};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct CheckTemplateRequest {
    pattern: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CheckTemplateResponse {
    warnings: Vec<String>,
}

async fn check_template(Json(req): Json<CheckTemplateRequest>) -> Json<CheckTemplateResponse> {
    Json(CheckTemplateResponse { warnings: check_pattern_selectivity(&req.pattern) })
}

pub fn router() -> Router {
    Router::new().route("/template-check", post(check_template))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;

    async fn post_pattern(pattern: &str) -> (StatusCode, CheckTemplateResponse) {
        let body = serde_json::to_string(&serde_json::json!({ "pattern": pattern })).unwrap();
        let response = router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/template-check")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: CheckTemplateResponse = serde_json::from_slice(&bytes).unwrap();
        (status, parsed)
    }

    #[tokio::test]
    async fn real_template_pattern_has_no_warnings() {
        let (status, resp) = post_pattern("%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring").await;
        assert_eq!(status, StatusCode::OK);
        assert!(resp.warnings.is_empty());
    }

    #[tokio::test]
    async fn pure_placeholder_pattern_returns_warning_not_error() {
        let (status, resp) = post_pattern("%w").await;
        assert_eq!(status, StatusCode::OK, "предупреждение — это не HTTP-ошибка, эндпоинт не блокирует создание шаблона");
        assert_eq!(resp.warnings.len(), 1);
    }

    #[tokio::test]
    async fn short_literal_fragment_returns_warning() {
        let (status, resp) = post_pattern("%d{1,1}ab").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp.warnings.len(), 1);
    }

    #[tokio::test]
    async fn malformed_json_body_is_a_400_not_a_warning() {
        // Отличие от "плохого" pattern: это ошибка самого HTTP-запроса
        // (невалидный JSON), не диагностика по содержимому pattern — тут
        // уместен обычный 400, а не 200 с предупреждением.
        let response = router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/template-check")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
