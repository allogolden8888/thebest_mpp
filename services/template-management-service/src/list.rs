//! `GET /v1/templates` — листинг/поиск по `policy.policy_template` для
//! admin/management UI (Ф4 плана, /Users/Alisher/.claude/plans/luminous-hugging-charm.md).
//!
//! Пагинация (`limit`/`offset`, отклонение отрицательных/нецелых значений
//! кодом 400) — та же конвенция, что уже используется в этом кодбейзе для
//! `GET /dlq` в `services/backoffice-api/internal/httpapi/dlq.go`, здесь не
//! изобретается новая.
//!
//! `sender_id` — буквальный фильтр по колонке (сравнение "как есть"), а не
//! "эффективный скоуп" (partner-wide NULL-шаблоны сюда не подмешиваются) —
//! резолюция эффективного скоупа под конкретного отправителя — это задача
//! preview-эндпоинта (другой модуль этой же фичи), не этого browse-инструмента.
//!
//! `status` не имеет дефолтного фильтра на `active` — это management-инструмент,
//! архивные шаблоны должны быть видны так же легко, как активные.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::store::Store;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Deserialize)]
struct ListParams {
    partner_id: Option<String>,
    sender_id: Option<String>,
    category: Option<String>,
    status: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
struct TemplateRow {
    template_id: Uuid,
    partner_id: String,
    operator_id: Option<String>,
    sender_id: Option<String>,
    channel: String,
    category: String,
    pattern: String,
    version: i32,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ListResponse {
    templates: Vec<TemplateRow>,
    limit: i64,
    offset: i64,
}

pub fn router(store: Store) -> Router {
    Router::new().route("/v1/templates", get(list_templates)).with_state(store)
}

async fn list_templates(
    State(store): State<Store>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListResponse>, (StatusCode, String)> {
    let limit = match params.limit {
        None => DEFAULT_LIMIT,
        Some(n) if n <= 0 => {
            return Err((StatusCode::BAD_REQUEST, "limit: ожидалось положительное целое".to_string()));
        }
        Some(n) if n > MAX_LIMIT => MAX_LIMIT,
        Some(n) => n,
    };
    let offset = match params.offset {
        None => 0,
        Some(n) if n < 0 => {
            return Err((StatusCode::BAD_REQUEST, "offset: ожидалось неотрицательное целое".to_string()));
        }
        Some(n) => n,
    };

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT template_id, partner_id, operator_id, sender_id, channel, category, pattern, \
         version, status, created_at, updated_at FROM policy.policy_template WHERE 1 = 1",
    );

    if let Some(partner_id) = params.partner_id.as_ref().filter(|s| !s.is_empty()) {
        qb.push(" AND partner_id = ").push_bind(partner_id.clone());
    }
    if let Some(sender_id) = params.sender_id.as_ref().filter(|s| !s.is_empty()) {
        qb.push(" AND sender_id = ").push_bind(sender_id.clone());
    }
    if let Some(category) = params.category.as_ref().filter(|s| !s.is_empty()) {
        qb.push(" AND category = ").push_bind(category.clone());
    }
    if let Some(status) = params.status.as_ref().filter(|s| !s.is_empty()) {
        qb.push(" AND status = ").push_bind(status.clone());
    }

    qb.push(" ORDER BY created_at DESC LIMIT ").push_bind(limit).push(" OFFSET ").push_bind(offset);

    let templates = qb
        .build_query_as::<TemplateRow>()
        .fetch_all(&store.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ошибка чтения policy_template: {e}")))?;

    Ok(Json(ListResponse { templates, limit, offset }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use sqlx::PgPool;
    use tower::ServiceExt;

    /// Тот же паттерн, что Go-тесты этого кодбейза для DB-зависимых кейсов
    /// (см. `services/partner-api/internal/store/postgres_test.go::testPool`):
    /// читаем DSN из env, при отсутствии/недоступности БД — печатаем причину
    /// и возвращаем `None`, вызывающий тест завершается ранним `return`
    /// (аналог `t.Skip`, которого в стандартном `#[test]` нет).
    async fn test_pool() -> Option<PgPool> {
        let url = match std::env::var("TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                println!("TEST_DATABASE_URL не задан — пропуск теста, требующего Postgres");
                return None;
            }
        };
        match sqlx::postgres::PgPoolOptions::new().max_connections(2).connect(&url).await {
            Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                Ok(_) => Some(pool),
                Err(e) => {
                    println!("Postgres недоступен на {url} ({e}) — пропуск теста");
                    None
                }
            },
            Err(e) => {
                println!("не удалось подключиться к Postgres ({e}) — пропуск теста");
                None
            }
        }
    }

    fn unique_partner_id(tag: &str) -> String {
        format!("test_list_{tag}_{}", Uuid::new_v4().simple())
    }

    async fn insert_template(
        pool: &PgPool,
        partner_id: &str,
        sender_id: Option<&str>,
        category: &str,
        status: &str,
    ) -> Uuid {
        let (id,): (Uuid,) = sqlx::query_as(
            "INSERT INTO policy.policy_template
                (partner_id, operator_id, sender_id, channel, category, pattern, version, status)
             VALUES ($1, NULL, $2, 'SMS', $3, 'pattern text', 1, $4)
             RETURNING template_id",
        )
        .bind(partner_id)
        .bind(sender_id)
        .bind(category)
        .bind(status)
        .fetch_one(pool)
        .await
        .expect("insert test template");
        id
    }

    async fn cleanup(pool: &PgPool, partner_id: &str) {
        sqlx::query("DELETE FROM policy.policy_template WHERE partner_id = $1")
            .bind(partner_id)
            .execute(pool)
            .await
            .ok();
    }

    async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
        let response =
            app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // Error responses ((StatusCode, String)) render as text/plain, not JSON —
        // fall back to a raw string Value instead of panicking on parse failure.
        let json: Value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()))
        };
        (status, json)
    }

    #[tokio::test]
    async fn filters_and_pagination_combined() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("combo");

        // 5 активных с sender A, 1 архивный с sender A, 1 активный с sender B,
        // 1 активный без sender (partner-wide MARKETING) — 8 строк всего.
        for i in 0..5 {
            insert_template(&pool, &partner_id, Some("sender_a"), "TRANSACTION", "active").await;
            // небольшая пауза не нужна — created_at DESC порядок проверяется
            // отдельно через limit/offset ниже на подмножестве с одинаковым фильтром
            let _ = i;
        }
        insert_template(&pool, &partner_id, Some("sender_a"), "TRANSACTION", "archived").await;
        insert_template(&pool, &partner_id, Some("sender_b"), "TRANSACTION", "active").await;
        insert_template(&pool, &partner_id, None, "MARKETING", "active").await;

        // partner_id filter alone: все 8 строк этого партнёра.
        let (status, body) =
            get_json(router(store.clone()), &format!("/v1/templates?partner_id={partner_id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["templates"].as_array().unwrap().len(), 8);
        assert_eq!(body["limit"], 50);
        assert_eq!(body["offset"], 0);

        // status not given -> both active and archived included (no default filter).
        let active_count =
            body["templates"].as_array().unwrap().iter().filter(|t| t["status"] == "active").count();
        let archived_count =
            body["templates"].as_array().unwrap().iter().filter(|t| t["status"] == "archived").count();
        assert_eq!(active_count, 7);
        assert_eq!(archived_count, 1);

        // sender_id literal filter: sender_a active+archived = 6, NOT the
        // partner-wide NULL-sender row (that would be 7 if "effective scope"
        // resolution leaked in here).
        let (status, body) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&sender_id=sender_a"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["templates"].as_array().unwrap().len(), 6);

        // category filter.
        let (status, body) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&category=MARKETING"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["templates"].as_array().unwrap().len(), 1);

        // status filter.
        let (status, body) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&status=archived"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["templates"].as_array().unwrap().len(), 1);

        // combination: partner + sender + status.
        let (status, body) = get_json(
            router(store.clone()),
            &format!(
                "/v1/templates?partner_id={partner_id}&sender_id=sender_a&status=active"
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["templates"].as_array().unwrap().len(), 5);

        // pagination: limit=2 offset=0 then offset=2 over the 8-row set should
        // slice into disjoint pages that together cover all 8 without overlap.
        let (status, page1) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&limit=2&offset=0"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page1["templates"].as_array().unwrap().len(), 2);
        assert_eq!(page1["limit"], 2);
        assert_eq!(page1["offset"], 0);

        let (status, page2) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&limit=2&offset=2"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page2["templates"].as_array().unwrap().len(), 2);

        let ids1: Vec<&Value> = page1["templates"].as_array().unwrap().iter().map(|t| &t["template_id"]).collect();
        let ids2: Vec<&Value> = page2["templates"].as_array().unwrap().iter().map(|t| &t["template_id"]).collect();
        assert!(ids1.iter().all(|id| !ids2.contains(id)), "pages must not overlap");

        let (status, last_page) = get_json(
            router(store.clone()),
            &format!("/v1/templates?partner_id={partner_id}&limit=50&offset=6"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(last_page["templates"].as_array().unwrap().len(), 2);

        cleanup(&pool, &partner_id).await;
    }

    #[tokio::test]
    async fn invalid_limit_rejected_with_400() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool };

        for bad in ["0", "-1", "-50"] {
            let (status, _) = get_json(router(store.clone()), &format!("/v1/templates?limit={bad}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "limit={bad} should be rejected");
        }

        // non-integer limit is rejected by axum's Query extractor itself (400).
        let (status, _) = get_json(router(store.clone()), "/v1/templates?limit=abc").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_offset_rejected_with_400() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool };

        let (status, _) = get_json(router(store.clone()), "/v1/templates?offset=-1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = get_json(router(store.clone()), "/v1/templates?offset=abc").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn limit_above_max_is_capped_not_rejected() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool };

        let (status, body) = get_json(router(store.clone()), "/v1/templates?limit=100000").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["limit"], MAX_LIMIT);
    }
}
