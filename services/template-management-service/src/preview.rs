//! Ф4 плана (/Users/Alisher/.claude/plans/luminous-hugging-charm.md) —
//! флагманская фича: "прогони пример текста против всего активного ruleset
//! партнёра". `POST /v1/templates/preview` строит эфемерный
//! `CompiledRuleset` (см. `template_matching.rs`, буквальная копия
//! `policy-service`'s матчера) из строк `policy.policy_template`,
//! отфильтрованных по partner_id/status/sender_id, и прогоняет через него
//! один пример текста. Это debugging/management-инструмент, не hot path —
//! ruleset НЕ кэшируется между запросами, строится заново на каждый вызов
//! (в отличие от `policy-service`, где загрузка ruleset — дорогая
//! bootstrap-операция, здесь типичное число шаблонов на партнёра малое, а
//! свежесть важнее, чем экономия CPU на повторной компиляции).
//!
//! Sender-scoping (см. doc-comment `template_matching::CompiledRuleset`):
//! шаблон с `sender_id: None` матчится любому отправителю партнёра, шаблон
//! с `sender_id: Some(x)` — только отправителю с ровно этим id. Наивный SQL
//! "partner_id = $1 AND status = 'active'" без учёта sender_id вернул бы В
//! КАНДИДАТЫ шаблоны ЧУЖИХ отправителей, и хотя `find_match` сам отфильтрует
//! их по sender-параметру, при превью "для партнёра вообще" (запрос без
//! sender_id) это означало бы прогон текста ПРОТИВ шаблонов случайного
//! отправителя, просто с гарантией, что они не победят из-за
//! sender-фильтра внутри `find_match` — лишняя работа и концептуальная
//! путаница ("почему в кандидатах вообще оказался шаблон sender'а X, если я
//! никого не указал"). Поэтому фильтрация по sender_id происходит уже на
//! уровне SQL (`sender_id IS NULL OR sender_id = $2`): при $2 = NULL
//! (Postgres) это стягивается ровно к `sender_id IS NULL`, потому что
//! `sender_id = NULL` — трёхзначная логика, всегда NULL/false в WHERE,
//! никогда не true. Тот же принцип, что и `sender_id_by_template` фильтр
//! внутри `find_match`, продублированный на уровне SQL до того, как строки
//! вообще попадут в `CompiledRuleset::new`.
//!
//! Второй эндпоинт на этом же роутере, `POST /v1/templates/validate-pattern`,
//! — тонкая HTTP-обёртка над `template_matching::check_pattern_selectivity`
//! (та же функция, что уже приводит в действие `policy-service`'s
//! `/template-check`, см. `template_check.rs` там) — не переизобретается
//! здесь, только реэкспортируется под управленческий REST-путь этого
//! сервиса. БД не трогает вообще: чистая функция над строкой pattern.

use crate::store::Store;
use crate::template_matching::{CompiledRuleset, Template, check_pattern_selectivity};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct PreviewRequest {
    #[serde(default)]
    partner_id: String,
    #[serde(default)]
    sender_id: Option<String>,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct PreviewResponse {
    matched: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    template_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct TemplateRow {
    template_id: uuid::Uuid,
    category: String,
    pattern: String,
    sender_id: Option<String>,
}

async fn preview(
    State(store): State<Store>,
    Json(req): Json<PreviewRequest>,
) -> Result<Json<PreviewResponse>, (StatusCode, String)> {
    if req.partner_id.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "partner_id обязателен и не может быть пустым".to_string()));
    }
    if req.text.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "text обязателен и не может быть пустым".to_string()));
    }

    // Пустая строка трактуется как "не указан" — та же семантика, что
    // отсутствие поля вообще (не пытаемся угадать реальный sender_id "").
    let sender_id: Option<&str> = req.sender_id.as_deref().filter(|s| !s.trim().is_empty());

    // См. doc-comment модуля: `sender_id = $2` при $2 = NULL никогда не true
    // (трёхзначная логика SQL) — это стягивает условие ровно к
    // `sender_id IS NULL`, когда caller не указал sender_id, без отдельной
    // ветки запроса.
    let rows: Vec<TemplateRow> = sqlx::query_as(
        r#"
        SELECT template_id, category, pattern, sender_id
        FROM policy.policy_template
        WHERE partner_id = $1
          AND status = 'active'
          AND (sender_id IS NULL OR sender_id = $2)
        "#,
    )
    .bind(&req.partner_id)
    .bind(sender_id)
    .fetch_all(&store.pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("не удалось прочитать policy_template: {e}")))?;

    let templates: Vec<Template> = rows
        .into_iter()
        .map(|r| Template {
            template_id: r.template_id.to_string(),
            pattern: r.pattern,
            category: r.category,
            sender_id: r.sender_id,
        })
        .collect();

    let ruleset = CompiledRuleset::new(templates);
    // Все кандидаты в `templates` уже гарантированно имеют sender_id: None,
    // когда caller не указал sender_id (см. SQL-фильтр выше) — фильтр
    // sender_id внутри `find_match` для них no-op, поэтому "" здесь
    // безопасен как заглушка.
    let matched = ruleset.find_match(&req.text, sender_id.unwrap_or(""));

    Ok(Json(match matched {
        Some(m) => PreviewResponse { matched: true, template_id: Some(m.template_id), category: Some(m.category) },
        None => PreviewResponse { matched: false, template_id: None, category: None },
    }))
}

#[derive(Debug, Deserialize)]
struct ValidatePatternRequest {
    pattern: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ValidatePatternResponse {
    warnings: Vec<String>,
}

/// Не переиспользует `store` вообще — намеренно (см. doc-comment модуля).
/// Не принимает `State<Store>` явно: axum не требует, чтобы каждый handler
/// на роутере с заданным state-типом потреблял этот state.
async fn validate_pattern(Json(req): Json<ValidatePatternRequest>) -> Json<ValidatePatternResponse> {
    Json(ValidatePatternResponse { warnings: check_pattern_selectivity(&req.pattern) })
}

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/v1/templates/preview", post(preview))
        .route("/v1/templates/validate-pattern", post(validate_pattern))
        .with_state(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header};
    use serde_json::{Value, json};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt;

    async fn post_json(app: Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // Ошибочные ответы (400/500) — это (StatusCode, String) из хендлера,
        // сериализуется axum как plain text, не JSON — в отличие от успешных
        // JSON-ответов. Подставляем сырой текст как JSON-строку, а не
        // паникуем на serde_json::from_slice, чтобы один и тот же helper
        // годился и для happy-path, и для проверки статусов ошибок.
        let parsed: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        (status, parsed)
    }

    /// `connect_lazy` не открывает реальное соединение — годится для тестов
    /// путей, которые вообще не должны трогать БД (валидация запроса,
    /// `/validate-pattern`), без требования живого Postgres.
    fn lazy_store() -> Store {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost:5432/nonexistent-db-never-connected")
            .expect("connect_lazy не должен реально подключаться");
        Store { pool }
    }

    /// Тот же skip-конвенция, что Go-сервисы этого кодбейза используют для
    /// тестов, которым реально нужен Postgres (см.
    /// services/partner-api/internal/store/postgres_test.go:
    /// `t.Skipf` при недоступной БД) — портировано на идиому Rust-тестов:
    /// печатаем причину и возвращаем `None`, вызывающий тест ранний-return'ит
    /// без падения (Rust `#[test]` не имеет собственного понятия "skipped",
    /// "тихо пройден без реальной проверки" — ближайший честный эквивалент).
    async fn test_pool() -> Option<sqlx::PgPool> {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL не задан — пропуск теста, которому нужен реальный Postgres");
            return None;
        };
        match PgPoolOptions::new().max_connections(2).connect(&url).await {
            Ok(pool) => Some(pool),
            Err(e) => {
                eprintln!("не удалось подключиться к Postgres по TEST_DATABASE_URL ({e}) — пропуск теста");
                None
            }
        }
    }

    async fn insert_template(pool: &sqlx::PgPool, partner_id: &str, sender_id: Option<&str>, category: &str, pattern: &str) {
        sqlx::query(
            r#"
            INSERT INTO policy.policy_template (partner_id, sender_id, category, pattern, version, status)
            VALUES ($1, $2, $3, $4, 1, 'active')
            "#,
        )
        .bind(partner_id)
        .bind(sender_id)
        .bind(category)
        .bind(pattern)
        .execute(pool)
        .await
        .expect("insert тестового policy_template не прошёл");
    }

    #[tokio::test]
    async fn missing_partner_id_returns_400_without_touching_db() {
        let (status, _) = post_json(router(lazy_store()), "/v1/templates/preview", json!({ "text": "hello" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn empty_partner_id_returns_400() {
        let (status, _) =
            post_json(router(lazy_store()), "/v1/templates/preview", json!({ "partner_id": "  ", "text": "hello" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn missing_text_returns_400_without_touching_db() {
        let (status, _) =
            post_json(router(lazy_store()), "/v1/templates/preview", json!({ "partner_id": "acme" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn validate_pattern_warns_on_pure_placeholder_pattern() {
        let (status, body) =
            post_json(router(lazy_store()), "/v1/templates/validate-pattern", json!({ "pattern": "%w" })).await;
        assert_eq!(status, StatusCode::OK);
        let warnings = body["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1, "чистый плейсхолдер без литералов — предупреждение через check_pattern_selectivity");
    }

    #[tokio::test]
    async fn validate_pattern_no_warnings_for_well_formed_pattern() {
        let (status, body) = post_json(
            router(lazy_store()),
            "/v1/templates/validate-pattern",
            json!({ "pattern": "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["warnings"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sender_scoping_end_to_end_against_real_postgres() {
        let Some(pool) = test_pool().await else { return };
        // partner_id уникален на прогон теста, чтобы не пересекаться ни с
        // seed-данными миграций, ни с параллельными прогонами этого файла.
        let partner_id = format!("preview-test-{}", uuid::Uuid::new_v4());

        insert_template(&pool, &partner_id, Some("sender-a"), "SERVICE", "code: %d{4,4}").await;
        // "hello %w" (не "hello %w world"): %w должен матчить непустой
        // непробельный токен МЕЖДУ двумя литералами — "hello world" ниже
        // содержит ровно одно слово после "hello ", второй литерал с
        // ведущим пробелом оставил бы регион пустым и никогда не совпал бы.
        insert_template(&pool, &partner_id, None, "GENERIC", "hello %w").await;

        let store = Store { pool: pool.clone() };

        // 1. sender_id запроса совпадает с sender_id шаблона — матч.
        let (status, body) = post_json(
            router(store.clone()),
            "/v1/templates/preview",
            json!({ "partner_id": partner_id, "sender_id": "sender-a", "text": "code: 1234" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["matched"], true, "sender-a должен получить матч на свой шаблон: {body:?}");
        assert_eq!(body["category"], "SERVICE");

        // 2. тот же текст, другой sender_id — НЕ матч (шаблон не eligible).
        let (_, body) = post_json(
            router(store.clone()),
            "/v1/templates/preview",
            json!({ "partner_id": partner_id, "sender_id": "sender-b", "text": "code: 1234" }),
        )
        .await;
        assert_eq!(body["matched"], false, "чужой sender_id не должен видеть sender-scoped шаблон: {body:?}");

        // 2b. тот же текст, sender_id вообще не указан — тоже НЕ матч (см.
        // doc-comment модуля: превью "для партнёра вообще" не должно
        // случайно всплывать через шаблон конкретного отправителя).
        let (_, body) = post_json(
            router(store.clone()),
            "/v1/templates/preview",
            json!({ "partner_id": partner_id, "text": "code: 1234" }),
        )
        .await;
        assert_eq!(body["matched"], false, "без sender_id sender-scoped шаблон не должен быть кандидатом: {body:?}");

        // 3. partner-wide (sender_id IS NULL) шаблон матчится независимо от
        // того, указан ли sender_id в запросе, и каким конкретно значением.
        let (_, body) = post_json(
            router(store.clone()),
            "/v1/templates/preview",
            json!({ "partner_id": partner_id, "sender_id": "sender-a", "text": "hello world" }),
        )
        .await;
        assert_eq!(body["matched"], true, "partner-wide шаблон должен матчиться при указанном sender_id: {body:?}");

        let (_, body) = post_json(
            router(store.clone()),
            "/v1/templates/preview",
            json!({ "partner_id": partner_id, "text": "hello world" }),
        )
        .await;
        assert_eq!(body["matched"], true, "partner-wide шаблон должен матчиться без sender_id: {body:?}");

        sqlx::query("DELETE FROM policy.policy_template WHERE partner_id = $1")
            .bind(&partner_id)
            .execute(&pool)
            .await
            .expect("cleanup тестовых policy_template не прошёл");
    }
}
