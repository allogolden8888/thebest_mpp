//! `GET /v1/templates/export` + `POST /v1/templates/import` — bulk
//! CSV export/import для `policy.policy_template` (Ф4 плана,
//! /Users/Alisher/.claude/plans/luminous-hugging-charm.md).
//!
//! КРИТИЧЕСКИ ВАЖНО (проверено чтением реального кода, не предположено):
//! `policy.policy_template` — это ТОЛЬКО read-модель, материализуемая
//! `src/projector.rs` из `config.changes`. Единственный писатель для
//! entity_type=policy_template — `configuration-service`
//! (`internal/store/store.go::CreateImmutableVersionAndOutbox`,
//! `skipsConfigVersionsTable` ветка) — и он тоже пишет НЕ в
//! `policy.policy_template`, а голый INSERT в `config.config_outbox`
//! (`config_version_id=NULL, entity_type='policy_template', entity_id=<id>,
//! payload=<json>`), откуда `config-event-publisher` публикует в Kafka
//! `config.changes`, а уже оттуда и `policy-service` (живой matching), и наш
//! собственный `projector.rs` (эта read-модель) получают консистентное
//! состояние. Поэтому импорт здесь пишет ТОЧНО В ТОТ ЖЕ `config.config_outbox`
//! тем же способом — НИКОГДА не пишет напрямую в `policy.policy_template` —
//! иначе импортированные строки были бы видны в list/preview этого сервиса,
//! но никогда не попали бы в реальный matching policy-service (тихая
//! рассинхронизация). Экспорт, наоборот, читает `policy.policy_template`
//! напрямую — это чтение текущего материализованного состояния, не запись,
//! так что обходить write-path здесь незачем.
//!
//! Поскольку этот путь полностью обходит JSON-schema-валидацию, которую
//! `configuration-service` обычно делает в своём gRPC/HTTP хендлере до
//! вызова store, импорт здесь — ЕДИНСТВЕННЫЙ гейт валидации на этом пути:
//! каждая строка валидируется против настоящего
//! `config_schemas/policy_template.schema.json` (крейт `jsonschema`, НЕ
//! рукописный дубликат проверок, который мог бы разойтись со схемой).
//!
//! Транзакционная модель: КАЖДАЯ строка — свой независимый `INSERT` без
//! общей многострочной транзакции. Требование "одна плохая строка не должна
//! блокировать остальные 499 в батче из 500" (частичный успех) прямо
//! противоречит "всё в одной транзакции" (там любая ошибка требует
//! `ROLLBACK` всего или ручного `SAVEPOINT` на каждую строку — то же самое
//! per-row-commit поведение, но сложнее и без выигрыша, раз sqlx/Postgres и
//! так делает каждый одиночный `INSERT` собственной auto-commit единицей
//! работы). Валидация схемой (шаг 4 в плане) в любом случае отсеивает
//! почти все содержательные ошибки ДО обращения к БД — до `INSERT` доходят
//! только структурно корректные payload'ы, так что атомарность всего батча
//! не покупает почти ничего, а стоит усложнения.

use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::OnceLock;
use uuid::Uuid;

use crate::store::Store;
use crate::template_matching::check_pattern_selectivity;

const CSV_HEADER_ORDER: &str = "template_id,partner_id,operator_id,sender_id,channel,category,pattern,version,status";

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/v1/templates/export", get(export_templates))
        .route("/v1/templates/import", post(import_templates))
        .with_state(store)
}

// ---------------------------------------------------------------------
// Схема (config_schemas/policy_template.schema.json), скомпилирована один
// раз и переиспользуется для каждой строки импорта — компиляция схемы
// заметно дороже, чем один `iter_errors` прогон, гонять её на каждую строку
// 500-строчного файла незачем.
// ---------------------------------------------------------------------

static SCHEMA_JSON: &str = include_str!("../../../config_schemas/policy_template.schema.json");

fn validator() -> &'static jsonschema::Validator {
    static VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();
    VALIDATOR.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON)
            .expect("config_schemas/policy_template.schema.json: невалидный JSON");
        jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .expect("config_schemas/policy_template.schema.json: не скомпилировался как JSON Schema")
    })
}

/// `Err` — все ошибки валидации, склеенные в одну строку (per-row error
/// сообщение в ответе — не структурированный список, JSON Schema может
/// вернуть несколько нарушений на один payload, все они полезны для
/// диагностики конкретной строки CSV).
fn validate_payload(payload: &Value) -> Result<(), String> {
    let errors: Vec<String> = validator()
        .iter_errors(payload)
        .map(|e| format!("{e} (в {})", e.instance_path()))
        .collect();
    if errors.is_empty() { Ok(()) } else { Err(errors.join("; ")) }
}

// ---------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ExportQuery {
    partner_id: Option<String>,
    format: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct PolicyTemplateDbRow {
    template_id: Uuid,
    partner_id: String,
    operator_id: Option<String>,
    sender_id: Option<String>,
    channel: String,
    category: String,
    pattern: String,
    version: i32,
    status: String,
}

/// Форма одной строки CSV — общая для экспорта (Serialize) и импорта
/// (Deserialize, см. `ImportRow` ниже) в том смысле, что порядок и имена
/// колонок идентичны (round-trip-friendly), но не один и тот же тип: при
/// экспорте `version` — целое (реальное значение из NOT NULL колонки),
/// при импорте — строка (нужно уметь отличить "не число" от "число вне
/// диапазона" отдельными сообщениями об ошибке для конкретной строки).
#[derive(Debug, Serialize)]
struct ExportRow {
    template_id: String,
    partner_id: String,
    operator_id: String,
    sender_id: String,
    channel: String,
    category: String,
    pattern: String,
    version: i32,
    status: String,
}

impl From<PolicyTemplateDbRow> for ExportRow {
    fn from(r: PolicyTemplateDbRow) -> Self {
        Self {
            template_id: r.template_id.to_string(),
            partner_id: r.partner_id,
            operator_id: r.operator_id.unwrap_or_default(),
            sender_id: r.sender_id.unwrap_or_default(),
            channel: r.channel,
            category: r.category,
            pattern: r.pattern,
            version: r.version,
            status: r.status,
        }
    }
}

async fn export_templates(State(store): State<Store>, Query(q): Query<ExportQuery>) -> Response {
    let Some(partner_id) = q.partner_id.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) else {
        return (
            StatusCode::BAD_REQUEST,
            "partner_id query-параметр обязателен — экспорт шаблонов всей платформы без \
             ограничения по партнёру не является разумным дефолтом",
        )
            .into_response();
    };
    if let Some(fmt) = q.format.as_ref() {
        if fmt != "csv" {
            return (StatusCode::BAD_REQUEST, format!("неподдерживаемый format={fmt:?}, поддерживается только csv"))
                .into_response();
        }
    }

    let rows: Vec<PolicyTemplateDbRow> = match sqlx::query_as(
        "SELECT template_id, partner_id, operator_id, sender_id, channel, category, pattern, version, status \
         FROM policy.policy_template WHERE partner_id = $1 ORDER BY template_id",
    )
    .bind(partner_id)
    .fetch_all(&store.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("export: ошибка чтения policy_template: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("ошибка чтения policy_template: {e}"))
                .into_response();
        }
    };

    let mut wtr = csv::Writer::from_writer(Vec::new());
    for row in rows {
        let export_row: ExportRow = row.into();
        if let Err(e) = wtr.serialize(&export_row) {
            tracing::error!("export: ошибка сериализации CSV-строки: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("ошибка сериализации CSV: {e}")).into_response();
        }
    }
    let csv_bytes = match wtr.into_inner() {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("export: ошибка flush CSV writer: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "ошибка формирования CSV").into_response();
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv")
        .header(header::CONTENT_DISPOSITION, "attachment; filename=\"templates.csv\"")
        .body(Body::from(csv_bytes))
        .expect("валидный CSV export response")
}

// ---------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------

/// Та же колонка-форма, что `ExportRow`, но все поля — сырые строки: CSV
/// текст не различает "пусто" от "0" от "не число" на уровне типов, это
/// разбирается явно ниже (шаг за шагом, с отдельным сообщением об ошибке на
/// каждый практический случай, а не одним общим "невалидная строка").
#[derive(Debug, Deserialize)]
struct ImportRow {
    template_id: String,
    partner_id: String,
    operator_id: String,
    sender_id: String,
    channel: String,
    category: String,
    pattern: String,
    version: String,
    status: String,
}

#[derive(Debug, Serialize)]
struct FailedRow {
    row: u64,
    error: String,
}

#[derive(Debug, Serialize)]
struct WarningRow {
    row: u64,
    template_id: String,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ImportResponse {
    imported: usize,
    failed: Vec<FailedRow>,
    warnings: Vec<WarningRow>,
}

fn non_empty(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() { None } else { Some(t.to_string()) }
}

async fn import_templates(State(store): State<Store>, body: Bytes) -> Response {
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(body.as_ref());
    let headers = match rdr.headers() {
        Ok(h) => h.clone(),
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("не удалось прочитать заголовок CSV (ожидается {CSV_HEADER_ORDER:?}): {e}"))
                .into_response();
        }
    };

    let mut imported = 0usize;
    let mut failed: Vec<FailedRow> = Vec::new();
    let mut warnings: Vec<WarningRow> = Vec::new();

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                // Позицию сломанной записи сам csv-крейт не всегда может
                // восстановить (это и есть парс-ошибка) — 0 сигнализирует
                // "см. текст ошибки", не настоящий номер строки.
                failed.push(FailedRow { row: 0, error: format!("ошибка разбора CSV: {e}") });
                continue;
            }
        };
        let row_num = record.position().map(|p| p.line()).unwrap_or(0);

        let row: ImportRow = match record.deserialize(Some(&headers)) {
            Ok(r) => r,
            Err(e) => {
                failed.push(FailedRow { row: row_num, error: format!("некорректные колонки: {e}") });
                continue;
            }
        };

        let template_id = if row.template_id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            match Uuid::parse_str(row.template_id.trim()) {
                Ok(u) => u.to_string(),
                Err(e) => {
                    failed.push(FailedRow {
                        row: row_num,
                        error: format!("template_id {:?} не является UUID: {e}", row.template_id),
                    });
                    continue;
                }
            }
        };

        let version: i64 = match row.version.trim().parse() {
            Ok(v) => v,
            Err(_) => {
                failed.push(FailedRow {
                    row: row_num,
                    error: format!("version {:?} не является целым числом", row.version),
                });
                continue;
            }
        };

        let payload = json!({
            "template_id": template_id,
            "partner_id": row.partner_id,
            "operator_id": non_empty(&row.operator_id),
            "sender_id": non_empty(&row.sender_id),
            "channel": row.channel,
            "category": row.category,
            "pattern": row.pattern,
            "version": version,
            "status": row.status,
        });

        if let Err(err_msg) = validate_payload(&payload) {
            failed.push(FailedRow { row: row_num, error: err_msg });
            continue;
        }

        // Тот же самый write-path, что `configuration-service`
        // (`internal/store/store.go::CreateImmutableVersionAndOutbox`,
        // `skipsConfigVersionsTable` ветка) использует для
        // entity_type=policy_template: голый INSERT в config.config_outbox,
        // config_version_id=NULL. payload биндится как TEXT + explicit
        // `::jsonb` cast, не через sqlx::types::Json — тот путь требует
        // cargo-фичу sqlx "json", которой нет в текущем Cargo.toml этого
        // сервиса, а трогать Cargo.toml из этого модуля нельзя (см. отчёт).
        let payload_text = payload.to_string();
        match sqlx::query(
            "INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload) \
             VALUES (NULL, 'policy_template', $1, $2::jsonb)",
        )
        .bind(&template_id)
        .bind(&payload_text)
        .execute(&store.pool)
        .await
        {
            Ok(_) => {
                imported += 1;
                let selectivity = check_pattern_selectivity(&row.pattern);
                if !selectivity.is_empty() {
                    warnings.push(WarningRow { row: row_num, template_id: template_id.clone(), warnings: selectivity });
                }
            }
            Err(e) => {
                tracing::error!("import: не удалось вставить outbox-запись (row={row_num}, template_id={template_id}): {e}");
                failed.push(FailedRow { row: row_num, error: format!("ошибка записи в БД: {e}") });
            }
        }
    }

    Json(ImportResponse { imported, failed, warnings }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use serde_json::Value as JsonValue;
    use sqlx::PgPool;
    use tower::ServiceExt;

    /// Тот же паттерн, что `src/list.rs::tests::test_pool` (и Go-эквивалент
    /// в `services/configuration-service/internal/store/store_test.go`):
    /// читаем DSN из `TEST_DATABASE_URL`, при отсутствии/недоступности БД
    /// печатаем причину и возвращаем `None` — вызывающий тест завершается
    /// ранним `return` (аналог `t.Skip`).
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
        format!("test_bulk_{tag}_{}", Uuid::new_v4().simple())
    }

    async fn cleanup(pool: &PgPool, partner_id: &str) {
        sqlx::query("DELETE FROM policy.policy_template WHERE partner_id = $1")
            .bind(partner_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM config.config_outbox WHERE entity_type = 'policy_template' AND payload->>'partner_id' = $1")
            .bind(partner_id)
            .execute(pool)
            .await
            .ok();
    }

    async fn to_json(response: Response) -> JsonValue {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).expect("response body должен быть JSON")
    }

    // -----------------------------------------------------------------
    // Пуре-логические тесты (не трогают БД) — csv-крейт корректно
    // экранирует запятые/кавычки внутри pattern при экспорте и корректно
    // разбирает их обратно при повторном чтении того же текста.
    // -----------------------------------------------------------------

    #[test]
    fn csv_export_round_trips_commas_and_quotes_in_pattern() {
        let row = ExportRow {
            template_id: "11111111-1111-1111-1111-111111111111".to_string(),
            partner_id: "acme".to_string(),
            operator_id: String::new(),
            sender_id: "acme_sms".to_string(),
            channel: "SMS".to_string(),
            category: "TRANSACTION".to_string(),
            pattern: r#"%w, shartnoma "raqami" bo'yicha %d{1,6} so'm"#.to_string(),
            version: 3,
            status: "active".to_string(),
        };

        let mut wtr = csv::Writer::from_writer(Vec::new());
        wtr.serialize(&row).unwrap();
        let csv_bytes = wtr.into_inner().unwrap();

        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(csv_bytes.as_slice());
        let headers = rdr.headers().unwrap().clone();
        let mut records = rdr.records();
        let record = records.next().unwrap().unwrap();
        let parsed: ImportRow = record.deserialize(Some(&headers)).unwrap();

        assert_eq!(parsed.template_id, row.template_id);
        assert_eq!(parsed.partner_id, row.partner_id);
        assert_eq!(parsed.operator_id, "");
        assert_eq!(parsed.sender_id, row.sender_id);
        assert_eq!(parsed.channel, row.channel);
        assert_eq!(parsed.category, row.category);
        assert_eq!(parsed.pattern, row.pattern, "запятая и кавычка внутри pattern должны пережить round-trip без искажения");
        assert_eq!(parsed.version, "3");
        assert_eq!(parsed.status, row.status);
        assert!(records.next().is_none(), "ровно одна строка данных");
    }

    #[test]
    fn schema_validation_rejects_reserved_category() {
        let payload = json!({
            "template_id": Uuid::new_v4().to_string(),
            "partner_id": "acme",
            "operator_id": null,
            "sender_id": null,
            "channel": "SMS",
            "category": "UNTEMPLATED",
            "pattern": "some literal %w",
            "version": 1,
            "status": "active",
        });
        assert!(validate_payload(&payload).is_err(), "UNTEMPLATED зарезервирована, схема должна её отклонить");
    }

    #[test]
    fn schema_validation_accepts_well_formed_payload() {
        let payload = json!({
            "template_id": Uuid::new_v4().to_string(),
            "partner_id": "acme",
            "operator_id": null,
            "sender_id": null,
            "channel": "SMS",
            "category": "TRANSACTION",
            "pattern": "some literal %w",
            "version": 1,
            "status": "active",
        });
        assert!(validate_payload(&payload).is_ok());
    }

    // -----------------------------------------------------------------
    // DB-зависимые тесты (config.config_outbox / policy.policy_template).
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn valid_row_produces_exactly_one_outbox_row() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("valid");

        let csv_body = format!(
            "{CSV_HEADER_ORDER}\n,{partner_id},,,SMS,TRANSACTION,\"%w shartnoma bo'yicha %d{{1,6}} so'm\",1,active\n"
        );

        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/templates/import")
                    .header("content-type", "text/csv")
                    .body(Body::from(csv_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["imported"], 1);
        assert_eq!(body["failed"].as_array().unwrap().len(), 0);

        let rows: Vec<(String, String, JsonValue)> = sqlx::query_as(
            "SELECT entity_type, entity_id, payload FROM config.config_outbox WHERE payload->>'partner_id' = $1",
        )
        .bind(&partner_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1, "ровно одна outbox-запись для этого импорта");
        let (entity_type, entity_id, payload) = &rows[0];
        assert_eq!(entity_type, "policy_template");
        assert_eq!(payload["partner_id"], partner_id);
        assert_eq!(payload["category"], "TRANSACTION");
        assert_eq!(entity_id, payload["template_id"].as_str().unwrap(), "entity_id должен совпадать со сгенерированным template_id");

        cleanup(&pool, &partner_id).await;
    }

    #[tokio::test]
    async fn one_bad_row_does_not_block_the_rest_of_the_batch() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("mixed");

        // Строка 1: category=UNTEMPLATED — запрещена схемой (CHECK-эквивалент
        // из V013__policy_template.sql), должна провалиться. Строка 2:
        // валидная — должна пройти. Порядок специально "плохая, потом
        // хорошая", чтобы доказать, что первая ошибка не прерывает батч.
        let csv_body = format!(
            "{CSV_HEADER_ORDER}\n\
             ,{partner_id},,,SMS,UNTEMPLATED,\"%w bad category\",1,active\n\
             ,{partner_id},,,SMS,TRANSACTION,\"%w shartnoma bo'yicha %d{{1,6}} so'm\",1,active\n"
        );

        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/templates/import")
                    .header("content-type", "text/csv")
                    .body(Body::from(csv_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["imported"], 1, "ровно одна из двух строк должна импортироваться");
        assert_eq!(body["failed"].as_array().unwrap().len(), 1, "ровно одна должна провалиться, не 0 и не 2");
        assert!(body["failed"][0]["error"].as_str().unwrap().contains("UNTEMPLATED") || body["failed"][0]["error"].as_str().unwrap().len() > 0);

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM config.config_outbox WHERE payload->>'partner_id' = $1",
        )
        .bind(&partner_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 1, "только валидная строка должна была реально записаться в outbox");

        cleanup(&pool, &partner_id).await;
    }

    #[tokio::test]
    async fn pattern_with_no_literal_content_warns_without_failing_import() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("selectivity");

        // pattern = "%w" целиком — ни одного литерального фрагмента,
        // check_pattern_selectivity должна выдать ровно одно предупреждение
        // (см. template_matching.rs::selectivity_warns_on_pattern_with_no_literal_content),
        // но это не должно провалить импорт — схема этого не запрещает.
        let csv_body = format!("{CSV_HEADER_ORDER}\n,{partner_id},,,SMS,TRANSACTION,%w,1,active\n");

        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/templates/import")
                    .header("content-type", "text/csv")
                    .body(Body::from(csv_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["imported"], 1, "низкая селективность — предупреждение, не отказ");
        assert_eq!(body["failed"].as_array().unwrap().len(), 0);
        let warns = body["warnings"].as_array().unwrap();
        assert_eq!(warns.len(), 1);
        assert!(warns[0]["warnings"][0].as_str().unwrap().contains("литерального фрагмента"));

        cleanup(&pool, &partner_id).await;
    }

    #[tokio::test]
    async fn reimport_with_explicit_template_id_updates_same_entity() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("reimport");
        let template_id = Uuid::new_v4();

        let csv_body = format!(
            "{CSV_HEADER_ORDER}\n{template_id},{partner_id},,,SMS,TRANSACTION,\"%w v1\",1,active\n"
        );
        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/templates/import")
                    .header("content-type", "text/csv")
                    .body(Body::from(csv_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(to_json(response).await["imported"], 1);

        // Повторный импорт с ТЕМ ЖЕ template_id, другой версией/pattern —
        // должно использовать переданный id, не сгенерировать новый.
        let csv_body_v2 = format!(
            "{CSV_HEADER_ORDER}\n{template_id},{partner_id},,,SMS,TRANSACTION,\"%w v2 updated\",2,active\n"
        );
        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/templates/import")
                    .header("content-type", "text/csv")
                    .body(Body::from(csv_body_v2))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(to_json(response).await["imported"], 1);

        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT entity_id FROM config.config_outbox WHERE payload->>'partner_id' = $1 ORDER BY id",
        )
        .bind(&partner_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "два outbox-события — по одному на каждый импорт, проекция версия-гейтит сама");
        assert_eq!(rows[0].0, template_id.to_string());
        assert_eq!(rows[1].0, template_id.to_string(), "оба события должны нести один и тот же entity_id — обновление той же сущности, не создание новой");

        cleanup(&pool, &partner_id).await;
    }

    #[tokio::test]
    async fn export_requires_partner_id() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool };

        let app = router(store);
        let response = app
            .oneshot(Request::builder().uri("/v1/templates/export").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn export_returns_csv_content_type_and_correct_rows() {
        let Some(pool) = test_pool().await else { return };
        let store = Store { pool: pool.clone() };
        let partner_id = unique_partner_id("export");

        sqlx::query(
            "INSERT INTO policy.policy_template (partner_id, operator_id, sender_id, channel, category, pattern, version, status) \
             VALUES ($1, NULL, NULL, 'SMS', 'TRANSACTION', 'literal, with comma', 1, 'active')",
        )
        .bind(&partner_id)
        .execute(&pool)
        .await
        .unwrap();

        let app = router(store.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/templates/export?partner_id={partner_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::CONTENT_TYPE).unwrap(), "text/csv");
        assert!(response.headers().get(header::CONTENT_DISPOSITION).unwrap().to_str().unwrap().contains("templates.csv"));

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(body.as_ref());
        let headers = rdr.headers().unwrap().clone();
        assert_eq!(headers.iter().collect::<Vec<_>>().join(","), CSV_HEADER_ORDER);

        let records: Vec<csv::StringRecord> = rdr.records().collect::<Result<_, _>>().unwrap();
        assert_eq!(records.len(), 1);
        let row: ImportRow = records[0].deserialize(Some(&headers)).unwrap();
        assert_eq!(row.partner_id, partner_id);
        assert_eq!(row.pattern, "literal, with comma", "запятая в pattern не должна ломать колонки при экспорте");
        assert_eq!(row.operator_id, "", "NULL operator_id -> пустая строка в CSV");

        cleanup(&pool, &partner_id).await;
    }
}
