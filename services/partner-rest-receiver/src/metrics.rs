//! `BACKOFFICE_ROADMAP.md` P1 "Observability": заменяет прежнюю `/metrics`-
//! заглушку (`partner_rest_receiver_up 1`, ничего больше, см. `health.rs`)
//! реальными счётчиком/гистограммой хот-пути (`POST /v1/messages`, см.
//! `http.rs::handle_send_message`). Ни один Rust-сервис в этом репозитории
//! раньше не тянул metrics-библиотеку — `prometheus` (`prometheus.io`
//! официальный Rust client) выбран как промышленный стандарт для этой
//! экосистемы, не переизобретённый текстовый формат вручную (сравнимо с тем,
//! почему Go-сторона этой же сессии взяла `client_golang`, а не
//! самодельный writer).
//!
//! Использует дефолтный глобальный `Registry` крейта `prometheus`
//! (`prometheus::gather()`/`register_*!` макросы) — с одним HTTP-сервером на
//! процесс (см. `health.rs`, порт 9090) отдельный `Registry` не даёт
//! никакой изоляции, только лишний параметр, который пришлось бы протаскивать
//! через `AppState`/`HealthState` сразу в двух местах (`http.rs` для записи,
//! `health.rs` для чтения).

use prometheus::{Encoder, HistogramVec, IntCounterVec, TextEncoder, register_histogram_vec, register_int_counter_vec};
use std::sync::LazyLock;

/// `IngestRequestsTotal` — каждый ответ `POST /v1/messages`, по HTTP-статусу
/// (низкая кардинальность — `error_response_parts` в `http.rs` уже
/// перечисляет закрытый набор кодов: 200/400/401/403/413/429/503).
pub static INGEST_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "partner_rest_receiver_ingest_requests_total",
        "Общее число ответов POST /v1/messages, по HTTP-статусу.",
        &["status"]
    )
    .expect("register partner_rest_receiver_ingest_requests_total")
});

/// `IngestRequestDuration` — латентность `handle_send_message` целиком,
/// включая `authorize_and_admit` (может делать реальный Vault-round-trip на
/// cache miss, см. `vault_auth.rs`) и `publish_incoming` в Kafka.
pub static INGEST_REQUEST_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "partner_rest_receiver_ingest_request_duration_seconds",
        "Латентность POST /v1/messages целиком (auth + admission + rate-limit + Kafka publish).",
        &["status"]
    )
    .expect("register partner_rest_receiver_ingest_request_duration_seconds")
});

/// `gather_text` — Prometheus text exposition format поверх дефолтного
/// registry, вызывается `health.rs`'s `/metrics` handler'ом.
pub fn gather_text() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .expect("encode prometheus metrics");
    String::from_utf8(buffer).expect("prometheus text format is valid utf-8")
}
