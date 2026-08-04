//! Оркестрация всего `service_internal_methods.md` §1.1: разделено на
//! `authorize_and_admit` — чистая синхронная функция (validate/auth/IP/
//! admission/rate-limit, тестируется без сети) и тонкий Axum-обработчик,
//! который добавляет только реальный async I/O (`publish_incoming`).
//! Тот же паттерн "чистое ядро + тонкая обвязка", что и `handle_command`
//! у остальных сервисов этого среза.

use crate::admission::{AdmissionDecision, AdmissionGate};
use crate::auth::AuthVerifier;
use crate::build_incoming::{DEFAULT_MESSAGE_TTL, build_incoming_message, generate_message_id, generate_trace_id};
use crate::idempotency::{self, ClaimOutcome, ClaimedIds};
use crate::kafka_io::{self, PublishError};
use crate::partner_config::PartnerSnapshot;
use crate::rate_limit::RateLimiter;
use crate::ip_allowlist;
use crate::request::{RawRequest, ValidatedRequest, ValidationError, validate_request_schema};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use rdkafka::producer::FutureProducer;
use serde::Serialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::SystemTime;

pub struct AppState {
    pub partner_snapshot: PartnerSnapshot,
    pub auth_verifier: Box<dyn AuthVerifier>,
    pub admission_gate: Box<dyn AdmissionGate>,
    pub rate_limiter: RateLimiter,
    pub producer: FutureProducer,
    pub redis_runtime_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HandlerError {
    Validation(ValidationError),
    AuthFailed,
    AuthRateLimited,
    PartnerNotActive,
    IpOrChannelDenied,
    AdmissionRejected { retry_after_seconds: u32 },
    RateLimited,
}

/// `authenticate_partner` -> `check_ip_and_application` -> `check_admission`
/// -> `check_rate_limit`, в этом порядке (`service_internal_methods.md` §1.1,
/// строки 20-23) — тот же порядок для УСПЕШНОГО запроса. Отклонение от
/// документа, исправляющее HIGH находку кодревью: `try_consume_auth_attempt`
/// вставлен ПЕРЕД собственно `authenticate_partner` (API-key сравнением) —
/// `partner_id`/`application_id` не секретны (обычные заголовки), поэтому без
/// этой проверки запрос с неверным/подобранным ключом обходил единственный
/// rate limit в системе (тот, что после успешной аутентификации) и мог
/// повторяться неограниченно. Использует ОТДЕЛЬНЫЙ bucket от
/// `check_rate_limit` ниже (см. `RateLimiter::try_consume_auth_attempt`) —
/// легитимный трафик того же партнёра не наказывается чужими попытками
/// подбора ключа.
pub fn authorize_and_admit(
    raw: &RawRequest,
    snapshot: &PartnerSnapshot,
    auth_verifier: &dyn AuthVerifier,
    admission_gate: &dyn AdmissionGate,
    rate_limiter: &RateLimiter,
) -> Result<ValidatedRequest, HandlerError> {
    let validated = validate_request_schema(raw).map_err(HandlerError::Validation)?;

    let (partner, application) =
        snapshot.application(&validated.partner_id, &validated.application_id).ok_or(HandlerError::AuthFailed)?;

    if !rate_limiter.try_consume_auth_attempt(&validated.partner_id, &validated.application_id, application.rate_limit_tps) {
        return Err(HandlerError::AuthRateLimited);
    }

    if !partner.is_active() {
        return Err(HandlerError::PartnerNotActive);
    }
    if !auth_verifier.verify(application, &validated.api_key) {
        return Err(HandlerError::AuthFailed);
    }

    let ip_ok = ip_allowlist::ip_allowed(&application.ip_allowlist, validated.remote_ip);
    let channel_ok = ip_allowlist::channel_allowed(&application.allowed_channels, "SMS");
    if !ip_ok || !channel_ok {
        return Err(HandlerError::IpOrChannelDenied);
    }

    if let AdmissionDecision::Reject { retry_after_seconds } = admission_gate.check(&validated.partner_id) {
        return Err(HandlerError::AdmissionRejected { retry_after_seconds });
    }

    if !rate_limiter.try_consume(&validated.partner_id, &validated.application_id, application.rate_limit_tps) {
        return Err(HandlerError::RateLimited);
    }

    Ok(validated)
}

#[derive(Debug, Serialize)]
pub struct AckResponse {
    pub message_id: String,
    pub trace_id: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// `build_ack_response` для ошибочных путей — статус + тело + опциональный
/// `Retry-After` (только для admission-reject, `service_io_contracts.md` §1.1:
/// "429/503 при admission control").
pub fn error_response_parts(err: &HandlerError) -> (StatusCode, Option<u32>, &'static str) {
    match err {
        HandlerError::Validation(_) => (StatusCode::BAD_REQUEST, None, "VALIDATION_FAILED"),
        HandlerError::AuthFailed => (StatusCode::UNAUTHORIZED, None, "AUTH_FAILED"),
        HandlerError::AuthRateLimited => (StatusCode::TOO_MANY_REQUESTS, None, "AUTH_RATE_LIMITED"),
        HandlerError::PartnerNotActive => (StatusCode::UNAUTHORIZED, None, "PARTNER_NOT_ACTIVE"),
        HandlerError::IpOrChannelDenied => (StatusCode::FORBIDDEN, None, "IP_OR_CHANNEL_DENIED"),
        HandlerError::AdmissionRejected { retry_after_seconds } => {
            (StatusCode::SERVICE_UNAVAILABLE, Some(*retry_after_seconds), "ADMISSION_REJECTED")
        }
        HandlerError::RateLimited => (StatusCode::TOO_MANY_REQUESTS, None, "RATE_LIMITED"),
    }
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string).filter(|s| !s.is_empty())
}

/// Реальный remote IP: приоритет `X-Forwarded-For` (первый адрес — клиент,
/// стандартная конвенция за reverse-proxy/ingress), иначе TCP peer address
/// из `ConnectInfo`. Известное ограничение: только IPv4 (см. `ip_allowlist.rs`
/// — `partner.schema.json` тоже только IPv4 CIDR); IPv6-клиент за прокси без
/// `X-Forwarded-For` даст `MissingRemoteIp`, не паникует.
fn extract_remote_ip(headers: &HeaderMap, peer: SocketAddr) -> Option<Ipv4Addr> {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = xff.split(',').next() {
            if let Ok(ip) = first.trim().parse::<Ipv4Addr>() {
                return Some(ip);
            }
        }
    }
    match peer.ip() {
        std::net::IpAddr::V4(v4) => Some(v4),
        std::net::IpAddr::V6(_) => None,
    }
}

async fn handle_send_message(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let raw = RawRequest {
        partner_id: header_string(&headers, "x-partner-id"),
        application_id: header_string(&headers, "x-application-id"),
        api_key: header_string(&headers, "x-api-key"),
        remote_ip: extract_remote_ip(&headers, peer),
        body_json: body,
        idempotency_key: header_string(&headers, "x-idempotency-key"),
    };

    let validated = match authorize_and_admit(
        &raw,
        &state.partner_snapshot,
        state.auth_verifier.as_ref(),
        state.admission_gate.as_ref(),
        &state.rate_limiter,
    ) {
        Ok(v) => v,
        Err(e) => {
            let (status, retry_after, error) = error_response_parts(&e);
            let mut response = (status, Json(ErrorResponse { error: error.to_string() })).into_response();
            if let Some(seconds) = retry_after {
                response.headers_mut().insert("retry-after", seconds.into());
            }
            return response;
        }
    };

    let message_id = generate_message_id();
    let trace_id = generate_trace_id();

    // Исправление HIGH находки кодревью: partner-инициированный retry с тем
    // же X-Idempotency-Key переиспользует уже опубликованный message_id/trace_id
    // вместо повторной публикации (что привело бы к дублю SMS/billing). См. `idempotency.rs`.
    if let Some(idempotency_key) = &validated.idempotency_key {
        let claimed = ClaimedIds { message_id: message_id.clone(), trace_id: trace_id.clone() };
        match idempotency::claim(
            &state.redis_runtime_url,
            &validated.partner_id,
            &validated.application_id,
            idempotency_key,
            &claimed,
            DEFAULT_MESSAGE_TTL.as_secs(),
        )
        .await
        {
            ClaimOutcome::AlreadyClaimed(existing) => {
                return (StatusCode::ACCEPTED, Json(AckResponse { message_id: existing.message_id, trace_id: existing.trace_id }))
                    .into_response();
            }
            ClaimOutcome::Won | ClaimOutcome::Unavailable => {
                // Won — первая попытка с этим ключом, публикуем как обычно.
                // Unavailable — Redis недоступен, best-effort: не блокируем
                // ingress, публикуем как если бы idempotency_key не был передан.
            }
        }
    }

    let incoming = build_incoming_message(&validated, message_id.clone(), trace_id.clone(), SystemTime::now());

    match kafka_io::publish_incoming(&state.producer, &incoming).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(AckResponse { message_id, trace_id })).into_response(),
        Err(PublishError::Kafka(e)) => {
            tracing::error!("не удалось опубликовать IncomingMessage {message_id}: {e}");
            (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "PUBLISH_FAILED".to_string() })).into_response()
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new().route("/v1/messages", post(handle_send_message)).with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::AlwaysAdmit;
    use crate::auth::AuthVerifier;
    use crate::partner_config::{Application, AuthConfig, Partner};

    struct AcceptAnyKey;
    impl AuthVerifier for AcceptAnyKey {
        fn verify(&self, _application: &Application, provided_key: &str) -> bool {
            provided_key == "correct-key"
        }
    }

    struct AlwaysReject(u32);
    impl AdmissionGate for AlwaysReject {
        fn check(&self, _partner_id: &str) -> AdmissionDecision {
            AdmissionDecision::Reject { retry_after_seconds: self.0 }
        }
    }

    fn snapshot() -> PartnerSnapshot {
        PartnerSnapshot::from_partners(vec![Partner {
            partner_id: "click_uz".into(),
            version: 1,
            status: "active".into(),
            applications: vec![Application {
                application_id: "click_uz_main".into(),
                display_name: "Click".into(),
                auth: AuthConfig { auth_type: "API_KEY".into(), credential_ref: "vault://x".into() },
                ip_allowlist: vec!["185.65.212.0/24".into()],
                rate_limit_tps: 5,
                allowed_channels: vec!["SMS".into()],
            }],
        }])
    }

    fn raw() -> RawRequest {
        RawRequest {
            partner_id: Some("click_uz".into()),
            application_id: Some("click_uz_main".into()),
            api_key: Some("correct-key".into()),
            remote_ip: Some("185.65.212.55".parse().unwrap()),
            body_json: r#"{"msisdn":"998901331835","sender_id":"Click","body":"OTP 123456"}"#.into(),
            idempotency_key: None,
        }
    }

    #[test]
    fn happy_path_authorized() {
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert!(result.is_ok());
    }

    #[test]
    fn wrong_api_key_rejected_as_auth_failed() {
        let mut r = raw();
        r.api_key = Some("wrong-key".into());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert_eq!(result.unwrap_err(), HandlerError::AuthFailed);
    }

    #[test]
    fn unknown_partner_rejected_as_auth_failed_not_leaking_existence() {
        let mut r = raw();
        r.partner_id = Some("unknown_partner".into());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert_eq!(result.unwrap_err(), HandlerError::AuthFailed);
    }

    #[test]
    fn suspended_partner_rejected() {
        let mut snap = snapshot();
        snap = PartnerSnapshot::from_partners(vec![Partner {
            partner_id: "click_uz".into(),
            version: 1,
            status: "suspended".into(),
            applications: snap.application("click_uz", "click_uz_main").unwrap().0.applications.clone(),
        }]);
        let result = authorize_and_admit(&raw(), &snap, &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert_eq!(result.unwrap_err(), HandlerError::PartnerNotActive);
    }

    #[test]
    fn ip_outside_allowlist_rejected() {
        let mut r = raw();
        r.remote_ip = Some("8.8.8.8".parse().unwrap());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert_eq!(result.unwrap_err(), HandlerError::IpOrChannelDenied);
    }

    #[test]
    fn admission_reject_propagates_retry_after() {
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysReject(30), &RateLimiter::default());
        assert_eq!(result.unwrap_err(), HandlerError::AdmissionRejected { retry_after_seconds: 30 });
    }

    /// Прямое доказательство исправления HIGH находки кодревью: до фикса
    /// неограниченное число попыток с неверным ключом против ИЗВЕСТНОЙ пары
    /// (partner_id/application_id — не секретны) все возвращались AuthFailed
    /// без единого throttling. Теперь после headroom-ёмкости (rate_limit_tps=5
    /// * 4 = 20) попытки подбора начинают получать AuthRateLimited — конечная,
    /// не бесконечная попытка перебора.
    #[test]
    fn brute_force_against_known_pair_with_wrong_key_is_eventually_throttled() {
        let limiter = RateLimiter::default();
        let mut wrong_key_request = raw();
        wrong_key_request.api_key = Some("guessed-wrong-key".into());

        let mut auth_failed_count = 0;
        let mut auth_rate_limited_count = 0;
        for _ in 0..30 {
            match authorize_and_admit(&wrong_key_request, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter) {
                Err(HandlerError::AuthFailed) => auth_failed_count += 1,
                Err(HandlerError::AuthRateLimited) => auth_rate_limited_count += 1,
                other => panic!("ожидали AuthFailed или AuthRateLimited, получили {other:?}"),
            }
        }
        assert_eq!(auth_failed_count, 20, "ровно headroom-ёмкость (5 * 4) попыток должна дойти до реальной проверки ключа");
        assert_eq!(auth_rate_limited_count, 10, "остальные попытки в этом окне должны быть отклонены throttling'ом, не тратить CPU на сравнение ключа");
    }

    #[test]
    fn legitimate_traffic_at_configured_tps_never_sees_auth_rate_limited() {
        let limiter = RateLimiter::default();
        // rate_limit_tps=5 в snapshot() — ровно 5 легитимных запросов подряд
        // должны все пройти (последний — успешно, не AuthRateLimited).
        for i in 0..5 {
            let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter);
            assert!(result.is_ok(), "легитимный запрос {i} в пределах собственного rate_limit_tps не должен быть отклонён");
        }
    }

    #[test]
    fn rate_limit_exhausted_after_configured_tps() {
        let limiter = RateLimiter::default();
        for _ in 0..5 {
            assert!(authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter).is_ok());
        }
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter);
        assert_eq!(result.unwrap_err(), HandlerError::RateLimited);
    }

    #[test]
    fn validation_error_propagates_with_original_reason() {
        let mut r = raw();
        r.body_json = "not json".into();
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default());
        assert!(matches!(result.unwrap_err(), HandlerError::Validation(_)));
    }

    #[test]
    fn error_response_parts_maps_each_error_to_documented_status() {
        assert_eq!(error_response_parts(&HandlerError::Validation(ValidationError::EmptyBody)).0, StatusCode::BAD_REQUEST);
        assert_eq!(error_response_parts(&HandlerError::AuthFailed).0, StatusCode::UNAUTHORIZED);
        assert_eq!(error_response_parts(&HandlerError::IpOrChannelDenied).0, StatusCode::FORBIDDEN);
        assert_eq!(error_response_parts(&HandlerError::RateLimited).0, StatusCode::TOO_MANY_REQUESTS);
        let (status, retry_after, _) = error_response_parts(&HandlerError::AdmissionRejected { retry_after_seconds: 12 });
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(retry_after, Some(12));
    }

    #[test]
    fn extract_remote_ip_prefers_x_forwarded_for_over_peer() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "185.65.212.9, 10.0.0.1".parse().unwrap());
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        assert_eq!(extract_remote_ip(&headers, peer), Some("185.65.212.9".parse().unwrap()));
    }

    #[test]
    fn extract_remote_ip_falls_back_to_peer_without_header() {
        let headers = HeaderMap::new();
        let peer: SocketAddr = "10.1.2.3:12345".parse().unwrap();
        assert_eq!(extract_remote_ip(&headers, peer), Some("10.1.2.3".parse().unwrap()));
    }
}
