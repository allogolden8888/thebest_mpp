//! Оркестрация всего `service_internal_methods.md` §1.1: `authorize_and_admit`
//! (validate/auth/IP/admission/rate-limit) + тонкий Axum-обработчик, который
//! добавляет остальной async I/O (`publish_incoming` и т.д.). Тот же паттерн
//! "ядро + тонкая обвязка", что и `handle_command` у остальных сервисов этого
//! среза — но, в отличие от них, ядро больше не полностью синхронное: с
//! появлением `VaultAuthVerifier` (`vault_auth.rs`) `auth_verifier.verify`
//! может делать реальный сетевой I/O (Vault read на cache miss), поэтому
//! `authorize_and_admit` теперь `async fn`. Остаётся тестируемым без реальной
//! сети — `AuthVerifier` тестового дубля (`AcceptAnyKey` ниже) ничего не
//! ждёт, `#[tokio::test]` вместо `#[test]` — единственное отличие для тестов.

use crate::admission::{AdmissionDecision, AdmissionGate};
use crate::auth::AuthVerifier;
use crate::build_incoming::{DEFAULT_MESSAGE_TTL, build_incoming_message, generate_message_id, generate_trace_id};
use crate::idempotency::{self, ClaimOutcome, ClaimedIds};
use crate::kafka_io::{self, PublishError};
use crate::partner_config::PartnerSnapshot;
use crate::rate_limit::RateLimiter;
use crate::ip_allowlist;
use crate::msgctx;
use crate::request::{RawRequest, ValidatedRequest, ValidationError, validate_request_schema};
use crate::segmentation::compute_segments;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use rdkafka::producer::FutureProducer;
use redis::aio::MultiplexedConnection;
use serde::Serialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;

pub struct AppState {
    pub partner_snapshot: PartnerSnapshot,
    pub auth_verifier: Box<dyn AuthVerifier>,
    pub admission_gate: Box<dyn AdmissionGate>,
    pub rate_limiter: RateLimiter,
    pub producer: FutureProducer,
    /// Один `MultiplexedConnection`, созданный при старте, клонируется на
    /// каждый запрос (клон дешёвый — общий хендл на одно и то же
    /// TCP-соединение, не новое подключение). См. `msgctx::write` за разбором
    /// находки (1500 TPS push, `strace -c`): раньше здесь была голая
    /// `redis_runtime_url: String`, и `msgctx::write`/`idempotency::claim`
    /// открывали свежее TCP-соединение + Redis AUTH на КАЖДЫЙ запрос —
    /// ~90% времени в syscall'ах уходило на socket/connect/close, что и
    /// объясняло 578-684% CPU этого сервиса при 1500 TPS.
    pub redis_conn: MultiplexedConnection,
    /// MEDIUM находка кодревью: без этого деградированный/недоступный Kafka
    /// (до `REQUEST_TIMEOUT` держит каждый in-flight запрос) не имел ВООБЩЕ
    /// никакого ограничения на количество одновременно удерживаемых
    /// запросов — см. `MAX_CONCURRENT_REQUESTS`. `try_acquire_owned`
    /// (не `.acquire().await`) намеренно — выше лимита сразу 503, не
    /// неограниченная очередь, которая свела бы защиту на нет.
    pub concurrency_limit: Arc<Semaphore>,
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
pub async fn authorize_and_admit(
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
    if !auth_verifier.verify(application, &validated.api_key).await {
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

/// Реальный remote IP: приоритет `X-Forwarded-For`, иначе TCP peer address
/// из `ConnectInfo`. Известное ограничение: только IPv4 (см. `ip_allowlist.rs`
/// — `partner.schema.json` тоже только IPv4 CIDR); IPv6-клиент за прокси без
/// `X-Forwarded-For` даст `MissingRemoteIp`, не паникует.
///
/// MEDIUM находка кодревью: раньше брался ПЕРВЫЙ (левый) адрес — безопасно
/// только пока ingress-nginx *перезаписывает* заголовок целиком
/// (`use-forwarded-headers: false`, дефолт Helm-чарта, но нигде явно не
/// закреплённый в Terraform) — если это когда-нибудь сменится на
/// дозапись/проксирование через доп. слой (CDN/WAF), левый адрес станет
/// тем, что прислал сам клиент, то есть подделываемым. Берём ПОСЛЕДНИЙ
/// (правый) адрес — тот, что дописала наша собственная инфраструктура
/// (единственный доверенный hop, ingress-nginx) непосредственно перед тем,
/// как запрос попал сюда — не то, что мог заявить о себе клиент. В текущем
/// режиме "перезапись" единственная запись в заголовке одна и та же что
/// слева, что справа — поведение не меняется; но если ingress однажды
/// начнёт дописывать, а не перезаписывать, эта защита не даёт клиенту
/// подделать allowlist-проверку через собственный `X-Forwarded-For`.
fn extract_remote_ip(headers: &HeaderMap, peer: SocketAddr) -> Option<Ipv4Addr> {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(last) = xff.split(',').next_back() {
            if let Ok(ip) = last.trim().parse::<Ipv4Addr>() {
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
    // Concurrency limit — см. AppState::concurrency_limit. Захватывается
    // ДО любой другой работы (даже до auth) — цель ограничить сырые
    // одновременные запросы на реплику, не "валидную работу"; permit держится
    // до конца функции (Drop освобождает слот).
    let _permit = match state.concurrency_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "TOO_MANY_CONCURRENT_REQUESTS".to_string() }))
                .into_response();
        }
    };

    let raw = RawRequest {
        partner_id: header_string(&headers, "x-partner-id"),
        application_id: header_string(&headers, "x-application-id"),
        api_key: header_string(&headers, "x-api-key"),
        remote_ip: extract_remote_ip(&headers, peer),
        body_json: body,
        idempotency_key: header_string(&headers, "x-idempotency-key"),
        // Фаза 11 плана закрытия API-пробелов — dry-run отправка. Только
        // литеральное "true" включает sandbox: отсутствие заголовка,
        // "false", "1", опечатка и т.п. — всё безопасно трактуется как
        // false, не пытаемся угадывать намерение партнёра из мусора.
        sandbox: header_string(&headers, "x-sandbox").as_deref() == Some("true"),
    };

    let validated = match authorize_and_admit(
        &raw,
        &state.partner_snapshot,
        state.auth_verifier.as_ref(),
        state.admission_gate.as_ref(),
        &state.rate_limiter,
    )
    .await
    {
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
            state.redis_conn.clone(),
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

    // Реальная находка (только прогоном локального docker-compose, см.
    // msgctx.rs) — ни один сервис в репозитории не писал msgctx:{message_id}
    // в Runtime Redis, хотя все downstream-стадии его читают. Пишем ДО
    // Kafka publish, чтобы к моменту, когда pipeline-engine/policy-service
    // реально дойдут до чтения, запись уже гарантированно существовала.
    let segments = compute_segments(&validated.body);
    let ctx = msgctx::MessageContext {
        message_id: &message_id,
        body: &validated.body,
        sender_id: &validated.sender_id,
        msisdn: &validated.msisdn,
        encoding: segments.encoding.as_str(),
        partner_id: &validated.partner_id,
        segment_count: segments.segment_count,
    };
    msgctx::write(state.redis_conn.clone(), &ctx, DEFAULT_MESSAGE_TTL.as_secs()).await;

    // MEDIUM находка кодревью: Kafka publish (5с producer-queue wait + 5с
    // delivery timeout, до ~10с суммарно) раньше awaited'ился без верхней
    // границы на уровне этого сервиса — деградировавший/недоступный Kafka
    // мог держать запрос неограниченно долго (сами внутренние 5с+5с — это
    // таймауты librdkafka на КАЖДУЮ отдельную попытку, не гарантия, что
    // publish_incoming вернётся за 10с суммарно при повторных внутренних
    // ретраях). `REQUEST_TIMEOUT` — верхняя граница поверх этого.
    match tokio::time::timeout(REQUEST_TIMEOUT, kafka_io::publish_incoming(&state.producer, &incoming)).await {
        Ok(Ok(())) => (StatusCode::ACCEPTED, Json(AckResponse { message_id, trace_id })).into_response(),
        Ok(Err(PublishError::Kafka(e))) => {
            tracing::error!("не удалось опубликовать IncomingMessage {message_id}: {e}");
            (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "PUBLISH_FAILED".to_string() })).into_response()
        }
        Err(_elapsed) => {
            tracing::error!("публикация IncomingMessage {message_id} превысила REQUEST_TIMEOUT={REQUEST_TIMEOUT:?}");
            (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "PUBLISH_TIMEOUT".to_string() })).into_response()
        }
    }
}

/// MEDIUM находка кодревью: раньше не было ни таймаута на весь путь
/// Kafka-публикации, ни ограничения на количество одновременно удерживаемых
/// запросов, ни переопределения дефолтного 2МБ body-лимита axum —
/// деградировавший/недоступный Kafka мог держать каждый in-flight запрос
/// до ~10с (5с producer-queue wait + 5с delivery timeout, оба await'ятся
/// прямо в handler'е) без circuit breaker'а и без верхней границы на
/// количество таких запросов сразу.
///
/// `MAX_BODY_BYTES` — 64КиБ, с большим запасом: `body` ограничено 1600
/// символами (`request.rs`), `msisdn`/`sender_id` — короткие строки,
/// реалистичный JSON-конверт на порядок меньше; всё ещё на два порядка
/// меньше дефолтных 2МБ axum, которые CODE_REVIEW.md отметило как
/// эксплуатируемые вместе с находкой про `sender_id` (закрыта отдельно,
/// см. `request.rs`).
const MAX_BODY_BYTES: usize = 64 * 1024;
/// `REQUEST_TIMEOUT` — с запасом выше худшего случая Kafka-пути (~10с),
/// чтобы не резать легитимные, просто медленные запросы, но всё же
/// ограничивать, насколько долго деградировавший Kafka может держать
/// соединение открытым — см. использование вокруг `kafka_io::publish_incoming`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// `MAX_CONCURRENT_REQUESTS` — верхняя граница одновременно удерживаемых
/// in-flight запросов на реплику (см. `AppState::concurrency_limit`); при
/// деградированном Kafka (каждый запрос держится до `REQUEST_TIMEOUT`) не
/// даёт памяти/файловым дескрипторам расти неограниченно — запросы сверх
/// лимита получают немедленный 503 вместо неограниченной очереди.
pub const MAX_CONCURRENT_REQUESTS: usize = 1024;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/messages", post(handle_send_message))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::AlwaysAdmit;
    use crate::auth::AuthVerifier;
    use crate::partner_config::{Application, AuthConfig, Partner};

    struct AcceptAnyKey;
    impl AuthVerifier for AcceptAnyKey {
        fn verify<'a>(&'a self, _application: &'a Application, provided_key: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
            Box::pin(async move { provided_key == "correct-key" })
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
            sandbox: false,
        }
    }

    #[tokio::test]
    async fn happy_path_authorized() {
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wrong_api_key_rejected_as_auth_failed() {
        let mut r = raw();
        r.api_key = Some("wrong-key".into());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
        assert_eq!(result.unwrap_err(), HandlerError::AuthFailed);
    }

    #[tokio::test]
    async fn unknown_partner_rejected_as_auth_failed_not_leaking_existence() {
        let mut r = raw();
        r.partner_id = Some("unknown_partner".into());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
        assert_eq!(result.unwrap_err(), HandlerError::AuthFailed);
    }

    #[tokio::test]
    async fn suspended_partner_rejected() {
        let mut snap = snapshot();
        snap = PartnerSnapshot::from_partners(vec![Partner {
            partner_id: "click_uz".into(),
            version: 1,
            status: "suspended".into(),
            applications: snap.application("click_uz", "click_uz_main").unwrap().0.applications.clone(),
        }]);
        let result = authorize_and_admit(&raw(), &snap, &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
        assert_eq!(result.unwrap_err(), HandlerError::PartnerNotActive);
    }

    #[tokio::test]
    async fn ip_outside_allowlist_rejected() {
        let mut r = raw();
        r.remote_ip = Some("8.8.8.8".parse().unwrap());
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
        assert_eq!(result.unwrap_err(), HandlerError::IpOrChannelDenied);
    }

    #[tokio::test]
    async fn admission_reject_propagates_retry_after() {
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysReject(30), &RateLimiter::default()).await;
        assert_eq!(result.unwrap_err(), HandlerError::AdmissionRejected { retry_after_seconds: 30 });
    }

    /// Прямое доказательство исправления HIGH находки кодревью: до фикса
    /// неограниченное число попыток с неверным ключом против ИЗВЕСТНОЙ пары
    /// (partner_id/application_id — не секретны) все возвращались AuthFailed
    /// без единого throttling. Теперь после headroom-ёмкости (rate_limit_tps=5
    /// * 4 = 20) попытки подбора начинают получать AuthRateLimited — конечная,
    /// не бесконечная попытка перебора.
    #[tokio::test]
    async fn brute_force_against_known_pair_with_wrong_key_is_eventually_throttled() {
        let limiter = RateLimiter::default();
        let mut wrong_key_request = raw();
        wrong_key_request.api_key = Some("guessed-wrong-key".into());

        let mut auth_failed_count = 0;
        let mut auth_rate_limited_count = 0;
        for _ in 0..30 {
            match authorize_and_admit(&wrong_key_request, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter).await {
                Err(HandlerError::AuthFailed) => auth_failed_count += 1,
                Err(HandlerError::AuthRateLimited) => auth_rate_limited_count += 1,
                other => panic!("ожидали AuthFailed или AuthRateLimited, получили {other:?}"),
            }
        }
        assert_eq!(auth_failed_count, 20, "ровно headroom-ёмкость (5 * 4) попыток должна дойти до реальной проверки ключа");
        assert_eq!(auth_rate_limited_count, 10, "остальные попытки в этом окне должны быть отклонены throttling'ом, не тратить CPU на сравнение ключа");
    }

    #[tokio::test]
    async fn legitimate_traffic_at_configured_tps_never_sees_auth_rate_limited() {
        let limiter = RateLimiter::default();
        // rate_limit_tps=5 в snapshot() -> message bucket capacity=1 (см.
        // MESSAGE_BURST_HEADROOM_FACTOR: max(5*0.15, 1.0)=1) — ровно 1
        // мгновенный запрос должен пройти (не AuthRateLimited). Значение
        // capacity — из main (1500 TPS load-test push), вызов через .await —
        // из subagent-1 (authorize_and_admit стал async ради VaultAuthVerifier,
        // который делает реальный сетевой вызов; слияние двух веток).
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter).await;
        assert!(result.is_ok(), "легитимный запрос в пределах собственного rate_limit_tps не должен быть отклонён");
    }

    #[tokio::test]
    async fn rate_limit_exhausted_after_configured_tps() {
        let limiter = RateLimiter::default();
        // rate_limit_tps=5 -> message bucket capacity=1 (см. MESSAGE_BURST_HEADROOM_FACTOR).
        assert!(authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter).await.is_ok());
        let result = authorize_and_admit(&raw(), &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &limiter).await;
        assert_eq!(result.unwrap_err(), HandlerError::RateLimited);
    }

    #[tokio::test]
    async fn validation_error_propagates_with_original_reason() {
        let mut r = raw();
        r.body_json = "not json".into();
        let result = authorize_and_admit(&r, &snapshot(), &AcceptAnyKey, &AlwaysAdmit, &RateLimiter::default()).await;
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
        // Однозначный (одноэлементный) X-Forwarded-For — режим "перезапись"
        // ingress-nginx (текущий деплой) — единственная запись доверенная.
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "185.65.212.9".parse().unwrap());
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        assert_eq!(extract_remote_ip(&headers, peer), Some("185.65.212.9".parse().unwrap()));
    }

    /// Регрессия на MEDIUM находку кодревью: если бы брался ЛЕВЫЙ адрес,
    /// клиент мог бы прислать `X-Forwarded-For: <произвольный IP>` и
    /// обойти allowlist в сценарии, где ingress однажды начнёт дописывать
    /// в заголовок, а не перезаписывать его. Правый адрес — тот, что
    /// дописала бы наша инфраструктура (не клиент) — должен побеждать.
    #[test]
    fn extract_remote_ip_trusts_rightmost_hop_not_client_claimed_leftmost() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "6.6.6.6, 10.0.0.1".parse().unwrap());
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        assert_eq!(
            extract_remote_ip(&headers, peer),
            Some("10.0.0.1".parse().unwrap()),
            "должен доверять ПОСЛЕДНЕМУ (дописанному нашей инфраструктурой) адресу, не первому (клиентскому)"
        );
    }

    #[test]
    fn extract_remote_ip_falls_back_to_peer_without_header() {
        let headers = HeaderMap::new();
        let peer: SocketAddr = "10.1.2.3:12345".parse().unwrap();
        assert_eq!(extract_remote_ip(&headers, peer), Some("10.1.2.3".parse().unwrap()));
    }
}
