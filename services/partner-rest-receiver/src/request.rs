//! `validate_request_schema` (service_internal_methods.md §1.1) — чистая
//! функция, отделена от Axum-экстракторов в `http.rs`, тестируется без сети.
//! REST-контракт (заголовки/JSON-поля) нигде не специфицирован дословно ни в
//! одном документе (`service_io_contracts.md` §1.1 описывает данные текстом:
//! "партнёрские креды, msisdn/sender, тело, application_id"), поэтому здесь —
//! конкретное, разумное решение, не выдумка без обоснования:
//! `X-Partner-Id`/`X-Application-Id`/`X-Api-Key` заголовки (симметрично тому,
//! как SMPP `system_id`/`password`/`system_type` идентифицируют сессию у
//! Partner SMPP Gateway) + JSON-тело `{msisdn, sender_id, body}`.

use std::net::Ipv4Addr;

#[derive(Debug, Clone)]
pub struct RawRequest {
    pub partner_id: Option<String>,
    pub application_id: Option<String>,
    pub api_key: Option<String>,
    pub remote_ip: Option<Ipv4Addr>,
    pub body_json: String,
    /// `X-Idempotency-Key` — опциональный, партнёр сам генерирует. См. `idempotency.rs`.
    pub idempotency_key: Option<String>,
    /// `X-Sandbox: true` (Фаза 11 плана закрытия API-пробелов) — dry-run
    /// отправка: не тарифицируется, не уходит реальному оператору, lifecycle
    /// всё равно доходит до DELIVERED через синтетический DLR
    /// (delivery-service). Уже bool на этом уровне (не Option<String>, как
    /// idempotency_key) — здесь нечего валидировать: любое отсутствующее/
    /// не-"true" значение однозначно означает false, отдельного
    /// ValidationError не требуется.
    pub sandbox: bool,
}

#[derive(Debug, serde::Deserialize)]
struct RequestBody {
    msisdn: String,
    sender_id: String,
    body: String,
    /// SMPP priority_flag (0-3, 3=наивысший) — опционально, партнёр сам
    /// решает; отсутствие поля не ошибка, см. `build_incoming.rs::DEFAULT_PRIORITY_FLAG`.
    #[serde(default)]
    priority: Option<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedRequest {
    pub partner_id: String,
    pub application_id: String,
    pub api_key: String,
    pub remote_ip: Ipv4Addr,
    pub msisdn: String,
    pub sender_id: String,
    pub body: String,
    pub idempotency_key: Option<String>,
    /// SMPP priority_flag (0-3), `None` если партнёр не указал — вызывающая
    /// сторона (`build_incoming.rs`) применяет `DEFAULT_PRIORITY_FLAG`.
    pub priority: Option<u8>,
    pub sandbox: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationError {
    MissingPartnerId,
    MissingApplicationId,
    MissingApiKey,
    MissingRemoteIp,
    InvalidJsonBody,
    EmptyMsisdn,
    InvalidMsisdn,
    EmptySenderId,
    SenderIdTooLong,
    InvalidSenderId,
    EmptyBody,
    BodyTooLong,
    IdempotencyKeyTooLong,
    InvalidPriority,
}

/// SMPP 3.4 §5.2.14 priority_flag — валидный диапазон 0-3.
pub const MAX_PRIORITY_FLAG: u8 = 3;

/// Произвольный, но задокументированный операционный лимит — нигде в спеках
/// не зафиксирован дословно; ~10 сегментов GSM-7 конкатенации (153*10=1530,
/// округлено вверх) как разумная защита от abuse без блокировки легитимных
/// длинных сообщений.
pub const MAX_BODY_CHARS: usize = 1600;

/// HIGH находка кодревью: `sender_id` раньше проверялся только на
/// непустоту (в отличие от `msisdn` — длина+цифры — и `body` — 1600-символьный
/// cap), а форвардился as-is в полный `IncomingMessage` protobuf и
/// публиковался в Kafka, ограниченный только дефолтным 2MB body limit axum
/// (нигде не переопределён). Реальный SMPP `source_addr` — ≤21 октет;
/// авторизованный партнёр, отправляющий ~2MB `sender_id` на запрос,
/// превращает каждый запрос в ~2MB Kafka-сообщение вместо пары сотен
/// байт — resource-amplification вектор в Kafka/Redis message-context кэш
/// Pipeline Engine. 21 — та же граница, что реальный протокол, не
/// придуманная здесь заново.
pub const MAX_SENDER_ID_CHARS: usize = 21;

/// Произвольная, но разумная граница на партнёром задаваемый `X-Idempotency-Key`
/// — тот же принцип, что `MAX_SENDER_ID_CHARS`: заголовок форвардится в Redis
/// key (см. `idempotency.rs`), неограниченная длина — тот же класс
/// resource-amplification, что был у `sender_id`.
pub const MAX_IDEMPOTENCY_KEY_CHARS: usize = 128;

pub fn validate_request_schema(raw: &RawRequest) -> Result<ValidatedRequest, ValidationError> {
    let partner_id = raw.partner_id.clone().filter(|s| !s.is_empty()).ok_or(ValidationError::MissingPartnerId)?;
    let application_id = raw.application_id.clone().filter(|s| !s.is_empty()).ok_or(ValidationError::MissingApplicationId)?;
    let api_key = raw.api_key.clone().filter(|s| !s.is_empty()).ok_or(ValidationError::MissingApiKey)?;
    let remote_ip = raw.remote_ip.ok_or(ValidationError::MissingRemoteIp)?;

    let parsed: RequestBody = serde_json::from_str(&raw.body_json).map_err(|_| ValidationError::InvalidJsonBody)?;

    if parsed.msisdn.is_empty() {
        return Err(ValidationError::EmptyMsisdn);
    }
    if !is_valid_msisdn(&parsed.msisdn) {
        return Err(ValidationError::InvalidMsisdn);
    }
    if parsed.sender_id.is_empty() {
        return Err(ValidationError::EmptySenderId);
    }
    if parsed.sender_id.chars().count() > MAX_SENDER_ID_CHARS {
        return Err(ValidationError::SenderIdTooLong);
    }
    if !is_valid_sender_id(&parsed.sender_id) {
        return Err(ValidationError::InvalidSenderId);
    }
    if parsed.body.is_empty() {
        return Err(ValidationError::EmptyBody);
    }
    if parsed.body.chars().count() > MAX_BODY_CHARS {
        return Err(ValidationError::BodyTooLong);
    }

    let idempotency_key = raw.idempotency_key.clone().filter(|s| !s.is_empty());
    if let Some(key) = &idempotency_key {
        if key.chars().count() > MAX_IDEMPOTENCY_KEY_CHARS {
            return Err(ValidationError::IdempotencyKeyTooLong);
        }
    }

    if let Some(priority) = parsed.priority {
        if priority > MAX_PRIORITY_FLAG {
            return Err(ValidationError::InvalidPriority);
        }
    }

    Ok(ValidatedRequest {
        partner_id,
        application_id,
        api_key,
        remote_ip,
        msisdn: parsed.msisdn,
        sender_id: parsed.sender_id,
        body: parsed.body,
        idempotency_key,
        priority: parsed.priority,
        sandbox: raw.sandbox,
    })
}

/// E.164 без ведущего `+` (как используется по всему этому репозиторию,
/// напр. "998901331835") — только цифры, разумная длина 9-15.
fn is_valid_msisdn(msisdn: &str) -> bool {
    msisdn.len() >= 9 && msisdn.len() <= 15 && msisdn.chars().all(|c| c.is_ascii_digit())
}

/// Печатаемый ASCII (пробел..тильда, 0x20-0x7E) — тот же практический
/// диапазон, что реальный SMPP `source_addr` (буквенно-цифровые бизнес-имена
/// вроде "Click"/"MPP-SMS" или numeric sender). Управляющие символы/произвольный
/// Unicode здесь не имеют легитимного применения для этого поля и отклоняются.
fn is_valid_sender_id(sender_id: &str) -> bool {
    sender_id.chars().all(|c| (' '..='~').contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_raw() -> RawRequest {
        RawRequest {
            partner_id: Some("click_uz".into()),
            application_id: Some("click_uz_main".into()),
            api_key: Some("secret".into()),
            remote_ip: Some("185.65.212.55".parse().unwrap()),
            body_json: r#"{"msisdn":"998901331835","sender_id":"Click","body":"Your OTP is 123456"}"#.into(),
            idempotency_key: None,
            sandbox: false,
        }
    }

    #[test]
    fn valid_request_parses() {
        let result = validate_request_schema(&valid_raw()).unwrap();
        assert_eq!(result.partner_id, "click_uz");
        assert_eq!(result.msisdn, "998901331835");
        assert_eq!(result.body, "Your OTP is 123456");
    }

    #[test]
    fn missing_partner_id_header_rejected() {
        let mut raw = valid_raw();
        raw.partner_id = None;
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::MissingPartnerId));
    }

    #[test]
    fn missing_api_key_header_rejected() {
        let mut raw = valid_raw();
        raw.api_key = None;
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::MissingApiKey));
    }

    #[test]
    fn malformed_json_rejected() {
        let mut raw = valid_raw();
        raw.body_json = "not json".into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidJsonBody));
    }

    #[test]
    fn missing_json_field_rejected_as_invalid_json() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","body":"text"}"#.into(); // sender_id отсутствует
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidJsonBody));
    }

    #[test]
    fn non_numeric_msisdn_rejected() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"not-a-number","sender_id":"Click","body":"text"}"#.into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidMsisdn));
    }

    #[test]
    fn empty_body_rejected() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"Click","body":""}"#.into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::EmptyBody));
    }

    #[test]
    fn body_over_max_length_rejected() {
        let mut raw = valid_raw();
        let long_body = "a".repeat(MAX_BODY_CHARS + 1);
        raw.body_json = format!(r#"{{"msisdn":"998901331835","sender_id":"Click","body":"{long_body}"}}"#);
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::BodyTooLong));
    }

    #[test]
    fn body_at_exactly_max_length_accepted() {
        let mut raw = valid_raw();
        let body = "a".repeat(MAX_BODY_CHARS);
        raw.body_json = format!(r#"{{"msisdn":"998901331835","sender_id":"Click","body":"{body}"}}"#);
        assert!(validate_request_schema(&raw).is_ok());
    }

    #[test]
    fn sender_id_over_max_length_rejected() {
        let mut raw = valid_raw();
        let long_sender = "A".repeat(MAX_SENDER_ID_CHARS + 1);
        raw.body_json = format!(r#"{{"msisdn":"998901331835","sender_id":"{long_sender}","body":"text"}}"#);
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::SenderIdTooLong));
    }

    #[test]
    fn sender_id_at_exactly_max_length_accepted() {
        let mut raw = valid_raw();
        let sender = "A".repeat(MAX_SENDER_ID_CHARS);
        raw.body_json = format!(r#"{{"msisdn":"998901331835","sender_id":"{sender}","body":"text"}}"#);
        assert!(validate_request_schema(&raw).is_ok());
    }

    /// Прямое доказательство исправления HIGH находки кодревью
    /// (resource-amplification): ~2MB sender_id раньше проходил валидацию
    /// без единой проверки длины — теперь отклоняется как SenderIdTooLong
    /// задолго до публикации в Kafka.
    #[test]
    fn oversized_sender_id_resource_amplification_vector_rejected() {
        let mut raw = valid_raw();
        let huge_sender = "A".repeat(2 * 1024 * 1024);
        raw.body_json = format!(r#"{{"msisdn":"998901331835","sender_id":"{huge_sender}","body":"text"}}"#);
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::SenderIdTooLong));
    }

    #[test]
    fn sender_id_with_control_character_rejected() {
        let mut raw = valid_raw();
        raw.body_json = "{\"msisdn\":\"998901331835\",\"sender_id\":\"Click\\u0000\",\"body\":\"text\"}".into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidSenderId));
    }

    #[test]
    fn sender_id_with_non_ascii_rejected() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"Клик","body":"text"}"#.into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidSenderId));
    }

    #[test]
    fn numeric_sender_id_accepted() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"998712345678","body":"text"}"#.into();
        assert!(validate_request_schema(&raw).is_ok());
    }

    #[test]
    fn sender_id_with_hyphen_and_space_accepted() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"MPP-SMS Info","body":"text"}"#.into();
        assert!(validate_request_schema(&raw).is_ok());
    }

    #[test]
    fn priority_absent_is_none_not_an_error() {
        let result = validate_request_schema(&valid_raw()).unwrap();
        assert_eq!(result.priority, None);
    }

    #[test]
    fn priority_present_and_valid_passes_through() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"Click","body":"text","priority":3}"#.into();
        let result = validate_request_schema(&raw).unwrap();
        assert_eq!(result.priority, Some(3));
    }

    #[test]
    fn priority_over_max_rejected() {
        let mut raw = valid_raw();
        raw.body_json = r#"{"msisdn":"998901331835","sender_id":"Click","body":"text","priority":4}"#.into();
        assert_eq!(validate_request_schema(&raw), Err(ValidationError::InvalidPriority));
    }

    /// Фаза 11 плана закрытия API-пробелов: sandbox проходит через валидацию
    /// как есть — не влияет ни на одну другую проверку, ничего не может
    /// отклонить.
    #[test]
    fn sandbox_flag_passes_through_validation_unchanged() {
        let mut raw = valid_raw();
        raw.sandbox = true;
        let result = validate_request_schema(&raw).unwrap();
        assert!(result.sandbox);
    }

    #[test]
    fn sandbox_defaults_to_false() {
        let result = validate_request_schema(&valid_raw()).unwrap();
        assert!(!result.sandbox);
    }
}
