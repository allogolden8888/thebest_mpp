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
}

#[derive(Debug, serde::Deserialize)]
struct RequestBody {
    msisdn: String,
    sender_id: String,
    body: String,
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
    EmptyBody,
    BodyTooLong,
}

/// Произвольный, но задокументированный операционный лимит — нигде в спеках
/// не зафиксирован дословно; ~10 сегментов GSM-7 конкатенации (153*10=1530,
/// округлено вверх) как разумная защита от abuse без блокировки легитимных
/// длинных сообщений.
pub const MAX_BODY_CHARS: usize = 1600;

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
    if parsed.body.is_empty() {
        return Err(ValidationError::EmptyBody);
    }
    if parsed.body.chars().count() > MAX_BODY_CHARS {
        return Err(ValidationError::BodyTooLong);
    }

    Ok(ValidatedRequest {
        partner_id,
        application_id,
        api_key,
        remote_ip,
        msisdn: parsed.msisdn,
        sender_id: parsed.sender_id,
        body: parsed.body,
    })
}

/// E.164 без ведущего `+` (как используется по всему этому репозиторию,
/// напр. "998901331835") — только цифры, разумная длина 9-15.
fn is_valid_msisdn(msisdn: &str) -> bool {
    msisdn.len() >= 9 && msisdn.len() <= 15 && msisdn.chars().all(|c| c.is_ascii_digit())
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
}
