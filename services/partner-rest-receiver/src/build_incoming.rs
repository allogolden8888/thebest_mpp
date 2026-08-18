//! `generate_message_id` + `generate_trace_id` + `build_incoming_message`
//! (service_internal_methods.md §1.1) — строит `IncomingMessage` (единственное
//! место, где `body` передаётся полностью, `service_internal_methods.md` §0).

use crate::proto::common::{Channel, SmsPayload};
use crate::proto::events::IncomingMessage;
use crate::proto::events::incoming_message::Body;
use crate::request::ValidatedRequest;
use crate::segmentation::compute_segments;
use prost_types::Timestamp;
use std::time::{Duration, SystemTime};

pub fn generate_message_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn generate_trace_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn to_timestamp(t: SystemTime) -> Timestamp {
    let duration = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    Timestamp { seconds: duration.as_secs() as i64, nanos: duration.subsec_nanos() as i32 }
}

/// `message_ttl` — не в контракте REST-запроса, платформенный дефолт.
/// Нигде не задокументирован дословно как константа — разумное значение
/// (24 часа, стандартный SMS validity period по умолчанию у большинства
/// SMSC/операторов) явно помечено как выбор этого среза, не найденное число.
pub const DEFAULT_MESSAGE_TTL: Duration = Duration::from_secs(24 * 3600);

/// SMPP priority_flag дефолт для партнёров, не указавших `priority` явно —
/// намеренно НЕ 0 (SMPP-дефолт для непомеченного трафика), а MEDIUM (2 —
/// см. `PriorityTier.forPriorityFlag` на operator-smpp-session-manager),
/// чтобы немаркированный/легаси-трафик не проваливался в LOW-очередь
/// (гарантия всего 10% туннеля) просто из-за того, что партнёр не обновил
/// интеграцию.
pub const DEFAULT_PRIORITY_FLAG: i32 = 2;

pub fn build_incoming_message(
    req: &ValidatedRequest,
    message_id: String,
    trace_id: String,
    now: SystemTime,
) -> IncomingMessage {
    let segments = compute_segments(&req.body);
    let sms = SmsPayload {
        msisdn: req.msisdn.clone(),
        sender: req.sender_id.clone(),
        body: req.body.clone(),
        encoding: segments.encoding.as_str().to_string(),
        segment_count: segments.segment_count,
    };

    IncomingMessage {
        message_id,
        trace_id,
        channel: Channel::Sms as i32,
        partner_id: req.partner_id.clone(),
        application_id: req.application_id.clone(),
        sandbox: req.sandbox,
        body: Some(Body::Sms(sms)),
        received_at: Some(to_timestamp(now)),
        message_ttl: Some(to_timestamp(now + DEFAULT_MESSAGE_TTL)),
        priority_flag: req.priority.map(|p| p as i32).unwrap_or(DEFAULT_PRIORITY_FLAG),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn req() -> ValidatedRequest {
        ValidatedRequest {
            partner_id: "click_uz".into(),
            application_id: "click_uz_main".into(),
            api_key: "secret".into(),
            remote_ip: Ipv4Addr::new(185, 65, 212, 55),
            msisdn: "998901331835".into(),
            sender_id: "Click".into(),
            body: "Your OTP is 123456".into(),
            idempotency_key: None,
            priority: None,
            sandbox: false,
        }
    }

    #[test]
    fn generated_ids_are_non_empty_and_distinct() {
        let id1 = generate_message_id();
        let id2 = generate_message_id();
        assert!(!id1.is_empty());
        assert_ne!(id1, id2);
    }

    #[test]
    fn builds_incoming_message_with_sms_payload_and_computed_segment_count() {
        let now = SystemTime::now();
        let msg = build_incoming_message(&req(), "m1".into(), "t1".into(), now);
        assert_eq!(msg.message_id, "m1");
        assert_eq!(msg.trace_id, "t1");
        assert_eq!(msg.channel, Channel::Sms as i32);
        assert_eq!(msg.partner_id, "click_uz");
        assert_eq!(msg.application_id, "click_uz_main");
        match msg.body {
            Some(Body::Sms(sms)) => {
                assert_eq!(sms.msisdn, "998901331835");
                assert_eq!(sms.sender, "Click");
                assert_eq!(sms.body, "Your OTP is 123456");
                assert_eq!(sms.encoding, "GSM7");
                assert_eq!(sms.segment_count, 1);
            }
            other => panic!("ожидали SMS payload, получили {other:?}"),
        }
    }

    #[test]
    fn message_ttl_is_received_at_plus_default_duration() {
        let now = SystemTime::now();
        let msg = build_incoming_message(&req(), "m1".into(), "t1".into(), now);
        let received_at = msg.received_at.unwrap();
        let ttl = msg.message_ttl.unwrap();
        assert_eq!(ttl.seconds - received_at.seconds, DEFAULT_MESSAGE_TTL.as_secs() as i64);
    }

    /// Фаза 11 плана закрытия API-пробелов: ValidatedRequest.sandbox
    /// обязан попасть в IncomingMessage.sandbox — единственная точка входа
    /// dry-run-флага в весь остальной пайплайн.
    #[test]
    fn sandbox_flag_propagates_into_incoming_message() {
        let mut r = req();
        r.sandbox = true;
        let msg = build_incoming_message(&r, "m1".into(), "t1".into(), SystemTime::now());
        assert!(msg.sandbox);
    }

    #[test]
    fn cyrillic_body_produces_ucs2_encoding() {
        let mut r = req();
        r.body = "Спасибо за оплату".into();
        let msg = build_incoming_message(&r, "m1".into(), "t1".into(), SystemTime::now());
        match msg.body {
            Some(Body::Sms(sms)) => assert_eq!(sms.encoding, "UCS2"),
            other => panic!("ожидали SMS payload, получили {other:?}"),
        }
    }

    #[test]
    fn priority_absent_defaults_to_medium() {
        let msg = build_incoming_message(&req(), "m1".into(), "t1".into(), SystemTime::now());
        assert_eq!(msg.priority_flag, DEFAULT_PRIORITY_FLAG);
    }

    #[test]
    fn priority_present_passes_through_unchanged() {
        let mut r = req();
        r.priority = Some(3);
        let msg = build_incoming_message(&r, "m1".into(), "t1".into(), SystemTime::now());
        assert_eq!(msg.priority_flag, 3);
    }
}
