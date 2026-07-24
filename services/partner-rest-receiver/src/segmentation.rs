//! `build_incoming_message` (service_internal_methods.md §1.1) должен заполнить
//! `SmsPayload.segment_count`/`encoding` — `types.proto`'s комментарий "посчитано
//! один раз Pipeline Engine" описывает, что Pipeline Engine лишь КЭШИРУЕТ уже
//! вычисленное значение в Runtime Redis (`service_internal_methods.md` §0), не
//! пересчитывает его: `pipeline-engine/src/kafka_io.rs::handle_incoming` читает
//! `sms.segment_count` прямо с `IncomingMessage`, не вычисляет — значит именно
//! этот сервис, на приёме, обязан посчитать реальное число сегментов.
//!
//! Реальная логика подсчёта SMS-сегментов (GSM 03.38 / 3GPP TS 23.038), не
//! заглушка — используется буквально в каждом SMS-шлюзе:
//! * GSM-7 (default alphabet + расширенная таблица escape-символов): 160
//!   септетов в одном сегменте, 153 на сегмент при конкатенации (7 септетов
//!   уходят под UDH concatenation header).
//! * UCS-2 (как только встречается хоть один символ вне GSM-7 alphabet,
//!   например кириллица): 70 code unit в одном сегменте, 67 при конкатенации.
//!
//! Осознанное упрощение: таблица GSM-7 basic/extension взята по стандарту
//! полностью (128 + 10 escape-символов), но подсчёт UCS-2 code units через
//! `encode_utf16().count()` — корректно для surrogate-пар (символы вне BMP,
//! напр. часть эмодзи, физически занимают 2 UCS-2 code unit, это учтено),
//! но НЕ реализует комбинирование GSM-7 basic+extension с реальным исходом
//! encoding detection операторов (некоторые SMSC применяют национальные
//! таблицы замены, например турецкую) — вне scope этого среза.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmsEncoding {
    Gsm7,
    Ucs2,
}

impl SmsEncoding {
    pub fn as_str(self) -> &'static str {
        match self {
            SmsEncoding::Gsm7 => "GSM7",
            SmsEncoding::Ucs2 => "UCS2",
        }
    }
}

/// GSM 03.38 default alphabet, basic character set (septet 0x00-0x7F минус те,
/// что переопределены расширенной таблицей). Позиция 0x1B (ESC — переход к
/// расширенной таблице) сюда намеренно не включена: управляющий код, не
/// самостоятельный отображаемый символ, реальный текст сообщения его не содержит.
const GSM7_BASIC: &str = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà";

/// Расширенная таблица (escape-последовательность, 2 септета на символ):
/// `|^€{}[]~\`.
const GSM7_EXTENDED: &str = "|^€{}[]~\\";

fn char_class(c: char) -> Option<u8> {
    if GSM7_BASIC.contains(c) {
        Some(1)
    } else if GSM7_EXTENDED.contains(c) {
        Some(2)
    } else {
        None
    }
}

/// `None`, если тело содержит хоть один символ вне GSM-7 alphabet (включая
/// расширенную таблицу) — тогда используется UCS-2 для всего сообщения
/// (нельзя посимвольно смешивать кодировки в одном SMS PDU).
fn gsm7_septet_count(body: &str) -> Option<usize> {
    let mut total = 0usize;
    for c in body.chars() {
        total += char_class(c)? as usize;
    }
    Some(total)
}

pub struct SegmentInfo {
    pub encoding: SmsEncoding,
    pub segment_count: i32,
}

pub fn compute_segments(body: &str) -> SegmentInfo {
    if let Some(septets) = gsm7_septet_count(body) {
        let segment_count = if septets == 0 {
            1
        } else if septets <= 160 {
            1
        } else {
            septets.div_ceil(153) as i32
        };
        SegmentInfo { encoding: SmsEncoding::Gsm7, segment_count }
    } else {
        let units = body.encode_utf16().count();
        let segment_count = if units == 0 {
            1
        } else if units <= 70 {
            1
        } else {
            units.div_ceil(67) as i32
        };
        SegmentInfo { encoding: SmsEncoding::Ucs2, segment_count }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_body_is_one_segment() {
        let info = compute_segments("");
        assert_eq!(info.segment_count, 1);
        assert_eq!(info.encoding, SmsEncoding::Gsm7);
    }

    #[test]
    fn ascii_under_160_is_one_gsm7_segment() {
        let info = compute_segments("Your OTP code is 123456. Do not share it with anyone.");
        assert_eq!(info.encoding, SmsEncoding::Gsm7);
        assert_eq!(info.segment_count, 1);
    }

    #[test]
    fn ascii_exactly_160_is_one_gsm7_segment() {
        let body = "a".repeat(160);
        let info = compute_segments(&body);
        assert_eq!(info.segment_count, 1);
    }

    #[test]
    fn ascii_161_chars_splits_into_two_concatenated_gsm7_segments() {
        let body = "a".repeat(161);
        let info = compute_segments(&body);
        assert_eq!(info.encoding, SmsEncoding::Gsm7);
        assert_eq!(info.segment_count, 2, "161 септет > 160 (одиночный лимит), делится по 153 на сегмент конкатенации");
    }

    #[test]
    fn ascii_306_chars_needs_two_segments_not_three() {
        // 2 * 153 = 306 — ровно на границе.
        let body = "a".repeat(306);
        let info = compute_segments(&body);
        assert_eq!(info.segment_count, 2);
    }

    #[test]
    fn ascii_307_chars_needs_three_segments() {
        let body = "a".repeat(307);
        let info = compute_segments(&body);
        assert_eq!(info.segment_count, 3);
    }

    #[test]
    fn extended_table_char_costs_two_septets() {
        // "€" — 1 символ, 2 септета. 159 обычных + 1 "€" = 161 септет > 160.
        let body = format!("{}€", "a".repeat(159));
        let info = compute_segments(&body);
        assert_eq!(info.encoding, SmsEncoding::Gsm7, "€ входит в расширенную GSM-7 таблицу, не переключает на UCS-2");
        assert_eq!(info.segment_count, 2, "159 + 2 септета евро = 161 септет, уже не влезает в 160");
    }

    #[test]
    fn cyrillic_forces_ucs2() {
        let info = compute_segments("Спасибо за оплату");
        assert_eq!(info.encoding, SmsEncoding::Ucs2);
        assert_eq!(info.segment_count, 1);
    }

    #[test]
    fn ucs2_under_70_is_one_segment() {
        let body = "Спасибо".repeat(5); // 35 символов кириллицы
        let info = compute_segments(&body);
        assert_eq!(info.encoding, SmsEncoding::Ucs2);
        assert_eq!(info.segment_count, 1);
    }

    #[test]
    fn ucs2_71_units_splits_into_two_concatenated_segments() {
        let body = "я".repeat(71);
        let info = compute_segments(&body);
        assert_eq!(info.encoding, SmsEncoding::Ucs2);
        assert_eq!(info.segment_count, 2, "71 code unit > 70 (одиночный лимит), делится по 67 на сегмент конкатенации");
    }

    #[test]
    fn single_non_gsm7_char_among_ascii_forces_whole_message_to_ucs2() {
        // Нельзя посимвольно смешивать кодировки в одном PDU — один символ
        // вне GSM-7 alphabet переключает ВСЁ сообщение на UCS-2.
        let body = format!("{}ю", "a".repeat(200));
        let info = compute_segments(&body);
        assert_eq!(info.encoding, SmsEncoding::Ucs2);
    }

    #[test]
    fn astral_plane_emoji_counts_as_two_ucs2_units_surrogate_pair() {
        // 🎉 (U+1F389) вне BMP — encode_utf16 даёт 2 code unit (суррогатная пара),
        // корректный UCS-2-подсчёт должен это учитывать, не считать 1 символ = 1 unit.
        let info = compute_segments("🎉");
        assert_eq!(info.encoding, SmsEncoding::Ucs2);
        assert_eq!(info.segment_count, 1);
        assert_eq!("🎉".encode_utf16().count(), 2);
    }
}
