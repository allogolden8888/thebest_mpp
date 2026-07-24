//! `check_admission` (service_internal_methods.md §1.1) — в проде читает
//! `execution.control` (compacted, local snapshot) для scope GLOBAL / PARTNER /
//! PARTNER_STAGE / OPERATOR_ROUTE. Ни один сервис в этом срезе не реализует
//! реальное потребление `execution.control` (Execution Control Service,
//! владелец Субагент 1, ещё не публикует его для всех scope — см.
//! `CODE_REVIEW.md` finding по `execution-control-service`); здесь — тот же
//! паттерн, что уже задокументирован в `routing-service/src/routing.rs`
//! (`ControlState`, fail-open по умолчанию для отсутствующих записей): трейт
//! `AdmissionGate` с реальной сигнатурой, единственная реализация —
//! всегда `Admit` (снапшот всегда пуст в этом срезе). Подключение реального
//! consumer'а `execution.control` не сделано в этом срезе — см. README.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Admit,
    // Никогда не строится продовым `AlwaysAdmit` (пустой снапшот, см. выше) —
    // реальный `AdmissionGate`, читающий `execution.control`, не реализован
    // в этом срезе. Вариант и весь путь его обработки в `http.rs` — уже
    // построены и протестированы (`http::tests::admission_reject_propagates_retry_after`),
    // чтобы включение реального consumer'а не требовало трогать вызывающий код.
    #[allow(dead_code)]
    Reject { retry_after_seconds: u32 },
}

pub trait AdmissionGate: Send + Sync {
    fn check(&self, partner_id: &str) -> AdmissionDecision;
}

/// Пустой снапшот `execution.control` — fail-open по всем scope, тот же выбор,
/// что и `ControlState`-заглушка в `routing-service` для отсутствующих записей.
#[derive(Default)]
pub struct AlwaysAdmit;

impl AdmissionGate for AlwaysAdmit {
    fn check(&self, _partner_id: &str) -> AdmissionDecision {
        AdmissionDecision::Admit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_admit_admits_any_partner() {
        assert_eq!(AlwaysAdmit.check("click_uz"), AdmissionDecision::Admit);
        assert_eq!(AlwaysAdmit.check("unknown_partner"), AdmissionDecision::Admit);
    }
}
