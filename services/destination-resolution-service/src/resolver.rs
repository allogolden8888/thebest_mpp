//! `resolve_operator_by_range` — service_internal_methods.md §1.4a.
//!
//! MNP overlay проверяется ДО number_range: точное совпадение по msisdn
//! перекрывает диапазон (data_infrastructure_spec.md §1.9a). Live HLR-резолв
//! не входит в первую версию (HLD §26) — только статическая таблица,
//! периодически обновляемая через config.changes (здесь — снапшот из JSON,
//! в проде — то же самое содержимое, спроецированное из
//! routing.number_range/routing.number_portability_override).

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct NumberRange {
    pub range_start: u64,
    pub range_end: u64,
    pub operator_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Snapshot {
    pub number_ranges: Vec<NumberRange>,
    #[serde(default)]
    pub portability_overrides: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ResolveResult {
    Resolved(String),
    NotFound,
}

impl Snapshot {
    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Соответствует `resolve_operator_by_range(destination_address, local_snapshot) -> resolved_operator_id | NotFound`.
    pub fn resolve_operator_by_range(&self, destination_address: &str) -> ResolveResult {
        if let Some(operator_id) = self.portability_overrides.get(destination_address) {
            return ResolveResult::Resolved(operator_id.clone());
        }

        let Ok(msisdn) = destination_address.parse::<u64>() else {
            return ResolveResult::NotFound;
        };

        self.number_ranges
            .iter()
            .find(|r| msisdn >= r.range_start && msisdn <= r.range_end)
            .map(|r| ResolveResult::Resolved(r.operator_id.clone()))
            .unwrap_or(ResolveResult::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ровно те же 9 диапазонов, что в migrations/V011__number_range.sql —
    /// не переизобретены, скопированы как источник истины для теста.
    fn real_snapshot() -> Snapshot {
        Snapshot::from_json_str(include_str!("../data/number_range_snapshot.json"))
            .expect("data/number_range_snapshot.json должен парситься")
    }

    /// 998901331835 — тот самый номер, реально проверенный на PostgreSQL
    /// в этом чате (migrations/README.md: "9/9 совпадений"). Остальные 8
    /// сконструированы внутри тех же реальных диапазонов V011 — исходные
    /// 8 не сохранились дословно нигде в репозитории, но диапазоны, из
    /// которых они строятся, это те самые верифицированные данные.
    #[test]
    fn resolves_all_nine_documented_prefixes() {
        let snapshot = real_snapshot();
        let cases = [
            ("998901331835", "beeline"),  // префикс 90 — реально прогнан на PostgreSQL
            ("998911234567", "beeline"),  // префикс 91
            ("998921234567", "beeline"),  // префикс 92
            ("998201234567", "beeline"),  // префикс 20
            ("998501234567", "ucell"),    // префикс 50
            ("998931234567", "ucell"),    // префикс 93
            ("998941234567", "ucell"),    // префикс 94
            ("998981234567", "uzmobile"), // префикс 98
            ("998991234567", "uzmobile"), // префикс 99
        ];
        for (msisdn, expected_operator) in cases {
            assert_eq!(
                snapshot.resolve_operator_by_range(msisdn),
                ResolveResult::Resolved(expected_operator.to_string()),
                "msisdn={msisdn} должен резолвиться в {expected_operator}"
            );
        }
    }

    #[test]
    fn mnp_override_wins_over_range() {
        let snapshot = real_snapshot();
        // 998901339999 лежит в диапазоне beeline (префикс 90), но снапшот
        // несёт portability override на ucell — override обязан победить.
        let ported_msisdn = "998901339999";
        assert_eq!(
            snapshot.resolve_operator_by_range(ported_msisdn),
            ResolveResult::Resolved("ucell".to_string())
        );
        // Без override тот же диапазон резолвился бы в beeline — контрольная проверка.
        assert_eq!(
            snapshot.resolve_operator_by_range("998901339998"),
            ResolveResult::Resolved("beeline".to_string())
        );
    }

    #[test]
    fn unknown_prefix_returns_not_found() {
        // Perfectum/UMS — операторы Узбекистана, реально существующие, но
        // не входящие в подтверждённый неполный список (migrations/README.md
        // "список заведомо неполный"). Номер с непокрытым префиксом обязан
        // вернуть NotFound, не тихо резолвиться в произвольного оператора.
        let snapshot = real_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("998770000000"),
            ResolveResult::NotFound
        );
    }

    #[test]
    fn non_numeric_destination_address_returns_not_found_not_panic() {
        let snapshot = real_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("not-a-number"),
            ResolveResult::NotFound
        );
    }
}
