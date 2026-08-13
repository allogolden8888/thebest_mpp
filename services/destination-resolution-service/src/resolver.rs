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
use std::sync::Mutex;

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

/// Одна строка `config.changes` (entity_type=NUMBER_RANGE) —
/// `config_schemas/number_range.schema.json`, ровно то, что кладёт в
/// `payload_json` `config-event-publisher/internal/kafkaio/publisher.go`.
/// Отдельная от [`NumberRange`] структура: статический файл-снапшот несёт
/// голый `{range_start,range_end,operator_id}` без `version`/`status` (эти
/// поля осмысленны только для одной CRUD-строки config.changes, не для
/// снапшота целиком).
#[derive(Debug, Clone, Deserialize)]
pub struct NumberRangeConfigPayload {
    pub range_start: u64,
    pub range_end: u64,
    pub operator_id: String,
    // Не используется в самой overlay-логике (last-write-wins по порядку
    // Kafka-сообщений, не по версии) — сохраняется как часть 1:1
    // соответствия number_range.schema.json, не для собственной логики.
    #[serde(default)]
    #[allow(dead_code)]
    pub version: i64,
    pub status: String, // "active" | "archived"
}

/// Живое состояние `number_range` поверх статического bootstrap-снапшота —
/// `config.changes` реально находка (см. README "Что НЕ реализовано"
/// историй destination-resolution-service/policy-service/routing-service):
/// arc-swap был объявлен в Cargo.toml, но никогда не подключался ни к
/// одному реальному Kafka-консьюмеру. `overlay` хранит текущее активное
/// состояние по `entity_id` (ключ Kafka-сообщения config.changes) —
/// `status=archived` удаляет запись, иначе она вставляется/обновляется.
/// `portability_overrides` НЕ входит в этот hot-reload контур — у него
/// нет своего `ConfigEntityType` в `common/enums.proto` (не изобретаем
/// контракт, которым не владеем), остаётся только статическим.
pub struct ConfigOverlay {
    base_ranges: Vec<NumberRange>,
    base_portability_overrides: HashMap<String, String>,
    overlay: Mutex<HashMap<String, NumberRange>>,
}

impl ConfigOverlay {
    pub fn new(base: Snapshot) -> Self {
        ConfigOverlay {
            base_ranges: base.number_ranges,
            base_portability_overrides: base.portability_overrides,
            overlay: Mutex::new(HashMap::new()),
        }
    }

    /// Применяет одну CRUD-запись `config.changes` — вызывающая сторона
    /// решает, когда пересобрать и опубликовать новый [`Snapshot`]
    /// ([`Self::build_snapshot`]), это отдельный шаг, не побочный эффект.
    pub fn apply(&self, entity_id: &str, payload: &NumberRangeConfigPayload) {
        let mut overlay = self.overlay.lock().expect("overlay mutex poisoned");
        if payload.status == "archived" {
            overlay.remove(entity_id);
        } else {
            overlay.insert(
                entity_id.to_string(),
                NumberRange {
                    range_start: payload.range_start,
                    range_end: payload.range_end,
                    operator_id: payload.operator_id.clone(),
                },
            );
        }
    }

    /// Overlay-диапазоны идут ПЕРВЫМИ — `resolve_operator_by_range` берёт
    /// первое совпадение, значит свежая запись из config.changes обязана
    /// перекрывать (не проигрывать) потенциально устаревший диапазон из
    /// статического bootstrap-файла при пересечении.
    pub fn build_snapshot(&self) -> Snapshot {
        let overlay = self.overlay.lock().expect("overlay mutex poisoned");
        let mut number_ranges: Vec<NumberRange> = overlay.values().cloned().collect();
        number_ranges.extend(self.base_ranges.iter().cloned());
        Snapshot {
            number_ranges,
            portability_overrides: self.base_portability_overrides.clone(),
        }
    }
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

    /// Ровно те же диапазоны, что в migrations/V011__number_range.sql —
    /// не переизобретены, скопированы как источник истины для теста.
    ///
    /// Реальная находка (только реальным прогоном платформы через
    /// docker-compose, не статичным чтением): operator_id здесь раньше был
    /// голым "beeline"/"ucell"/"uzmobile", а канонический operator_id
    /// (config_schemas/examples/operator.valid.json, routing_table.valid.json)
    /// — "beeline_uz"/"ucell_uz"/"uzmobile_uz". Расхождение не ловилось
    /// никаким тестом (каждый сервис по отдельности выглядел корректным —
    /// destination-resolution-service компилировался и тестировался против
    /// своего же (неверного) снапшота) — проявилось только когда реальное
    /// сообщение дошло до `routing-service` и тот отклонил его с
    /// `UNKNOWN_OPERATOR`, потому что его `routing_table.valid.json` не
    /// знает оператора "beeline". Исправлено здесь и в
    /// `migrations/V011__number_range.sql`.
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
            ("998901331835", "beeline_uz"),  // префикс 90 — реально прогнан на PostgreSQL
            ("998911234567", "beeline_uz"),  // префикс 91
            ("998921234567", "beeline_uz"),  // префикс 92
            ("998201234567", "beeline_uz"),  // префикс 20
            ("998501234567", "ucell_uz"),    // префикс 50
            ("998931234567", "ucell_uz"),    // префикс 93
            ("998941234567", "ucell_uz"),    // префикс 94
            ("998981234567", "uzmobile_uz"), // префикс 98
            ("998991234567", "uzmobile_uz"), // префикс 99
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
        // 998901339999 лежит в диапазоне beeline_uz (префикс 90), но снапшот
        // несёт portability override на ucell_uz — override обязан победить.
        let ported_msisdn = "998901339999";
        assert_eq!(
            snapshot.resolve_operator_by_range(ported_msisdn),
            ResolveResult::Resolved("ucell_uz".to_string())
        );
        // Без override тот же диапазон резолвился бы в beeline_uz — контрольная проверка.
        assert_eq!(
            snapshot.resolve_operator_by_range("998901339998"),
            ResolveResult::Resolved("beeline_uz".to_string())
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

    fn active_payload(range_start: u64, range_end: u64, operator_id: &str) -> NumberRangeConfigPayload {
        NumberRangeConfigPayload {
            range_start,
            range_end,
            operator_id: operator_id.to_string(),
            version: 1,
            status: "active".to_string(),
        }
    }

    #[test]
    fn config_overlay_starts_out_resolving_exactly_like_the_static_baseline() {
        let overlay = ConfigOverlay::new(real_snapshot());
        let snapshot = overlay.build_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("998901331835"),
            ResolveResult::Resolved("beeline_uz".to_string())
        );
    }

    #[test]
    fn config_overlay_apply_active_makes_a_brand_new_range_resolvable() {
        let overlay = ConfigOverlay::new(real_snapshot());
        // 998770000000 не покрыт ни одним диапазоном статического файла
        // (уже проверено unknown_prefix_returns_not_found выше).
        overlay.apply("cfg-1", &active_payload(998770000000, 998779999999, "new_operator_uz"));
        let snapshot = overlay.build_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("998770000000"),
            ResolveResult::Resolved("new_operator_uz".to_string())
        );
    }

    #[test]
    fn config_overlay_apply_overrides_a_conflicting_static_range() {
        let overlay = ConfigOverlay::new(real_snapshot());
        // 998901331835 статически резолвится в beeline_uz — та же CRUD-запись
        // (тот же диапазон) должна перекрыть его новым оператором.
        overlay.apply("cfg-2", &active_payload(998900000000, 998909999999, "reassigned_uz"));
        let snapshot = overlay.build_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("998901331835"),
            ResolveResult::Resolved("reassigned_uz".to_string())
        );
    }

    #[test]
    fn config_overlay_apply_archived_removes_a_previously_active_entry() {
        let overlay = ConfigOverlay::new(real_snapshot());
        overlay.apply("cfg-3", &active_payload(998770000000, 998779999999, "temp_uz"));
        assert_eq!(
            overlay.build_snapshot().resolve_operator_by_range("998770000000"),
            ResolveResult::Resolved("temp_uz".to_string())
        );

        let mut archived = active_payload(998770000000, 998779999999, "temp_uz");
        archived.status = "archived".to_string();
        overlay.apply("cfg-3", &archived);
        assert_eq!(
            overlay.build_snapshot().resolve_operator_by_range("998770000000"),
            ResolveResult::NotFound
        );
    }

    #[test]
    fn config_overlay_apply_same_entity_id_twice_updates_not_duplicates() {
        let overlay = ConfigOverlay::new(real_snapshot());
        overlay.apply("cfg-4", &active_payload(998770000000, 998779999999, "first_uz"));
        overlay.apply("cfg-4", &active_payload(998770000000, 998779999999, "second_uz"));
        let snapshot = overlay.build_snapshot();
        assert_eq!(
            snapshot.resolve_operator_by_range("998770000000"),
            ResolveResult::Resolved("second_uz".to_string())
        );
        // Один entity_id — одна запись в overlay, не две конкурирующие.
        assert_eq!(
            snapshot.number_ranges.iter().filter(|r| r.operator_id == "first_uz").count(),
            0
        );
    }
}
