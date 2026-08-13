//! Отслеживает, до какого offset безопасно закоммитить per-partition при
//! КОНКУРЕНТНОЙ (не строго последовательной) обработке записей.
//!
//! Реальная находка (нагрузочный прогон, не гипотетическая): `run_incoming_loop`/
//! `run_completed_loop` раньше обрабатывали ровно одну запись за раз —
//! `consumer.recv().await` -> Redis CAS round-trip -> Kafka produce -> commit
//! -> следующая запись — при 100 msg/s ingest это оставляло всего ~6 msg/s
//! реальной пропускной способности (Pipeline Engine видит 5 stage.completed
//! на каждое входящее сообщение, не одно), backlog рос на десятки тысяч
//! записей за 5-минутный прогон. Обработка вперемешку (много записей
//! одновременно) поднимает пропускную способность на порядки, но ломает
//! наивный "commit сразу после обработки": если offset=105 завершился
//! раньше offset=104, закоммитить 106 значило бы объявить 104 обработанной,
//! хотя её обработка может ещё не завершиться (и потеряться при падении
//! процесса до её реального завершения, если бы commit её уже "накрыл").
//!
//! Вместо этого коммитится только СПЛОШНОЙ префикс завершённых offset —
//! "дыры" ждут более ранних offset. Отправная точка watermark для каждой
//! партиции обязана браться из порядка ПОЛУЧЕНИЯ записей ([`observe_received`],
//! вызывается синхронно в основном poll-цикле, единственном месте, которое
//! реально видит записи строго по порядку — гонка обработки начинается
//! только ПОСЛЕ этого), не из порядка ЗАВЕРШЕНИЯ обработки ([`mark_done`],
//! вызывается из множества конкурентных задач в произвольном порядке) —
//! иначе watermark мог бы стартовать с "какой offset завершился первым",
//! что не то же самое, что "какой offset был получен первым".

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PartitionKey {
    pub topic: String,
    pub partition: i32,
}

#[derive(Default)]
struct PartitionState {
    /// Offset, который трекер ожидает увидеть следующим в сплошном префиксе
    /// завершённых. Инициализируется ТОЛЬКО через `observe_received` (порядок
    /// получения), не через `mark_done` (порядок завершения).
    next_to_commit: Option<i64>,
    completed_out_of_order: BTreeSet<i64>,
}

pub struct OffsetTracker {
    partitions: Mutex<HashMap<PartitionKey, PartitionState>>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self { partitions: Mutex::new(HashMap::new()) }
    }

    /// Вызывается СИНХРОННО в основном poll-цикле для каждой записи сразу
    /// после `consumer.recv()`, ДО того как её обработка уйдёт в spawned-таск.
    /// Единственный источник истины о том, какой offset был первым увиденным
    /// для этой партиции в ЭТОЙ сессии процесса.
    pub fn observe_received(&self, key: &PartitionKey, offset: i64) {
        let mut partitions = self.partitions.lock().expect("offset tracker mutex poisoned");
        let state = partitions.entry(key.clone()).or_default();
        if state.next_to_commit.is_none() {
            state.next_to_commit = Some(offset);
        }
    }

    /// Помечает offset как завершённый (успешно обработанный ИЛИ намеренно
    /// пропущенный без коммита — тот же случай, что уже был у
    /// последовательной версии: транзиентная ошибка -> offset НЕ считается
    /// done, watermark для этой партиции просто не продвигается дальше него,
    /// at-least-once, как и раньше). Возвращает `Some(commit_offset)`, если
    /// сплошной префикс продвинулся и нужно реально закоммитить в Kafka —
    /// `commit_offset` это offset СЛЕДУЮЩЕЙ непрочитанной записи (Kafka commit
    /// semantics: "что читать дальше", не "что было последним обработано").
    pub fn mark_done(&self, key: &PartitionKey, offset: i64) -> Option<i64> {
        let mut partitions = self.partitions.lock().expect("offset tracker mutex poisoned");
        let state = partitions.entry(key.clone()).or_default();

        let next = *state.next_to_commit.get_or_insert(offset);
        if offset < next {
            // Уже позади текущего watermark (повтор/гонка) — безопасный no-op,
            // не откатываем commit-точку назад.
            return None;
        }
        state.completed_out_of_order.insert(offset);

        let mut advanced = false;
        while state.completed_out_of_order.remove(&state.next_to_commit.expect("only set above")) {
            *state.next_to_commit.as_mut().expect("only set above") += 1;
            advanced = true;
        }
        if advanced { state.next_to_commit } else { None }
    }
}

impl Default for OffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> PartitionKey {
        PartitionKey { topic: "t".to_string(), partition: 0 }
    }

    #[test]
    fn in_order_completion_commits_immediately_each_time() {
        let tracker = OffsetTracker::new();
        tracker.observe_received(&key(), 10);
        assert_eq!(tracker.mark_done(&key(), 10), Some(11));
        tracker.observe_received(&key(), 11);
        assert_eq!(tracker.mark_done(&key(), 11), Some(12));
        tracker.observe_received(&key(), 12);
        assert_eq!(tracker.mark_done(&key(), 12), Some(13));
    }

    #[test]
    fn out_of_order_completion_withholds_commit_until_gap_fills() {
        let tracker = OffsetTracker::new();
        // Получены строго по порядку (как всегда бывает — один poll-цикл),
        // но ЗАВЕРШАЮТСЯ вперемешку (конкурентная обработка).
        tracker.observe_received(&key(), 10);
        tracker.observe_received(&key(), 11);
        tracker.observe_received(&key(), 12);

        // 12 обработался раньше 10 и 11 — коммит не должен продвинуться вообще.
        assert_eq!(tracker.mark_done(&key(), 12), None);
        assert_eq!(tracker.mark_done(&key(), 11), None);
        // Дыра (10) заполняется последней — сплошной префикс продвигается
        // сразу до 13 (10, 11 и 12 все теперь позади watermark).
        assert_eq!(tracker.mark_done(&key(), 10), Some(13));
    }

    #[test]
    fn different_partitions_tracked_independently() {
        let tracker = OffsetTracker::new();
        let p0 = PartitionKey { topic: "t".to_string(), partition: 0 };
        let p1 = PartitionKey { topic: "t".to_string(), partition: 1 };
        tracker.observe_received(&p0, 5);
        tracker.observe_received(&p1, 100);
        assert_eq!(tracker.mark_done(&p0, 5), Some(6));
        // p1 начинает свой собственный отсчёт с первого увиденного offset (100), не с 0.
        assert_eq!(tracker.mark_done(&p1, 100), Some(101));
    }

    #[test]
    fn duplicate_mark_done_is_safe_noop_after_already_advanced() {
        let tracker = OffsetTracker::new();
        tracker.observe_received(&key(), 10);
        assert_eq!(tracker.mark_done(&key(), 10), Some(11));
        // Повторная (ошибочная/гоночная) отметка того же offset не должна
        // паниковать и не должна откатить commit-точку назад.
        assert_eq!(tracker.mark_done(&key(), 10), None);
    }

    #[test]
    fn stalled_offset_blocks_commit_progress_for_later_completed_offsets_at_least_once() {
        // Тот же класс поведения, что уже был у последовательной версии:
        // если offset=10 никогда не завершится (перманентная ошибка), 11 и 12
        // не должны закоммититься тоже — at-least-once, не at-most-once.
        let tracker = OffsetTracker::new();
        tracker.observe_received(&key(), 10);
        tracker.observe_received(&key(), 11);
        tracker.observe_received(&key(), 12);
        assert_eq!(tracker.mark_done(&key(), 11), None);
        assert_eq!(tracker.mark_done(&key(), 12), None);
        // offset 10 никогда не помечается done — watermark навсегда застревает.
    }
}
