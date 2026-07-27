//! `cas_transition_and_track_deadline`/`finalize_pipeline`
//! (service_internal_methods.md §1.4, development_plan.md 4.2) — реальная
//! Redis-backed CAS-реализация `ExecutionState`, заменяющая
//! `Arc<Mutex<HashMap>>` (см. `kafka_io.rs` до этой правки) настоящим
//! атомарным Lua-скриптом. Это единственное изменение, которое снимает
//! задокументированный блокер "не работает с более чем одной репликой" —
//! `exec:{message_id}` теперь общее состояние в Runtime Redis, видимое
//! любой реплике, а не приватная память одного пода.
//!
//! `DEADLINE_BUCKETS = 16` — не выбрано произвольно: `CODE_REVIEW.md`
//! (независимая проверка Субагента 1's `scheduler-critical-sweep`) уже
//! нашло, что тот сервис жёстко использует `DEADLINE_BUCKETS=16` как
//! локальную константу "с нулевой проверкой, что она совпадает с реальным
//! числом бакетов Pipeline Engine" — несовпадение тихо теряло бы часть
//! дедлайнов в бакеты, которые Critical Sweep никогда не сканирует. Здесь
//! используется то же число, чтобы не воспроизвести ровно ту находку.

use crate::execution_state::ExecutionState;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;

pub const DEADLINE_BUCKETS: u64 = 16;

const CAS_TRANSITION_SCRIPT: &str = include_str!("../lua/cas_transition.lua");
const FINALIZE_SCRIPT: &str = include_str!("../lua/finalize.lua");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    Ok,
    /// Конфликт — реальный `awaiting_stage_execution_id`, найденный в Redis
    /// (может отличаться от того, что вызывающая сторона считала текущим).
    Conflict { actual_awaiting: String },
}

/// `bucket = hash(stage_execution_id) % N` (data_infrastructure_spec.md §283) —
/// не криптографический хэш, детерминированный и стабильный, тем и достаточен.
pub fn bucket_for(stage_execution_id: &str, num_buckets: u64) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    stage_execution_id.hash(&mut hasher);
    hasher.finish() % num_buckets
}

pub struct RedisStateStore {
    client: redis::Client,
}

impl RedisStateStore {
    pub fn new(redis_url: &str) -> Result<Self, String> {
        let client = redis::Client::open(redis_url).map_err(|e| format!("невалидный REDIS_RUNTIME_URL: {e}"))?;
        Ok(Self { client })
    }

    async fn connection(&self) -> Result<MultiplexedConnection, String> {
        self.client.get_multiplexed_async_connection().await.map_err(|e| format!("не удалось подключиться к Runtime Redis: {e}"))
    }

    /// `load_current_state` (упрощённое имя из hld.md §7.2 табличного вида) —
    /// обычный `HGETALL`, не CAS (только чтение) — используется, чтобы
    /// получить состояние для передачи в чистую функцию `handle_stage_completed`
    /// перед тем, как атомарно записать результат обратно через
    /// `cas_advance`.
    pub async fn load(&self, message_id: &str) -> Result<Option<ExecutionState>, String> {
        let mut conn = self.connection().await?;
        let key = format!("exec:{message_id}");
        let fields: HashMap<String, String> = conn.hgetall(&key).await.map_err(|e| format!("HGETALL {key}: {e}"))?;
        if fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(ExecutionState {
            message_id: message_id.to_string(),
            pipeline_version: fields.get("pipeline_version").and_then(|v| v.parse().ok()).unwrap_or(0),
            current_node_id: fields.get("current_node_id").cloned().unwrap_or_default(),
            attempt: fields.get("attempt").and_then(|v| v.parse().ok()).unwrap_or(1),
            config_versions: HashMap::new(),
            awaiting_stage_execution_id: fields.get("awaiting_stage_execution_id").filter(|s| !s.is_empty()).cloned(),
            resolved_operator_id: fields.get("resolved_operator_id").filter(|s| !s.is_empty()).cloned(),
            category: fields.get("category").filter(|s| !s.is_empty()).cloned(),
            segment_count: fields.get("segment_count").and_then(|v| v.parse().ok()).unwrap_or(0),
            route_id: fields.get("route_id").filter(|s| !s.is_empty()).cloned(),
            protocol: fields.get("protocol").and_then(|v| v.parse::<i32>().ok()).filter(|p| *p >= 0),
            route_version: fields.get("route_version").filter(|s| !s.is_empty()).cloned(),
            destination_address: fields.get("destination_address").cloned().unwrap_or_default(),
            deadline_ms: fields.get("deadline_ms").and_then(|v| v.parse().ok()).unwrap_or(0),
        }))
    }

    /// `cas_transition_and_track_deadline` — атомарная запись нового
    /// состояния, guarded CAS против `expected_awaiting_stage_execution_id`
    /// (пусто — ожидаем, что состояния ещё не существует: используется и
    /// для самого первого диспетча из `handle_incoming`, и одновременно
    /// служит идемпотентностью на входе — см. javadoc/комментарий в
    /// `cas_transition.lua`).
    pub async fn cas_advance(
        &self,
        expected_awaiting_stage_execution_id: Option<&str>,
        old_stage_execution_id_to_remove: Option<&str>,
        new_state: &ExecutionState,
    ) -> Result<CasOutcome, String> {
        let mut conn = self.connection().await?;
        let exec_key = format!("exec:{}", new_state.message_id);
        let new_stage_execution_id = new_state.awaiting_stage_execution_id.clone().unwrap_or_default();
        let bucket = bucket_for(&new_stage_execution_id, DEADLINE_BUCKETS);
        let deadlines_key = format!("deadlines:{bucket}");

        let result: Vec<String> = redis::Script::new(CAS_TRANSITION_SCRIPT)
            .key(&exec_key)
            .key(&deadlines_key)
            .arg(expected_awaiting_stage_execution_id.unwrap_or(""))
            .arg(new_state.pipeline_version)
            .arg(&new_state.current_node_id)
            .arg(new_state.attempt)
            .arg(&new_stage_execution_id)
            .arg(new_state.resolved_operator_id.as_deref().unwrap_or(""))
            .arg(new_state.category.as_deref().unwrap_or(""))
            .arg(new_state.segment_count)
            .arg(new_state.route_id.as_deref().unwrap_or(""))
            .arg(new_state.protocol.unwrap_or(-1))
            .arg(new_state.route_version.as_deref().unwrap_or(""))
            .arg(&new_state.destination_address)
            .arg(new_state.deadline_ms)
            .arg(old_stage_execution_id_to_remove.unwrap_or(""))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| format!("EVAL cas_transition: {e}"))?;

        parse_cas_result(result)
    }

    /// `finalize_pipeline` — атомарное удаление состояния + записи дедлайна.
    pub async fn finalize(&self, message_id: &str, expected_awaiting_stage_execution_id: &str) -> Result<CasOutcome, String> {
        let mut conn = self.connection().await?;
        let exec_key = format!("exec:{message_id}");
        let bucket = bucket_for(expected_awaiting_stage_execution_id, DEADLINE_BUCKETS);
        let deadlines_key = format!("deadlines:{bucket}");

        let result: Vec<String> = redis::Script::new(FINALIZE_SCRIPT)
            .key(&exec_key)
            .key(&deadlines_key)
            .arg(expected_awaiting_stage_execution_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| format!("EVAL finalize: {e}"))?;

        parse_cas_result(result)
    }
}

fn parse_cas_result(result: Vec<String>) -> Result<CasOutcome, String> {
    match result.first().map(String::as_str) {
        Some("OK") => Ok(CasOutcome::Ok),
        Some("CONFLICT") => Ok(CasOutcome::Conflict { actual_awaiting: result.get(1).cloned().unwrap_or_default() }),
        other => Err(format!("неожиданный ответ Lua-скрипта: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_for_is_deterministic() {
        let b1 = bucket_for("se-123", DEADLINE_BUCKETS);
        let b2 = bucket_for("se-123", DEADLINE_BUCKETS);
        assert_eq!(b1, b2);
        assert!(b1 < DEADLINE_BUCKETS);
    }

    #[test]
    fn bucket_for_distributes_across_range() {
        // Не строгое доказательство равномерности — просто что не все
        // значения схлопываются в один и тот же бакет для разных id.
        let buckets: std::collections::HashSet<u64> =
            (0..100).map(|i| bucket_for(&format!("se-{i}"), DEADLINE_BUCKETS)).collect();
        assert!(buckets.len() > 1, "100 разных stage_execution_id должны попасть больше чем в 1 бакет");
    }

    fn redis_url() -> String {
        std::env::var("PIPELINE_ENGINE_TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
    }

    fn sample_state(message_id: &str, stage_execution_id: &str) -> ExecutionState {
        ExecutionState {
            message_id: message_id.to_string(),
            pipeline_version: 1,
            current_node_id: "n1_destination_resolution".to_string(),
            attempt: 1,
            config_versions: HashMap::new(),
            awaiting_stage_execution_id: Some(stage_execution_id.to_string()),
            resolved_operator_id: None,
            category: None,
            segment_count: 1,
            route_id: None,
            protocol: None,
            route_version: None,
            destination_address: "998901331835".to_string(),
            deadline_ms: 1_000_000,
        }
    }

    /// Реальный round-trip против локального Redis (brew) — не мок.
    #[tokio::test]
    async fn cas_advance_then_load_round_trips() {
        let store = RedisStateStore::new(&redis_url()).unwrap();
        let message_id = format!("test-msg-{}", uuid::Uuid::new_v4());
        let state = sample_state(&message_id, "se1");

        let outcome = store.cas_advance(None, None, &state).await.unwrap();
        assert_eq!(outcome, CasOutcome::Ok);

        let loaded = store.load(&message_id).await.unwrap().expect("состояние должно быть найдено после cas_advance");
        assert_eq!(loaded.current_node_id, "n1_destination_resolution");
        assert_eq!(loaded.awaiting_stage_execution_id.as_deref(), Some("se1"));
        assert_eq!(loaded.destination_address, "998901331835");

        store.finalize(&message_id, "se1").await.unwrap();
    }

    #[tokio::test]
    async fn cas_advance_rejects_wrong_expected_value() {
        let store = RedisStateStore::new(&redis_url()).unwrap();
        let message_id = format!("test-msg-{}", uuid::Uuid::new_v4());
        let state = sample_state(&message_id, "se1");
        store.cas_advance(None, None, &state).await.unwrap();

        // Кто-то другой (или мы сами по ошибке) думает, что всё ещё ждём
        // se-wrong, хотя реально уже se1.
        let mut next_state = state.clone();
        next_state.current_node_id = "n2_policy".to_string();
        next_state.awaiting_stage_execution_id = Some("se2".to_string());
        let outcome = store.cas_advance(Some("se-wrong"), Some("se1"), &next_state).await.unwrap();

        match outcome {
            CasOutcome::Conflict { actual_awaiting } => assert_eq!(actual_awaiting, "se1"),
            CasOutcome::Ok => panic!("ожидали Conflict — expected не совпадал с реальным awaiting_stage_execution_id"),
        }

        // Состояние не должно было измениться после отклонённого CAS.
        let loaded = store.load(&message_id).await.unwrap().unwrap();
        assert_eq!(loaded.current_node_id, "n1_destination_resolution");
        store.finalize(&message_id, "se1").await.unwrap();
    }

    /// Прямое доказательство того, зачем это вообще написано: два "реплики"
    /// (два вызова cas_advance с expected=None, имитируя два пода, оба
    /// решившие, что message_id ещё не существует) гоняются за создание
    /// одного и того же начального состояния — только одна должна победить.
    #[tokio::test]
    async fn concurrent_initial_cas_only_one_replica_wins() {
        let store = std::sync::Arc::new(RedisStateStore::new(&redis_url()).unwrap());
        let message_id = format!("test-msg-{}", uuid::Uuid::new_v4());

        let s1 = sample_state(&message_id, "se-from-replica-1");
        let s2 = sample_state(&message_id, "se-from-replica-2");

        let store_a = store.clone();
        let store_b = store.clone();
        let (r1, r2) = tokio::join!(
            tokio::spawn(async move { store_a.cas_advance(None, None, &s1).await }),
            tokio::spawn(async move { store_b.cas_advance(None, None, &s2).await }),
        );
        let r1 = r1.unwrap().unwrap();
        let r2 = r2.unwrap().unwrap();

        let ok_count = [&r1, &r2].iter().filter(|r| ***r == CasOutcome::Ok).count();
        assert_eq!(ok_count, 1, "ровно одна из двух конкурентных попыток инициализации должна победить, не обе и не ноль");

        let loaded = store.load(&message_id).await.unwrap().unwrap();
        let winning_id = loaded.awaiting_stage_execution_id.unwrap();
        assert!(winning_id == "se-from-replica-1" || winning_id == "se-from-replica-2");

        store.finalize(&message_id, &winning_id).await.unwrap();
    }

    #[tokio::test]
    async fn finalize_removes_state_entirely() {
        let store = RedisStateStore::new(&redis_url()).unwrap();
        let message_id = format!("test-msg-{}", uuid::Uuid::new_v4());
        let state = sample_state(&message_id, "se1");
        store.cas_advance(None, None, &state).await.unwrap();

        let outcome = store.finalize(&message_id, "se1").await.unwrap();
        assert_eq!(outcome, CasOutcome::Ok);

        let loaded = store.load(&message_id).await.unwrap();
        assert!(loaded.is_none(), "finalize должен полностью удалить состояние");
    }
}
