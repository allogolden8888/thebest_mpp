//! Admission control для REST ingress.
//!
//! Каждая реплика строит собственный полный snapshot compacted-топика
//! `execution.control`: consumer не вступает в consumer group, а вручную
//! назначает себе все партиции с `Beginning`. До достижения EOF всех
//! партиций и получения записи GLOBAL ingress работает fail-closed.
//! После bootstrap при потере Kafka действует fail-static — последний
//! подтверждённый snapshot остаётся активным (HLD §8.6).

use crate::proto::{common, events};
use prost::Message as ProstMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const EXECUTION_CONTROL_TOPIC: &str = "execution.control";
pub const DEFAULT_RETRY_AFTER_SECONDS: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Admit,
    Reject { retry_after_seconds: u32 },
}

pub trait AdmissionGate: Send + Sync {
    fn check(&self, partner_id: &str) -> AdmissionDecision;
}

/// Только тестовый gate для unit-тестов HTTP-валидации, которым admission
/// не является предметом проверки. Production main всегда использует
/// `SnapshotAdmissionGate` и не может случайно собрать fail-open путь.
#[cfg(test)]
pub struct AlwaysAdmit;

#[cfg(test)]
impl AdmissionGate for AlwaysAdmit {
    fn check(&self, _partner_id: &str) -> AdmissionDecision {
        AdmissionDecision::Admit
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ControlKey {
    Global,
    Partner(String),
}

#[derive(Debug, Clone, Copy)]
struct ControlValue {
    state: common::ExecutionControlState,
    admission_rate: f64,
    /// Manual override TTL. После expiry запись больше не применима даже
    /// если producer не успел опубликовать следующее compacted-значение.
    expires_at: Option<(i64, i32)>,
}

impl ControlValue {
    fn is_expired_at(&self, now: (i64, i32)) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now)
    }
}

#[derive(Default)]
struct SnapshotState {
    /// Bootstrap считается завершённым только после EOF каждой партиции,
    /// которую Kafka metadata вернула для compacted-топика.
    bootstrapped: bool,
    records: HashMap<ControlKey, ControlValue>,
}

/// Потокобезопасный локальный snapshot. Отсутствие PARTNER-записи означает,
/// что применяется только GLOBAL. Отсутствие GLOBAL после bootstrap —
/// небезопасное/неинициализированное состояние, поэтому `is_ready=false`.
#[derive(Default)]
pub struct ControlSnapshot {
    state: RwLock<SnapshotState>,
}

impl ControlSnapshot {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, SnapshotState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SnapshotState> {
        self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Readiness означает не просто доступность процесса: полный initial
    /// replay завершён и в снапшоте есть GLOBAL sentinel от владельца
    /// контракта. Поэтому свежая реплика не открывает ingress на пустом или
    /// недочитанном control topic.
    pub fn is_ready(&self) -> bool {
        let state = self.read();
        state.bootstrapped
            && state
                .records
                .get(&ControlKey::Global)
                .is_some_and(|value| !value.is_expired_at(now_epoch()))
    }

    fn install_bootstrap(&self, records: HashMap<ControlKey, ControlValue>) {
        let mut state = self.write();
        state.records = records;
        state.bootstrapped = true;
    }

    fn apply(&self, key: ControlKey, value: ControlValue) {
        self.write().records.insert(key, value);
    }

    fn delete(&self, key: &ControlKey) {
        self.write().records.remove(key);
    }

    fn effective_rate_at(&self, partner_id: &str, now: (i64, i32)) -> Option<f64> {
        let state = self.read();
        if !state.bootstrapped {
            return None;
        }
        let global = state.records.get(&ControlKey::Global)?;
        if global.is_expired_at(now) || global.state == common::ExecutionControlState::Paused {
            return None;
        }

        let mut rate = global.admission_rate;
        if let Some(partner) = state.records.get(&ControlKey::Partner(partner_id.to_string())) {
            if !partner.is_expired_at(now) {
                if partner.state == common::ExecutionControlState::Paused {
                    return None;
                }
                rate = rate.min(partner.admission_rate);
            }
        }
        Some(rate)
    }

    fn effective_rate(&self, partner_id: &str) -> Option<f64> {
        self.effective_rate_at(partner_id, now_epoch())
    }

    #[cfg(test)]
    pub fn ready_for_test() -> Arc<Self> {
        let snapshot = Arc::new(Self::default());
        snapshot.install_bootstrap(HashMap::from([(
            ControlKey::Global,
            ControlValue {
                state: common::ExecutionControlState::Active,
                admission_rate: 1.0,
                expires_at: None,
            },
        )]));
        snapshot
    }
}

fn now_epoch() -> (i64, i32) {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos() as i32),
        Err(_) => (0, 0),
    }
}

/// Реальный admission gate: PAUSED и rate=0 отклоняют весь применимый поток,
/// DEGRADED/частичный `admission_rate` детерминированно семплируется. Счётчик
/// нужен только для распределения решений и не является durable state.
pub struct SnapshotAdmissionGate {
    snapshot: Arc<ControlSnapshot>,
    retry_after_seconds: u32,
    sequence: AtomicU64,
}

impl SnapshotAdmissionGate {
    pub fn new(snapshot: Arc<ControlSnapshot>, retry_after_seconds: u32) -> Self {
        Self {
            snapshot,
            retry_after_seconds: retry_after_seconds.max(1),
            sequence: AtomicU64::new(0),
        }
    }

    fn check_with_sample(&self, partner_id: &str, sample: u64) -> AdmissionDecision {
        let Some(rate) = self.snapshot.effective_rate(partner_id) else {
            return AdmissionDecision::Reject { retry_after_seconds: self.retry_after_seconds };
        };
        if rate >= 1.0 {
            return AdmissionDecision::Admit;
        }
        if rate <= 0.0 {
            return AdmissionDecision::Reject { retry_after_seconds: self.retry_after_seconds };
        }

        // Не используем thread_rng на hot path и не добавляем зависимость:
        // hash(partner_id, monotonic sequence) даёт устойчивое распределение
        // по [0, 1_000_000), достаточное для probabilistic admission.
        let mut hasher = DefaultHasher::new();
        partner_id.hash(&mut hasher);
        sample.hash(&mut hasher);
        let bucket = hasher.finish() % 1_000_000;
        let threshold = (rate * 1_000_000.0) as u64;
        if bucket < threshold {
            AdmissionDecision::Admit
        } else {
            AdmissionDecision::Reject { retry_after_seconds: self.retry_after_seconds }
        }
    }
}

impl AdmissionGate for SnapshotAdmissionGate {
    fn check(&self, partner_id: &str) -> AdmissionDecision {
        let sample = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.check_with_sample(partner_id, sample)
    }
}

fn record_key(scope: common::ExecutionControlScope, scope_id: &str) -> Result<Option<ControlKey>, String> {
    match scope {
        common::ExecutionControlScope::Global if scope_id.is_empty() => Ok(Some(ControlKey::Global)),
        common::ExecutionControlScope::Global => Err("GLOBAL execution.control должен иметь пустой scope_id".to_string()),
        common::ExecutionControlScope::Partner if !scope_id.is_empty() => {
            Ok(Some(ControlKey::Partner(scope_id.to_string())))
        }
        common::ExecutionControlScope::Partner => Err("PARTNER execution.control должен иметь непустой scope_id".to_string()),
        // Эти записи валидны, но не применимы на ingress: стадия и маршрут
        // ещё не выбраны. Их проверяют downstream consumers.
        common::ExecutionControlScope::Stage
        | common::ExecutionControlScope::PartnerStage
        | common::ExecutionControlScope::OperatorRoute => Ok(None),
        common::ExecutionControlScope::Unspecified => Err("execution.control содержит UNSPECIFIED scope".to_string()),
    }
}

fn decode_record(payload: &[u8]) -> Result<(Option<ControlKey>, ControlValue), String> {
    let record = events::ExecutionControlRecord::decode(payload)
        .map_err(|e| format!("не удалось декодировать ExecutionControlRecord: {e}"))?;
    let scope = common::ExecutionControlScope::try_from(record.scope)
        .map_err(|_| format!("execution.control содержит неизвестный scope={}", record.scope))?;
    let state = common::ExecutionControlState::try_from(record.state)
        .map_err(|_| format!("execution.control содержит неизвестный state={}", record.state))?;
    if state == common::ExecutionControlState::Unspecified {
        return Err("execution.control содержит UNSPECIFIED state".to_string());
    }
    if !record.admission_rate.is_finite() || !(0.0..=1.0).contains(&record.admission_rate) {
        return Err(format!("execution.control содержит admission_rate вне [0,1]: {}", record.admission_rate));
    }
    let expires_at = match record.expires_at {
        Some(timestamp) if !(0..1_000_000_000).contains(&timestamp.nanos) => {
            return Err(format!("execution.control содержит некорректный expires_at nanos={}", timestamp.nanos));
        }
        Some(timestamp) => Some((timestamp.seconds, timestamp.nanos)),
        None => None,
    };
    Ok((record_key(scope, &record.scope_id)?, ControlValue {
        state,
        admission_rate: record.admission_rate,
        expires_at,
    }))
}

fn parse_tombstone_key(raw: &[u8]) -> Result<Option<ControlKey>, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("execution.control key не UTF-8: {e}"))?;
    let (scope_name, scope_id) = text
        .split_once(':')
        .ok_or_else(|| format!("execution.control key {text:?} не в формате SCOPE:scope_id"))?;
    let scope = common::ExecutionControlScope::from_str_name(scope_name)
        .ok_or_else(|| format!("execution.control key содержит неизвестный scope {scope_name:?}"))?;
    record_key(scope, scope_id)
}

fn validate_kafka_key(key: Option<&[u8]>, expected: &ControlKey) -> Result<(), String> {
    let actual = parse_tombstone_key(key.ok_or_else(|| "execution.control record без Kafka key".to_string())?)?;
    if actual.as_ref() != Some(expected) {
        return Err("execution.control Kafka key не совпадает с protobuf scope/scope_id".to_string());
    }
    Ok(())
}

fn apply_payload(
    records: &mut HashMap<ControlKey, ControlValue>,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) -> Result<(), String> {
    match payload {
        Some(payload) => {
            let (control_key, value) = decode_record(payload)?;
            if let Some(control_key) = control_key {
                validate_kafka_key(key, &control_key)?;
                records.insert(control_key, value);
            }
        }
        None => {
            if let Some(control_key) = parse_tombstone_key(
                key.ok_or_else(|| "execution.control tombstone без Kafka key".to_string())?,
            )? {
                records.remove(&control_key);
            }
        }
    }
    Ok(())
}

async fn consume_session(brokers: &[String], snapshot: &Arc<ControlSnapshot>) -> Result<(), String> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers.join(","))
        // При manual assign consumer group не выполняет balancing и не делит
        // партиции между репликами; group.id остаётся только обязательным
        // client property. Офсеты не коммитятся.
        .set("group.id", format!("partner-rest-receiver-control-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
    let consumer: StreamConsumer = config.create().map_err(|e| format!("не удалось создать Kafka consumer: {e}"))?;

    let metadata = consumer
        .fetch_metadata(Some(EXECUTION_CONTROL_TOPIC), Timeout::After(Duration::from_secs(10)))
        .map_err(|e| format!("не удалось получить metadata {EXECUTION_CONTROL_TOPIC}: {e}"))?;
    let topic = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == EXECUTION_CONTROL_TOPIC)
        .ok_or_else(|| format!("Kafka metadata не содержит topic {EXECUTION_CONTROL_TOPIC}"))?;
    if topic.partitions().is_empty() {
        return Err(format!("topic {EXECUTION_CONTROL_TOPIC} не содержит партиций"));
    }

    let mut assignment = TopicPartitionList::new();
    let mut pending_eof = HashSet::new();
    for partition in topic.partitions() {
        assignment
            .add_partition_offset(EXECUTION_CONTROL_TOPIC, partition.id(), Offset::Beginning)
            .map_err(|e| format!("не удалось назначить partition {}: {e}", partition.id()))?;
        pending_eof.insert(partition.id());
    }
    consumer.assign(&assignment).map_err(|e| format!("Kafka assign failed: {e}"))?;

    let mut bootstrap = HashMap::new();
    let mut live = false;
    loop {
        match consumer.recv().await {
            Ok(message) => {
                let key = message.key();
                let payload = message.payload();
                if live {
                    match payload {
                        Some(payload) => {
                            let (control_key, value) = decode_record(payload)?;
                            if let Some(control_key) = control_key {
                                validate_kafka_key(key, &control_key)?;
                                snapshot.apply(control_key, value);
                            }
                        }
                        None => {
                            if let Some(control_key) = parse_tombstone_key(
                                key.ok_or_else(|| "execution.control tombstone без Kafka key".to_string())?,
                            )? {
                                snapshot.delete(&control_key);
                            }
                        }
                    }
                } else {
                    apply_payload(&mut bootstrap, key, payload)?;
                }
            }
            Err(KafkaError::PartitionEOF(partition)) => {
                if !live {
                    pending_eof.remove(&partition);
                    if pending_eof.is_empty() {
                        snapshot.install_bootstrap(std::mem::take(&mut bootstrap));
                        live = true;
                        tracing::info!(
                            ready = snapshot.is_ready(),
                            "initial execution.control snapshot полностью прочитан"
                        );
                    }
                }
            }
            Err(error) => return Err(format!("execution.control consumer error: {error}")),
        }
    }
}

/// Бесконечный reconnect-loop. На первом старте snapshot закрыт; после
/// успешного bootstrap reconnect не очищает последнее состояние, сохраняя
/// fail-static семантику при отказе Kafka/Execution Control Service.
pub async fn run_control_consumer(brokers: Vec<String>, snapshot: Arc<ControlSnapshot>) {
    loop {
        if let Err(error) = consume_session(&brokers, &snapshot).await {
            tracing::error!(%error, "execution.control consumer будет переподключён; действует последний подтверждённый snapshot");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(state: common::ExecutionControlState, rate: f64) -> ControlValue {
        ControlValue { state, admission_rate: rate, expires_at: None }
    }

    fn ready_snapshot(global: ControlValue) -> Arc<ControlSnapshot> {
        let snapshot = Arc::new(ControlSnapshot::default());
        snapshot.install_bootstrap(HashMap::from([(ControlKey::Global, global)]));
        snapshot
    }

    #[test]
    fn startup_is_fail_closed_and_not_ready() {
        let snapshot = Arc::new(ControlSnapshot::default());
        let gate = SnapshotAdmissionGate::new(snapshot.clone(), 7);
        assert!(!snapshot.is_ready());
        assert_eq!(gate.check("click_uz"), AdmissionDecision::Reject { retry_after_seconds: 7 });
    }

    #[test]
    fn empty_bootstrap_stays_fail_closed_until_global_sentinel_arrives() {
        let snapshot = Arc::new(ControlSnapshot::default());
        snapshot.install_bootstrap(HashMap::new());
        assert!(!snapshot.is_ready());

        snapshot.apply(ControlKey::Global, value(common::ExecutionControlState::Active, 1.0));
        assert!(snapshot.is_ready());
        assert_eq!(
            SnapshotAdmissionGate::new(snapshot, 1).check("click_uz"),
            AdmissionDecision::Admit
        );
    }

    #[test]
    fn global_pause_rejects_every_partner() {
        let snapshot = ready_snapshot(value(common::ExecutionControlState::Paused, 0.0));
        let gate = SnapshotAdmissionGate::new(snapshot, 3);
        assert_eq!(gate.check("click_uz"), AdmissionDecision::Reject { retry_after_seconds: 3 });
        assert_eq!(gate.check("other"), AdmissionDecision::Reject { retry_after_seconds: 3 });
    }

    #[test]
    fn partner_pause_is_scoped_and_tombstone_restores_global_state() {
        let snapshot = ready_snapshot(value(common::ExecutionControlState::Active, 1.0));
        let partner_key = ControlKey::Partner("click_uz".to_string());
        snapshot.apply(partner_key.clone(), value(common::ExecutionControlState::Paused, 0.0));
        let gate = SnapshotAdmissionGate::new(snapshot.clone(), 1);
        assert!(matches!(gate.check("click_uz"), AdmissionDecision::Reject { .. }));
        assert_eq!(gate.check("other"), AdmissionDecision::Admit);

        snapshot.delete(&partner_key);
        assert_eq!(gate.check("click_uz"), AdmissionDecision::Admit);
    }

    #[test]
    fn expired_partner_override_no_longer_blocks_but_expired_global_fails_closed() {
        let snapshot = ready_snapshot(value(common::ExecutionControlState::Active, 1.0));
        snapshot.apply(
            ControlKey::Partner("click_uz".to_string()),
            ControlValue {
                state: common::ExecutionControlState::Paused,
                admission_rate: 0.0,
                expires_at: Some((100, 0)),
            },
        );
        assert_eq!(snapshot.effective_rate_at("click_uz", (101, 0)), Some(1.0));

        snapshot.apply(
            ControlKey::Global,
            ControlValue {
                state: common::ExecutionControlState::Active,
                admission_rate: 1.0,
                expires_at: Some((100, 0)),
            },
        );
        assert_eq!(snapshot.effective_rate_at("click_uz", (101, 0)), None);
    }

    #[test]
    fn partial_admission_rate_is_actually_enforced() {
        let snapshot = ready_snapshot(value(common::ExecutionControlState::Degraded, 0.25));
        let gate = SnapshotAdmissionGate::new(snapshot, 1);
        let admitted = (0..10_000)
            .filter(|sample| gate.check_with_sample("click_uz", *sample) == AdmissionDecision::Admit)
            .count();
        assert!((2_300..=2_700).contains(&admitted), "ожидали около 25%, получили {admitted}/10000");
    }

    #[test]
    fn malformed_rate_is_rejected_before_it_can_replace_snapshot() {
        let record = events::ExecutionControlRecord {
            scope: common::ExecutionControlScope::Global as i32,
            scope_id: String::new(),
            state: common::ExecutionControlState::Active as i32,
            admission_rate: f64::NAN,
            ..Default::default()
        };
        assert!(decode_record(&record.encode_to_vec()).is_err());
    }

    #[test]
    fn tombstone_key_uses_the_publishers_contract() {
        assert_eq!(parse_tombstone_key(b"EXECUTION_CONTROL_SCOPE_GLOBAL:").unwrap(), Some(ControlKey::Global));
        assert_eq!(
            parse_tombstone_key(b"EXECUTION_CONTROL_SCOPE_PARTNER:click_uz").unwrap(),
            Some(ControlKey::Partner("click_uz".to_string()))
        );
    }
}
