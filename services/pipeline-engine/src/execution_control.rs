//! Локальный fail-static snapshot compacted-топика `execution.control`.
//!
//! Каждая реплика Pipeline Engine обязана видеть ВСЕ scope, поэтому consumer
//! не участвует в общей consumer group: он вручную назначает себе все
//! партиции и при каждом подключении реплеит их с начала. До EOF каждой
//! партиции и наличия GLOBAL sentinel основной data-plane не запускается
//! (fail-closed bootstrap). После первого успешного bootstrap разрыв с Kafka
//! не очищает snapshot — продолжает действовать последнее подтверждённое
//! состояние (fail-static, HLD §8.6).

use crate::proto::{common, events};
use prost::Message as ProstMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Notify;

pub const EXECUTION_CONTROL_TOPIC: &str = "execution.control";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ControlKey {
    Global,
    Stage(String),
    Partner(String),
    PartnerStage(String),
    OperatorRoute(String),
}

impl ControlKey {
    fn scope(&self) -> common::ExecutionControlScope {
        match self {
            Self::Global => common::ExecutionControlScope::Global,
            Self::Stage(_) => common::ExecutionControlScope::Stage,
            Self::Partner(_) => common::ExecutionControlScope::Partner,
            Self::PartnerStage(_) => common::ExecutionControlScope::PartnerStage,
            Self::OperatorRoute(_) => common::ExecutionControlScope::OperatorRoute,
        }
    }

    fn scope_id(&self) -> &str {
        match self {
            Self::Global => "",
            Self::Stage(id) | Self::Partner(id) | Self::PartnerStage(id) | Self::OperatorRoute(id) => id,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ControlValue {
    state: common::ExecutionControlState,
    admission_rate: f64,
}

#[derive(Default)]
struct SnapshotState {
    bootstrapped: bool,
    records: HashMap<ControlKey, ControlValue>,
}

/// Scope, из-за которого конкретная диспетчеризация должна уйти в hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldScope {
    pub scope: common::ExecutionControlScope,
    pub scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    Admit,
    Hold(HoldScope),
}

#[derive(Debug, Clone, Copy)]
pub struct AdmissionContext<'a> {
    pub partner_id: &'a str,
    pub stage_name: &'a str,
    /// `OPERATOR_ROUTE` становится применим только после Routing.
    pub operator_route_id: Option<&'a str>,
}

/// Потокобезопасный snapshot. После первого успешного bootstrap его нельзя
/// вернуть в пустое/fail-open состояние: невалидная live-запись или reconnect
/// оставляют предыдущую подтверждённую карту без изменений.
#[derive(Default)]
pub struct ControlSnapshot {
    state: RwLock<SnapshotState>,
    ready: Notify,
}

impl ControlSnapshot {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, SnapshotState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, SnapshotState> {
        self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn is_ready(&self) -> bool {
        let state = self.read();
        state.bootstrapped && state.records.contains_key(&ControlKey::Global)
    }

    pub async fn wait_until_ready(&self) {
        loop {
            // Создаём future ДО проверки, иначе notify между проверкой и
            // await мог бы быть потерян и startup завис бы навсегда.
            let notified = self.ready.notified();
            if self.is_ready() {
                return;
            }
            notified.await;
        }
    }

    fn install_bootstrap(&self, records: HashMap<ControlKey, ControlValue>) -> Result<(), String> {
        if !records.contains_key(&ControlKey::Global) {
            return Err("execution.control bootstrap завершён без GLOBAL sentinel".to_string());
        }
        let mut state = self.write();
        state.records = records;
        state.bootstrapped = true;
        drop(state);
        self.ready.notify_waiters();
        Ok(())
    }

    fn apply(&self, key: ControlKey, value: ControlValue) {
        // Порядок Kafka offset внутри compaction key авторитетен. `version`
        // пока нельзя использовать для fencing: registry владельца хранит её
        // in-memory и после рестарта начинает заново с 1.
        self.write().records.insert(key, value);
    }

    fn delete(&self, key: &ControlKey) -> Result<(), String> {
        if key == &ControlKey::Global {
            // GLOBAL — обязательный sentinel. Удаление превратило бы уже
            // работающую реплику в состояние, которое нельзя безопасно
            // представить SchedulerHoldCommand: никакого реального scope,
            // снятие которого разрешило бы release, больше нет.
            return Err("GLOBAL tombstone в execution.control запрещён".to_string());
        }
        self.write().records.remove(key);
        Ok(())
    }

    /// `check_admission` (service_internal_methods.md §1.4).
    ///
    /// Partial admission выполняется на ingress. На межстадийной
    /// диспетчеризации Pipeline удерживает команду при PAUSED либо нулевом
    /// admission_rate; постепенное освобождение выполняет Standard Lane по
    /// dispatch/ramp policy. Отсутствующий non-GLOBAL scope означает ACTIVE,
    /// но пустой/непрогретый snapshot никогда не Admit.
    pub fn check_admission(&self, context: AdmissionContext<'_>) -> AdmissionDecision {
        let state = self.read();
        if !state.bootstrapped || !state.records.contains_key(&ControlKey::Global) {
            return AdmissionDecision::Hold(HoldScope {
                scope: common::ExecutionControlScope::Global,
                scope_id: String::new(),
            });
        }

        let keys = applicable_keys(context);
        // GLOBAL/STAGE Standard Lane проверяет для любого held item. Среди
        // специфичных scope PARTNER идёт первым: его release-проверка также
        // включает производный PARTNER_STAGE ключ. Это наиболее полное
        // представление в текущем single-scope SchedulerHoldCommand.
        for key in keys {
            let blocked = state.records.get(&key).is_some_and(|value| {
                value.state == common::ExecutionControlState::Paused || value.admission_rate <= 0.0
            });
            if blocked {
                return AdmissionDecision::Hold(HoldScope {
                    scope: key.scope(),
                    scope_id: key.scope_id().to_string(),
                });
            }
        }
        AdmissionDecision::Admit
    }

    #[cfg(test)]
    fn ready_for_test(records: impl IntoIterator<Item = (ControlKey, ControlValue)>) -> Arc<Self> {
        let snapshot = Arc::new(Self::default());
        snapshot.install_bootstrap(records.into_iter().collect()).unwrap();
        snapshot
    }
}

fn applicable_keys(context: AdmissionContext<'_>) -> Vec<ControlKey> {
    let mut keys = vec![
        ControlKey::Partner(context.partner_id.to_string()),
        ControlKey::PartnerStage(format!("{}:{}", context.partner_id, context.stage_name)),
    ];
    if let Some(route_id) = context.operator_route_id.filter(|id| !id.is_empty()) {
        keys.push(ControlKey::OperatorRoute(route_id.to_string()));
    }
    keys.push(ControlKey::Stage(context.stage_name.to_string()));
    keys.push(ControlKey::Global);
    keys
}

fn record_key(scope: common::ExecutionControlScope, scope_id: &str) -> Result<ControlKey, String> {
    match scope {
        common::ExecutionControlScope::Global if scope_id.is_empty() => Ok(ControlKey::Global),
        common::ExecutionControlScope::Global => Err("GLOBAL execution.control должен иметь пустой scope_id".to_string()),
        common::ExecutionControlScope::Stage if !scope_id.is_empty() => Ok(ControlKey::Stage(scope_id.to_string())),
        common::ExecutionControlScope::Partner if !scope_id.is_empty() => Ok(ControlKey::Partner(scope_id.to_string())),
        common::ExecutionControlScope::PartnerStage if !scope_id.is_empty() => Ok(ControlKey::PartnerStage(scope_id.to_string())),
        common::ExecutionControlScope::OperatorRoute if !scope_id.is_empty() => Ok(ControlKey::OperatorRoute(scope_id.to_string())),
        common::ExecutionControlScope::Stage
        | common::ExecutionControlScope::Partner
        | common::ExecutionControlScope::PartnerStage
        | common::ExecutionControlScope::OperatorRoute => {
            Err(format!("{scope:?} execution.control должен иметь непустой scope_id"))
        }
        common::ExecutionControlScope::Unspecified => Err("execution.control содержит UNSPECIFIED scope".to_string()),
    }
}

fn decode_record(payload: &[u8]) -> Result<(ControlKey, ControlValue), String> {
    let record = events::ExecutionControlRecord::decode(payload)
        .map_err(|e| format!("не удалось декодировать ExecutionControlRecord: {e}"))?;
    let scope = common::ExecutionControlScope::try_from(record.scope)
        .map_err(|_| format!("execution.control содержит неизвестный scope={}", record.scope))?;
    let control_state = common::ExecutionControlState::try_from(record.state)
        .map_err(|_| format!("execution.control содержит неизвестный state={}", record.state))?;
    if control_state == common::ExecutionControlState::Unspecified {
        return Err("execution.control содержит UNSPECIFIED state".to_string());
    }
    if !record.admission_rate.is_finite() || !(0.0..=1.0).contains(&record.admission_rate) {
        return Err(format!("execution.control содержит admission_rate вне [0,1]: {}", record.admission_rate));
    }
    if !record.dispatch_rate.is_finite() || !(0.0..=1.0).contains(&record.dispatch_rate) {
        return Err(format!("execution.control содержит dispatch_rate вне [0,1]: {}", record.dispatch_rate));
    }
    if let Some(timestamp) = record.expires_at {
        if !(0..1_000_000_000).contains(&timestamp.nanos) {
            return Err(format!("execution.control содержит некорректный expires_at nanos={}", timestamp.nanos));
        }
        // TTL ручного override снимает сам Execution Control Service новой
        // записью. Consumer локально не размыкает PAUSED: это нарушило бы
        // fail-static при недоступности владельца (HLD §8.6).
    }
    Ok((record_key(scope, &record.scope_id)?, ControlValue {
        state: control_state,
        admission_rate: record.admission_rate,
    }))
}

fn parse_kafka_key(raw: &[u8]) -> Result<ControlKey, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("execution.control key не UTF-8: {e}"))?;
    let (scope_name, scope_id) = text
        .split_once(':')
        .ok_or_else(|| format!("execution.control key {text:?} не в формате SCOPE:scope_id"))?;
    let scope = common::ExecutionControlScope::from_str_name(scope_name)
        .ok_or_else(|| format!("execution.control key содержит неизвестный scope {scope_name:?}"))?;
    record_key(scope, scope_id)
}

fn apply_payload(
    records: &mut HashMap<ControlKey, ControlValue>,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) -> Result<(), String> {
    let raw_key = key.ok_or_else(|| "execution.control record без Kafka key".to_string())?;
    let kafka_key = parse_kafka_key(raw_key)?;
    match payload {
        Some(payload) => {
            let (record_key, value) = decode_record(payload)?;
            if kafka_key != record_key {
                return Err("execution.control Kafka key не совпадает с protobuf scope/scope_id".to_string());
            }
            records.insert(record_key, value);
        }
        None => {
            if kafka_key == ControlKey::Global {
                return Err("GLOBAL tombstone в execution.control запрещён".to_string());
            }
            records.remove(&kafka_key);
        }
    }
    Ok(())
}

async fn consume_session(brokers: &[String], snapshot: &Arc<ControlSnapshot>) -> Result<(), String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers.join(","))
        // Manual assign: каждая реплика читает все партиции сама; group.id
        // технический, офсеты намеренно не коммитятся.
        .set("group.id", format!("pipeline-engine-control-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|e| format!("не удалось создать execution.control consumer: {e}"))?;

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
            .map_err(|e| format!("не удалось назначить execution.control partition {}: {e}", partition.id()))?;
        pending_eof.insert(partition.id());
    }
    consumer.assign(&assignment).map_err(|e| format!("execution.control assign failed: {e}"))?;

    let mut bootstrap = HashMap::new();
    let mut live = false;
    loop {
        match consumer.recv().await {
            Ok(message) => {
                if live {
                    let raw_key = message.key().ok_or_else(|| "execution.control record без Kafka key".to_string())?;
                    let kafka_key = parse_kafka_key(raw_key)?;
                    match message.payload() {
                        Some(payload) => {
                            let (record_key, value) = decode_record(payload)?;
                            if kafka_key != record_key {
                                return Err("execution.control Kafka key не совпадает с protobuf scope/scope_id".to_string());
                            }
                            snapshot.apply(record_key, value);
                        }
                        None => snapshot.delete(&kafka_key)?,
                    }
                } else {
                    apply_payload(&mut bootstrap, message.key(), message.payload())?;
                }
            }
            Err(KafkaError::PartitionEOF(partition)) if !live => {
                pending_eof.remove(&partition);
                if pending_eof.is_empty() {
                    snapshot.install_bootstrap(std::mem::take(&mut bootstrap))?;
                    live = true;
                    tracing::info!("initial execution.control snapshot полностью прочитан; Pipeline Engine готов к dispatch");
                }
            }
            Err(KafkaError::PartitionEOF(_)) => {}
            Err(error) => return Err(format!("execution.control consumer error: {error}")),
        }
    }
}

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

    fn value(state: common::ExecutionControlState, admission_rate: f64, _version: i64) -> ControlValue {
        ControlValue { state, admission_rate }
    }

    fn active_global() -> (ControlKey, ControlValue) {
        (ControlKey::Global, value(common::ExecutionControlState::Active, 1.0, 1))
    }

    fn context<'a>(partner_id: &'a str, stage_name: &'a str, route: Option<&'a str>) -> AdmissionContext<'a> {
        AdmissionContext { partner_id, stage_name, operator_route_id: route }
    }

    #[test]
    fn startup_is_fail_closed() {
        let snapshot = ControlSnapshot::default();
        assert!(!snapshot.is_ready());
        assert_eq!(
            snapshot.check_admission(context("p1", "BILLING", None)),
            AdmissionDecision::Hold(HoldScope { scope: common::ExecutionControlScope::Global, scope_id: String::new() })
        );
    }

    #[test]
    fn bootstrap_requires_global_sentinel() {
        let snapshot = ControlSnapshot::default();
        assert!(snapshot.install_bootstrap(HashMap::new()).is_err());
        assert!(!snapshot.is_ready());
    }

    #[test]
    fn all_applicable_scopes_can_hold_and_unrelated_scopes_do_not() {
        let cases = [
            (ControlKey::Global, common::ExecutionControlScope::Global, ""),
            (ControlKey::Stage("BILLING".into()), common::ExecutionControlScope::Stage, "BILLING"),
            (ControlKey::Partner("p1".into()), common::ExecutionControlScope::Partner, "p1"),
            (ControlKey::PartnerStage("p1:BILLING".into()), common::ExecutionControlScope::PartnerStage, "p1:BILLING"),
            (ControlKey::OperatorRoute("route-1".into()), common::ExecutionControlScope::OperatorRoute, "route-1"),
        ];
        for (key, expected_scope, expected_id) in cases {
            let records = if key == ControlKey::Global {
                vec![(key, value(common::ExecutionControlState::Paused, 0.0, 2))]
            } else {
                vec![active_global(), (key, value(common::ExecutionControlState::Paused, 0.0, 2))]
            };
            let snapshot = ControlSnapshot::ready_for_test(records);
            assert_eq!(
                snapshot.check_admission(context("p1", "BILLING", Some("route-1"))),
                AdmissionDecision::Hold(HoldScope { scope: expected_scope, scope_id: expected_id.into() })
            );
            assert_eq!(
                snapshot.check_admission(context("other", "POLICY", Some("other-route"))),
                if expected_scope == common::ExecutionControlScope::Global {
                    AdmissionDecision::Hold(HoldScope { scope: expected_scope, scope_id: expected_id.into() })
                } else {
                    AdmissionDecision::Admit
                }
            );
        }
    }

    #[test]
    fn zero_rate_holds_even_if_state_is_active() {
        let snapshot = ControlSnapshot::ready_for_test([
            active_global(),
            (ControlKey::Partner("p1".into()), value(common::ExecutionControlState::Active, 0.0, 1)),
        ]);
        assert!(matches!(snapshot.check_admission(context("p1", "POLICY", None)), AdmissionDecision::Hold(_)));
    }

    #[test]
    fn degraded_positive_rate_is_admitted_for_scheduler_ramp_not_resampled() {
        let snapshot = ControlSnapshot::ready_for_test([
            active_global(),
            (ControlKey::Stage("DELIVERY".into()), value(common::ExecutionControlState::Degraded, 0.25, 1)),
        ]);
        assert_eq!(snapshot.check_admission(context("p1", "DELIVERY", None)), AdmissionDecision::Admit);
    }

    #[test]
    fn later_kafka_record_is_authoritative_even_if_owner_version_restarted() {
        let snapshot = ControlSnapshot::ready_for_test([
            active_global(),
            (ControlKey::Partner("p1".into()), value(common::ExecutionControlState::Paused, 0.0, 5)),
        ]);
        snapshot.apply(ControlKey::Partner("p1".into()), value(common::ExecutionControlState::Active, 1.0, 4));
        assert_eq!(snapshot.check_admission(context("p1", "POLICY", None)), AdmissionDecision::Admit);
    }

    #[test]
    fn global_tombstone_is_rejected_and_last_snapshot_stays_active() {
        let snapshot = ControlSnapshot::ready_for_test([(
            ControlKey::Global,
            value(common::ExecutionControlState::Paused, 0.0, 1),
        )]);
        assert!(snapshot.delete(&ControlKey::Global).is_err());
        assert!(snapshot.is_ready());
        assert!(matches!(snapshot.check_admission(context("p1", "POLICY", None)), AdmissionDecision::Hold(_)));
    }

    #[test]
    fn kafka_key_must_match_payload_and_tombstone_deletes_non_global_scope() {
        let record = events::ExecutionControlRecord {
            scope: common::ExecutionControlScope::Partner as i32,
            scope_id: "p1".into(),
            state: common::ExecutionControlState::Paused as i32,
            admission_rate: 0.0,
            dispatch_rate: 0.0,
            reason: String::new(),
            version: 1,
            expires_at: None,
            created_at: None,
        };
        let mut records = HashMap::new();
        assert!(apply_payload(&mut records, Some(b"EXECUTION_CONTROL_SCOPE_STAGE:BILLING"), Some(&record.encode_to_vec())).is_err());
        apply_payload(&mut records, Some(b"EXECUTION_CONTROL_SCOPE_PARTNER:p1"), Some(&record.encode_to_vec())).unwrap();
        assert!(records.contains_key(&ControlKey::Partner("p1".into())));
        apply_payload(&mut records, Some(b"EXECUTION_CONTROL_SCOPE_PARTNER:p1"), None).unwrap();
        assert!(!records.contains_key(&ControlKey::Partner("p1".into())));
    }

    #[tokio::test]
    async fn wait_until_ready_observes_install_without_lost_wakeup() {
        let snapshot = Arc::new(ControlSnapshot::default());
        let waiter = tokio::spawn({
            let snapshot = snapshot.clone();
            async move { snapshot.wait_until_ready().await }
        });
        snapshot.install_bootstrap(HashMap::from([active_global()])).unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    }
}
