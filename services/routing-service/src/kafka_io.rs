//! Kafka I/O — потребляет `stage.routing`, публикует `stage.completed`.
//! Тот же паттерн, что у остальных сервисов этого среза: `handle_command` —
//! чистая функция (тестируется без сети), `run_loop` — реальная Kafka-обвязка,
//! не интеграционно проверенная в этом окружении.

use crate::proto::stage_completed_event::StageResult;
use crate::proto::stage_execute_command::StageExtension;
use crate::proto::{Outcome, Protocol, RoutingResult, StageCompletedEvent, StageExecuteCommand};
use crate::route_table::RouteTableSnapshot;
use crate::routing::{ControlSnapshot, RoutingError, resolve_final_route};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub const INPUT_TOPIC: &str = "stage.routing";
pub const OUTPUT_TOPIC: &str = "stage.completed";

pub fn build_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()
        .expect("не удалось создать Kafka consumer")
}

pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("не удалось создать Kafka producer")
}

fn protocol_to_proto(protocol: &str) -> Protocol {
    match protocol {
        "SMPP" => Protocol::Smpp,
        "HTTP" => Protocol::Http,
        other => panic!("неизвестный protocol в routing_table: {other}"),
    }
}

pub fn handle_command(
    command: &StageExecuteCommand,
    snapshot: &RouteTableSnapshot,
    control: &ControlSnapshot,
) -> StageCompletedEvent {
    let resolved_operator_id = match &command.stage_extension {
        Some(StageExtension::Routing(ext)) => ext.resolved_operator_id.clone(),
        _ => return build_event(command, Outcome::Rejected, "MISSING_ROUTING_EXTENSION", None),
    };

    match resolve_final_route(snapshot, control, &resolved_operator_id) {
        Ok(route) => {
            let result = RoutingResult {
                route_id: route.route_id,
                protocol: protocol_to_proto(&route.protocol) as i32,
                route_version: route.route_version,
            };
            build_event(command, Outcome::Succeeded, "", Some(result))
        }
        // NO_HEALTHY_ROUTE ретраябельно — маршруты могут восстановиться (DEGRADED->ACTIVE,
        // PAUSED снят вручную); UNKNOWN_OPERATOR — нет, это конфигурационная ошибка.
        Err(RoutingError::NoHealthyRoute) => build_event(command, Outcome::Rejected, "NO_HEALTHY_ROUTE", None),
        Err(RoutingError::UnknownOperator) => build_event(command, Outcome::Rejected, "UNKNOWN_OPERATOR", None),
    }
}

fn build_event(command: &StageExecuteCommand, outcome: Outcome, reason_code: &str, result: Option<RoutingResult>) -> StageCompletedEvent {
    StageCompletedEvent {
        event_id: format!("evt-{}", command.stage_execution_id),
        message_id: command.message_id.clone(),
        stage_execution_id: command.stage_execution_id.clone(),
        attempt: command.attempt,
        stage_name: command.stage_name,
        outcome: outcome as i32,
        reason_code: reason_code.to_string(),
        retryable: matches!(outcome, Outcome::Rejected) && reason_code != "UNKNOWN_OPERATOR",
        retry_after: None,
        traceparent: command.traceparent.clone(),
        completed_at: None,
        stage_result: result.map(StageResult::Routing),
    }
}

pub async fn run_loop(consumer: StreamConsumer, producer: FutureProducer, snapshot: RouteTableSnapshot, control: ControlSnapshot) {
    consumer.subscribe(&[INPUT_TOPIC]).expect("не удалось подписаться на stage.routing");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else { continue };
                let command = match StageExecuteCommand::decode(payload) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("не удалось декодировать StageExecuteCommand: {e}");
                        continue;
                    }
                };

                let event = handle_command(&command, &snapshot, &control);
                let bytes = event.encode_to_vec();
                let record = FutureRecord::to(OUTPUT_TOPIC).key(&event.message_id).payload(&bytes);
                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    tracing::error!("не удалось опубликовать StageCompletedEvent: {e}");
                    continue;
                }
                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset: {e}");
                }
            }
            Err(e) => tracing::error!("ошибка Kafka consumer: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_table::RouteTable;

    fn snapshot() -> RouteTableSnapshot {
        let json = include_str!("../../../config_schemas/examples/routing_table.valid.json");
        let table: RouteTable = serde_json::from_str(json).unwrap();
        RouteTableSnapshot::from_tables(vec![table])
    }

    fn command(resolved_operator_id: &str) -> StageExecuteCommand {
        StageExecuteCommand {
            event_id: "e1".into(),
            message_id: "m1".into(),
            channel: 0,
            pipeline_id: "p1".into(),
            pipeline_version: "1".into(),
            node_id: "n1".into(),
            stage_name: 4, // STAGE_NAME_ROUTING
            stage_execution_id: "se1".into(),
            attempt: 1,
            deadline: None,
            message_ttl: None,
            config_versions: Default::default(),
            traceparent: "tp1".into(),
            payload_ref: None,
            stage_extension: Some(StageExtension::Routing(crate::proto::RoutingExtension {
                resolved_operator_id: resolved_operator_id.to_string(),
            })),
        }
    }

    #[test]
    fn healthy_operator_produces_succeeded_with_route() {
        let event = handle_command(&command("beeline_uz"), &snapshot(), &ControlSnapshot::new());
        assert_eq!(event.outcome, Outcome::Succeeded as i32);
        match event.stage_result {
            Some(StageResult::Routing(r)) => {
                assert_eq!(r.route_id, "beeline_smpp_primary");
                assert_eq!(r.protocol, Protocol::Smpp as i32);
                assert_eq!(r.route_version, "3");
            }
            other => panic!("ожидали RoutingResult, получили {other:?}"),
        }
    }

    #[test]
    fn unknown_operator_produces_non_retryable_rejection() {
        let event = handle_command(&command("no_such_operator"), &snapshot(), &ControlSnapshot::new());
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "UNKNOWN_OPERATOR");
        assert!(!event.retryable, "конфигурационная ошибка — повтор не поможет без изменения конфигурации");
    }

    #[test]
    fn all_routes_down_produces_retryable_rejection() {
        let mut control = ControlSnapshot::new();
        control.insert("beeline_smpp_primary".to_string(), crate::routing::ControlState::Paused);
        control.insert("beeline_http_reserve".to_string(), crate::routing::ControlState::Paused);
        let event = handle_command(&command("beeline_uz"), &snapshot(), &control);
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "NO_HEALTHY_ROUTE");
        assert!(event.retryable, "маршруты могут восстановиться — стоит повторить");
    }
}
