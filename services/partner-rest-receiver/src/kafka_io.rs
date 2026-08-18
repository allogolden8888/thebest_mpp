//! `publish_incoming` (service_internal_methods.md §1.1) — реальный
//! `rdkafka` producer, не интеграционно проверенный в этом окружении (тот же
//! паттерн, что у остальных сервисов этого среза — см. README).

use crate::proto::events::IncomingMessage;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub const OUTPUT_TOPIC: &str = "incoming.messages";

pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        // NEXT_STEPS_1500TPS.md 1.1 — тот же линг, что у остальных Rust-
        // сервисов; здесь особенно уместно смотреть на latency-бюджет, т.к.
        // это первый хоп (партнёр ждёт ACK) — 5мс далеко в пределах бюджета.
        .set("linger.ms", "5")
        .create()
        .expect("не удалось создать Kafka producer")
}

#[derive(Debug)]
pub enum PublishError {
    Kafka(String),
}

/// ACK партнёру строго после подтверждения Kafka (`service_io_contracts.md`
/// §1.1: "ACK после подтверждённой публикации в Kafka") — HTTP-обработчик в
/// `http.rs` не отвечает 200/202 до успешного возврата из этой функции.
pub async fn publish_incoming(producer: &FutureProducer, message: &IncomingMessage) -> Result<(), PublishError> {
    let bytes = message.encode_to_vec();
    let record = FutureRecord::to(OUTPUT_TOPIC).key(&message.message_id).payload(&bytes);
    producer.send(record, Duration::from_secs(5)).await.map_err(|(e, _)| PublishError::Kafka(e.to_string()))?;
    Ok(())
}
