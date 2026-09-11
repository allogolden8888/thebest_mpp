//! Full-mirror consumer `config.changes` для PARTNER-конфигурации.
//!
//! Consumer group здесь намеренно не делит партиции: каждой REST-реплике
//! нужен полный локальный auth/rate/IP snapshot. На каждом старте все
//! партиции назначаются вручную с Beginning; live state становится ready
//! только после EOF каждой из них. После bootstrap последний подтверждённый
//! snapshot сохраняется при временной потере Kafka (fail-static).

use crate::partner_config::{Partner, PartnerSnapshot};
use crate::proto::{common, events};
use prost::Message as ProstMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub const CONFIG_CHANGES_TOPIC: &str = "config.changes";

#[derive(Debug, Clone)]
enum PartnerUpdate {
    Active { version: i64, partner: Partner },
    Archived { version: i64, partner_id: String },
    Ignore,
}

fn decode_update(key: Option<&[u8]>, payload: Option<&[u8]>) -> Result<PartnerUpdate, String> {
    let Some(payload) = payload else {
        // Config Event Publisher выражает archive полем event.status и не
        // публикует Kafka tombstone. Без entity_type в null payload нельзя
        // безопасно заключить, что tombstone относится именно к PARTNER.
        return Ok(PartnerUpdate::Ignore);
    };
    let event = events::ConfigChangeEvent::decode(payload)
        .map_err(|error| format!("не удалось декодировать ConfigChangeEvent: {error}"))?;
    let entity_type = common::ConfigEntityType::try_from(event.entity_type).map_err(|_| {
        format!(
            "config.changes содержит неизвестный entity_type={}",
            event.entity_type
        )
    })?;
    if entity_type != common::ConfigEntityType::Partner {
        return Ok(PartnerUpdate::Ignore);
    }
    if event.entity_id.is_empty() || event.version <= 0 {
        return Err("PARTNER ConfigChangeEvent требует entity_id и version > 0".to_string());
    }
    let key =
        std::str::from_utf8(key.ok_or_else(|| "PARTNER config event без Kafka key".to_string())?)
            .map_err(|error| format!("PARTNER config Kafka key не UTF-8: {error}"))?;
    if key != event.entity_id {
        return Err(format!(
            "PARTNER config Kafka key {key:?} не совпадает с entity_id {:?}",
            event.entity_id
        ));
    }

    match event.status.as_str() {
        "archived" => Ok(PartnerUpdate::Archived {
            version: event.version,
            partner_id: event.entity_id,
        }),
        "active" => {
            let partner: Partner = serde_json::from_slice(&event.payload_json)
                .map_err(|error| format!("невалидный PARTNER payload_json: {error}"))?;
            if partner.partner_id != event.entity_id {
                return Err(format!(
                    "PARTNER payload partner_id {:?} не совпадает с entity_id {:?}",
                    partner.partner_id, event.entity_id
                ));
            }
            Ok(PartnerUpdate::Active {
                version: event.version,
                partner,
            })
        }
        other => Err(format!(
            "PARTNER ConfigChangeEvent содержит неизвестный status={other:?}"
        )),
    }
}

fn apply_bootstrap(partners: &mut HashMap<String, (i64, Option<Partner>)>, update: PartnerUpdate) {
    match update {
        PartnerUpdate::Active { version, partner } => {
            let id = partner.partner_id.clone();
            if partners
                .get(&id)
                .is_none_or(|(current, _)| version > *current)
            {
                partners.insert(id, (version, Some(partner)));
            }
        }
        PartnerUpdate::Archived {
            version,
            partner_id,
        } => {
            if partners
                .get(&partner_id)
                .is_none_or(|(current, _)| version > *current)
            {
                partners.insert(partner_id, (version, None));
            }
        }
        PartnerUpdate::Ignore => {}
    }
}

fn apply_live(snapshot: &PartnerSnapshot, update: PartnerUpdate) {
    match update {
        PartnerUpdate::Active { version, partner } => snapshot.apply_active(version, partner),
        PartnerUpdate::Archived {
            version,
            partner_id,
        } => snapshot.apply_archived(version, &partner_id),
        PartnerUpdate::Ignore => {}
    }
}

async fn consume_session(brokers: &[String], snapshot: &PartnerSnapshot) -> Result<(), String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers.join(","))
        .set(
            "group.id",
            format!("partner-rest-config-{}", uuid::Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|error| format!("не удалось создать config.changes consumer: {error}"))?;

    let metadata = consumer
        .fetch_metadata(
            Some(CONFIG_CHANGES_TOPIC),
            Timeout::After(Duration::from_secs(10)),
        )
        .map_err(|error| format!("не удалось получить metadata {CONFIG_CHANGES_TOPIC}: {error}"))?;
    let topic = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == CONFIG_CHANGES_TOPIC)
        .ok_or_else(|| format!("Kafka metadata не содержит topic {CONFIG_CHANGES_TOPIC}"))?;
    if topic.partitions().is_empty() {
        return Err(format!("topic {CONFIG_CHANGES_TOPIC} не содержит партиций"));
    }

    let mut assignment = TopicPartitionList::new();
    let mut pending_eof = HashSet::new();
    for partition in topic.partitions() {
        assignment
            .add_partition_offset(CONFIG_CHANGES_TOPIC, partition.id(), Offset::Beginning)
            .map_err(|error| {
                format!(
                    "не удалось назначить config partition {}: {error}",
                    partition.id()
                )
            })?;
        pending_eof.insert(partition.id());
    }
    consumer
        .assign(&assignment)
        .map_err(|error| format!("config Kafka assign failed: {error}"))?;

    let mut bootstrap = HashMap::new();
    let mut live = false;
    loop {
        match consumer.recv().await {
            Ok(message) => match decode_update(message.key(), message.payload()) {
                Ok(update) if live => apply_live(snapshot, update),
                Ok(update) => apply_bootstrap(&mut bootstrap, update),
                Err(error) => {
                    // Poison config must not replace the last valid snapshot
                    // or pin all later versions in the same partition.
                    tracing::error!(partition = message.partition(), offset = message.offset(), %error,
                        "PARTNER config event отклонён; действует последняя валидная версия");
                }
            },
            Err(KafkaError::PartitionEOF(partition)) => {
                if !live {
                    pending_eof.remove(&partition);
                    if pending_eof.is_empty() {
                        snapshot.install_bootstrap(std::mem::take(&mut bootstrap));
                        live = true;
                        tracing::info!("initial PARTNER config snapshot полностью прочитан");
                    }
                }
            }
            Err(error) => return Err(format!("config.changes consumer error: {error}")),
        }
    }
}

pub async fn run_config_consumer(brokers: Vec<String>, snapshot: PartnerSnapshot) {
    loop {
        if let Err(error) = consume_session(&brokers, &snapshot).await {
            tracing::error!(%error, "PARTNER config consumer будет переподключён; действует последний snapshot");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn partner_json(status: &str) -> Vec<u8> {
        format!(
            r#"{{"partner_id":"acme","version":1,"status":"{status}","applications":[{{"application_id":"app","display_name":"App","auth":{{"type":"API_KEY","credential_ref":"vault://acme/key"}},"ip_allowlist":[],"rate_limit_tps":10,"allowed_channels":["SMS"]}}]}}"#
        )
        .into_bytes()
    }

    fn event(version: i64, status: &str, payload: Vec<u8>) -> Vec<u8> {
        events::ConfigChangeEvent {
            entity_type: common::ConfigEntityType::Partner as i32,
            entity_id: "acme".into(),
            version,
            payload_json: payload,
            status: status.into(),
            created_at: None,
        }
        .encode_to_vec()
    }

    #[test]
    fn active_partner_is_installed_and_archive_tombstone_blocks_stale_revival() {
        let snapshot = PartnerSnapshot::default();
        let mut bootstrap = HashMap::new();
        apply_bootstrap(
            &mut bootstrap,
            decode_update(
                Some(b"acme"),
                Some(&event(10, "active", partner_json("active"))),
            )
            .unwrap(),
        );
        apply_bootstrap(
            &mut bootstrap,
            decode_update(
                Some(b"acme"),
                Some(&event(11, "archived", partner_json("archived"))),
            )
            .unwrap(),
        );
        snapshot.install_bootstrap(bootstrap);
        assert!(snapshot.get("acme").is_none());

        apply_live(
            &snapshot,
            decode_update(
                Some(b"acme"),
                Some(&event(10, "active", partner_json("active"))),
            )
            .unwrap(),
        );
        assert!(snapshot.get("acme").is_none());
        apply_live(
            &snapshot,
            decode_update(
                Some(b"acme"),
                Some(&event(12, "active", partner_json("active"))),
            )
            .unwrap(),
        );
        assert!(snapshot.get("acme").is_some());
    }

    #[test]
    fn mismatched_key_or_payload_partner_is_rejected() {
        assert!(
            decode_update(
                Some(b"other"),
                Some(&event(1, "active", partner_json("active")))
            )
            .is_err()
        );
        let bad_payload = partner_json("active");
        let mut json: serde_json::Value = serde_json::from_slice(&bad_payload).unwrap();
        json["partner_id"] = "other".into();
        assert!(
            decode_update(
                Some(b"acme"),
                Some(&event(1, "active", serde_json::to_vec(&json).unwrap()))
            )
            .is_err()
        );
    }

    #[test]
    fn unrelated_entity_and_kafka_tombstone_are_ignored() {
        let unrelated = events::ConfigChangeEvent {
            entity_type: common::ConfigEntityType::Pipeline as i32,
            entity_id: "acme".into(),
            version: 1,
            payload_json: b"not partner json".to_vec(),
            status: "active".into(),
            created_at: None,
        }
        .encode_to_vec();
        assert!(matches!(
            decode_update(None, None).unwrap(),
            PartnerUpdate::Ignore
        ));
        assert!(matches!(
            decode_update(Some(b"acme"), Some(&unrelated)).unwrap(),
            PartnerUpdate::Ignore
        ));
    }
}
