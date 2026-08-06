#!/bin/bash
# Создаёт все 29 топиков платформы в локальном docker-compose Kafka с теми
# же partitions/cleanup.policy/retention.ms, что реально сгенерированы для
# продакшена (infra/kafka/rendered/kafka-topics.yaml,
# infra/kafka/generate_kafka_topics.py) — replication factor=1 (локально
# один брокер), остальное 1:1 с продом, не дефолты auto-create.
#
# Запуск: bash infra/docker/create_topics.sh
set -euo pipefail

KAFKA_CONTAINER="docker-kafka-1"
BOOTSTRAP="localhost:9092"

create_topic() {
  local name="$1" partitions="$2" cleanup="$3" retention="$4"
  # kafka-topics.sh хочет отдельный --config на каждую пару key=val, не
  # запятую внутри одного аргумента (реальная находка — comma-form падал с
  # "all configs to be added must be in the format key=val").
  local configs=(--config "cleanup.policy=${cleanup}")
  if [ -n "$retention" ]; then
    configs+=(--config "retention.ms=${retention}")
  fi
  echo "=== ${name} (partitions=${partitions}, cleanup.policy=${cleanup}, retention.ms=${retention:-<default>}) ==="
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    "${configs[@]}"
}

create_topic incoming.messages 26 delete 21600000
create_topic stage.destination-resolution 3 delete 21600000
create_topic stage.policy 5 delete 21600000
create_topic stage.billing 5 delete 21600000
create_topic stage.routing 3 delete 21600000
create_topic stage.delivery 5 delete 21600000
create_topic stage.delivery-reconciliation 3 delete 21600000
create_topic stage.completed 26 delete 21600000
create_topic delivery.status 26 delete 21600000
create_topic message.lifecycle 18 delete 86400000
create_topic operator.submit.accepted 3 delete 86400000
create_topic operator.dlr 9 delete 86400000
create_topic operator.dlr.unresolved 9 delete 86400000
create_topic notification.retry 18 delete 86400000
create_topic billing.ledger 3 delete 86400000
create_topic scheduler.standard.commands 3 delete 86400000
create_topic scheduler.background.commands 3 delete 86400000
create_topic config.changes 6 compact ""
create_topic execution.control 6 compact ""
create_topic scheduler.critical.commands 6 delete 86400000
create_topic stage.destination-resolution.dlq 3 delete 604800000
create_topic stage.policy.dlq 3 delete 604800000
create_topic stage.billing.dlq 3 delete 604800000
create_topic stage.routing.dlq 3 delete 604800000
create_topic stage.delivery.dlq 3 delete 604800000
create_topic operator.dlr.dlq 3 delete 604800000
create_topic message-state.changelog 26 delete ""
create_topic scheduler.standard.state.changelog 3 delete ""
create_topic scheduler.background.state.changelog 3 delete ""

echo
echo "=== Итоговый список топиков ==="
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
