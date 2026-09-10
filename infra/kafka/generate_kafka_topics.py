"""
Kafka-топики — партиции/repl.factor/retention, ни разу не зафиксированные
конкретно ни в одном документе (platform_contracts.md перечисляет топики и
схемы, не количество партиций). Источник партиций — не произвольное число, а
реальное число реплик потребителя из k8s/generate_manifests.py SERVICES (тот
же источник истины, что и k8s-манифесты — если завтра capacity_model.md
изменит vCPU для Policy Service, партиции пересчитаются тем же прогоном).

Формула: partitions(topic) = ceil(max(replicas_for(consumer)) * 1.5) —
headroom, не точное совпадение, потому что Kafka-партиции можно только
УВЕЛИЧИВАТЬ, никогда не уменьшать; 1.5x позволяет вырасти потребителю без
болезненного repartitioning.

Категории (см. TopicCategory) — не все топики подчиняются этой формуле:
  WORKLOAD  — формула выше, один load-balancing consumer group на партицию.
  CONTROL   — низкий объём (config/manual override), фиксированное число
              партиций, часто compacted (текущее состояние по ключу, не поток).
  DLQ       — низкий ожидаемый объём по определению, фиксированное число,
              долгий retention для ручного разбора.
  CHANGELOG — Kafka Streams state-store changelog, партиции ОБЯЗАНЫ совпадать
              с партициями входного топика приложения (co-partitioning) — не
              независимое число, вычисляется от соответствующего workload-топика.

CO_PARTITIONED — ещё один Kafka Streams-специфичный инвариант: если одно
приложение (Message State Resolver) стримит из двух топиков и джойнит их
(stage.completed + delivery.status), у обоих топиков ОБЯЗАНО быть одинаковое
число партиций, иначе join невозможен. Здесь это не забыто — стоит явной
группой, после расчёта естественное (меньшее) число partitions у delivery.status
принудительно поднимается до числа у stage.completed.

Профили (--profile):
  production (по умолчанию) — как описано выше, партиции считаются от реальных
              production-реплик потребителей (REPLICAS из k8s/generate_manifests.py),
              RF=3, min.insync.replicas=2, 5 брокеров. Запуск без флагов даёт
              байт-в-байт тот же результат, что и раньше — поведение по умолчанию
              не изменилось.
  small       — компактная раскладка для одного dev/staging-узла на слабом железе.
              RF=1, min.insync.replicas=1, 1 брокер (не формула +2), партиции
              WORKLOAD-топиков считаются от MIN_REPLICAS (HA floor из
              k8s/generate_manifests.py = 2), а не от реальных production-реплик,
              CONTROL_PARTITIONS/DLQ_PARTITIONS пропорционально уменьшены.
              Пишет в отдельную директорию rendered-small/ — rendered/ (production,
              источник истины) при этом не трогается.
              ВНИМАНИЕ: RF=1 и 1 брокер означают ПОЛНОЕ отсутствие replica-level
              отказоустойчивости — потеря единственного брокера теряет и данные.
              Только для dev/staging, никогда не для production.
"""

import argparse
import math
import sys
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "k8s"))
from generate_manifests import SERVICES, replicas_for, MIN_REPLICAS  # noqa: E402

REPLICAS = {s.name: replicas_for(s) for s in SERVICES}
HEADROOM = 1.5

# Профиль-зависимые константы — назначаются в main() по --profile ДО вызова любой
# функции, которая их читает. Значения ниже — production-дефолты (совпадают с
# поведением до появления --profile, чтобы запуск без флагов был байт-в-байт тем же).
REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = 2  # acks=all + at-least-once (архитектурный принцип с самого HLD) требует >1 ISR
CLUSTER_BROKER_REPLICAS = REPLICATION_FACTOR + 2  # 5 брокеров — RF=3 переживает потерю 1 зоны из 3 с запасом
USE_MIN_REPLICAS_FLOOR = False  # small: WORKLOAD-партиции от MIN_REPLICAS вместо реальных REPLICAS[consumer]


class TopicCategory(Enum):
    WORKLOAD = auto()
    CONTROL = auto()
    DLQ = auto()
    CHANGELOG = auto()


@dataclass
class Topic:
    name: str
    category: TopicCategory
    consumers: list[str] = field(default_factory=list)
    retention_ms: int | None = None  # None => cleanup.policy=compact (CONTROL) либо CHANGELOG (Kafka Streams сам управляет)
    changelog_of: str | None = None  # для CHANGELOG — имя workload-топика, чьи партиции наследуются
    note: str = ""


HOUR = 3_600_000

TOPICS = [
    Topic("incoming.messages", TopicCategory.WORKLOAD, ["pipeline-engine"], 6 * HOUR),
    Topic("stage.destination-resolution", TopicCategory.WORKLOAD, ["destination-resolution-service"], 6 * HOUR),
    Topic("stage.policy", TopicCategory.WORKLOAD, ["policy-service"], 6 * HOUR),
    Topic("stage.billing", TopicCategory.WORKLOAD, ["billing-service"], 6 * HOUR),
    Topic("stage.routing", TopicCategory.WORKLOAD, ["routing-service"], 6 * HOUR),
    Topic("stage.delivery", TopicCategory.WORKLOAD, ["delivery-service"], 6 * HOUR),
    Topic("stage.delivery-reconciliation", TopicCategory.WORKLOAD, ["delivery-reconciliation-service"], 6 * HOUR),
    Topic("stage.completed", TopicCategory.WORKLOAD, ["pipeline-engine", "message-state-resolver"], 6 * HOUR),
    Topic("delivery.status", TopicCategory.WORKLOAD, ["message-state-resolver"], 6 * HOUR,
          note="co-partitioned со stage.completed — оба входа Message State Resolver, см. CO_PARTITIONED"),
    Topic("message.lifecycle", TopicCategory.WORKLOAD, ["lifecycle-writer", "partner-notification-service"], 24 * HOUR),
    Topic("operator.submit.accepted", TopicCategory.WORKLOAD, ["dlr-correlation-writer"], 24 * HOUR),
    Topic("operator.dlr", TopicCategory.WORKLOAD, ["dlr-manager"], 24 * HOUR),
    Topic("operator.dlr.unresolved", TopicCategory.WORKLOAD, ["dlr-manager"], 24 * HOUR),
    Topic("operator.pdu.log", TopicCategory.WORKLOAD, ["pdu-log-writer"], 24 * HOUR,
          note="BACKOFFICE_DESIGN_SPEC.md Экраны 38-40 — per-PDU диагностический след "
               "(submit_sm/submit_sm_resp/deliver_sm/deliver_sm_resp), публикует "
               "operator-smpp-session-manager; на порядок выше объём, чем operator.submit.accepted/"
               "operator.dlr (по PDU, не по сообщению/DLR), но та же 24ч retention — чисто "
               "диагностические данные, не бизнес-корреляция"),
    Topic("notification.retry", TopicCategory.WORKLOAD, ["partner-notification-service"], 24 * HOUR),
    Topic("billing.ledger", TopicCategory.WORKLOAD, ["billing-ledger-writer"], 24 * HOUR),
    Topic("scheduler.standard.commands", TopicCategory.WORKLOAD, ["scheduler-standard-lane"], 24 * HOUR),
    Topic("scheduler.background.commands", TopicCategory.WORKLOAD, ["scheduler-background-lane"], 24 * HOUR),
    Topic("pipeline.retry.triggers", TopicCategory.WORKLOAD, ["pipeline-engine"], 24 * HOUR,
          note="dynamic-seeking-russell.md — Scheduler Background Lane перепубликует сюда due STAGE_RETRY задачи, Pipeline Engine потребляет"),

    Topic("config.changes", TopicCategory.CONTROL, note="compacted — текущее состояние конфига по ключу entity_id, не поток событий"),
    Topic("execution.control", TopicCategory.CONTROL, note="compacted — текущее состояние scope, не поток"),
    Topic("scheduler.critical.commands", TopicCategory.CONTROL, retention_ms=24 * HOUR,
          note="только редкие ручные force-timeout/retry команды (hld.md) — обычный critical-lane трафик идёт через Redis, не сюда"),

    Topic("stage.destination-resolution.dlq", TopicCategory.DLQ, ["scheduler-background-lane"], 7 * 24 * HOUR),
    Topic("stage.policy.dlq", TopicCategory.DLQ, ["scheduler-background-lane"], 7 * 24 * HOUR),
    Topic("stage.billing.dlq", TopicCategory.DLQ, ["scheduler-background-lane"], 7 * 24 * HOUR),
    Topic("stage.routing.dlq", TopicCategory.DLQ, ["scheduler-background-lane"], 7 * 24 * HOUR),
    Topic("stage.delivery.dlq", TopicCategory.DLQ, ["scheduler-background-lane"], 7 * 24 * HOUR),
    Topic("operator.dlr.dlq", TopicCategory.DLQ, ["dlr-manager"], 7 * 24 * HOUR),
    Topic("notification.archived", TopicCategory.DLQ, ["partner-notification-service"], 7 * 24 * HOUR,
          note="реальная находка нагрузочного теста: раньше единственным пределом ретрая была NotificationTTL "
               "(24ч) — недостижимый partner-webhook держал сообщения в вечном retry-цикле, засоряя "
               "notification.retry устойчивой фоновой нагрузкой. MaxAttempts (kafkaio.Deps) архивирует сюда "
               "вместо повторной публикации в notification.retry после исчерпания попыток/TTL — читателя пока "
               "нет, топик для ручного разбора"),

    Topic("message-state.changelog", TopicCategory.CHANGELOG, changelog_of="stage.completed"),
    Topic("scheduler.standard.state.changelog", TopicCategory.CHANGELOG, changelog_of="scheduler.standard.commands"),
    Topic("scheduler.background.state.changelog", TopicCategory.CHANGELOG, changelog_of="scheduler.background.commands"),
]

CO_PARTITIONED_GROUPS = [
    ["stage.completed", "delivery.status"],  # оба — вход Message State Resolver, join требует равных партиций
]

CONTROL_PARTITIONS = 6
DLQ_PARTITIONS = 3


def compute_partitions() -> dict[str, int]:
    partitions: dict[str, int] = {}

    for t in TOPICS:
        if t.category == TopicCategory.WORKLOAD:
            if USE_MIN_REPLICAS_FLOOR:
                # small-профиль: считаем от HA floor (MIN_REPLICAS), не от
                # реальных production-реплик потребителя — компактная
                # раскладка для одного dev/staging-узла, не масштаб под
                # 20 000 msg/s.
                max_replicas = MIN_REPLICAS
            else:
                max_replicas = max(REPLICAS[c] for c in t.consumers)
            partitions[t.name] = math.ceil(max_replicas * HEADROOM)
        elif t.category == TopicCategory.CONTROL:
            partitions[t.name] = CONTROL_PARTITIONS
        elif t.category == TopicCategory.DLQ:
            partitions[t.name] = DLQ_PARTITIONS

    for group in CO_PARTITIONED_GROUPS:
        shared = max(partitions[name] for name in group)
        for name in group:
            partitions[name] = shared

    for t in TOPICS:
        if t.category == TopicCategory.CHANGELOG:
            partitions[t.name] = partitions[t.changelog_of]

    return partitions


def build_kafka_topic_crd(t: Topic, partition_count: int) -> dict:
    config = {"min.insync.replicas": str(MIN_INSYNC_REPLICAS)}
    if t.category == TopicCategory.CONTROL and t.retention_ms is None:
        config["cleanup.policy"] = "compact"
    else:
        config["cleanup.policy"] = "delete"
        config["retention.ms"] = str(t.retention_ms)

    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaTopic",
        "metadata": {
            "name": t.name.replace(".", "-"),  # Strimzi требует DNS-совместимое metadata.name; реальное имя топика — topicName ниже
            "namespace": "mpp",
            "labels": {"strimzi.io/cluster": "mpp-kafka"},
        },
        "spec": {
            "topicName": t.name,
            "partitions": partition_count,
            "replicas": REPLICATION_FACTOR,
            "config": config,
        },
    }


def build_kafka_cluster_crd() -> dict:
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "Kafka",
        "metadata": {"name": "mpp-kafka", "namespace": "mpp"},
        "spec": {
            "kafka": {
                "version": "3.9.0",
                "replicas": CLUSTER_BROKER_REPLICAS,
                "listeners": [
                    {"name": "plain", "port": 9092, "type": "internal", "tls": False},
                    {"name": "tls", "port": 9093, "type": "internal", "tls": True},
                ],
                "config": {
                    "default.replication.factor": REPLICATION_FACTOR,
                    "min.insync.replicas": MIN_INSYNC_REPLICAS,
                    "offsets.topic.replication.factor": REPLICATION_FACTOR,
                    "transaction.state.log.replication.factor": REPLICATION_FACTOR,
                    "transaction.state.log.min.isr": MIN_INSYNC_REPLICAS,  # Pipeline Engine/Message State Resolver
                                                                             # используют Kafka-транзакции (hld.md §10.1) —
                                                                             # exactly-once требует этого явно
                },
                "storage": {
                    "type": "persistent-claim",
                    "size": "500Gi",
                    "class": "network-ssd",
                },
            },
            "entityOperator": {"topicOperator": {}, "userOperator": {}},
        },
    }


def main():
    global REPLICATION_FACTOR, MIN_INSYNC_REPLICAS, CLUSTER_BROKER_REPLICAS, USE_MIN_REPLICAS_FLOOR
    global CONTROL_PARTITIONS, DLQ_PARTITIONS

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--profile", choices=["production", "small"], default="production",
        help="production (default, без изменений) | small (компактно для одного dev/staging-узла, см. docstring модуля)",
    )
    args = parser.parse_args()

    if args.profile == "small":
        # Только для dev/staging — см. предупреждение в docstring модуля и
        # SMALL_DEPLOYMENT_RUNBOOK.md. Никогда не использовать в production:
        # RF=1 + 1 брокер = полное отсутствие replica-level отказоустойчивости.
        REPLICATION_FACTOR = 1
        MIN_INSYNC_REPLICAS = 1
        CLUSTER_BROKER_REPLICAS = 1
        USE_MIN_REPLICAS_FLOOR = True
        CONTROL_PARTITIONS = 2
        DLQ_PARTITIONS = 1
        out_dir = Path(__file__).parent / "rendered-small"
    else:
        out_dir = Path(__file__).parent / "rendered"

    out_dir.mkdir(exist_ok=True)
    for f in out_dir.glob("*.yaml"):
        f.unlink()

    partitions = compute_partitions()

    docs = [build_kafka_cluster_crd()]
    for t in TOPICS:
        docs.append(build_kafka_topic_crd(t, partitions[t.name]))

    path = out_dir / "kafka-topics.yaml"
    path.write_text(yaml.dump_all(docs, sort_keys=False))

    print(f"profile={args.profile}  RF={REPLICATION_FACTOR}  brokers={CLUSTER_BROKER_REPLICAS}")
    print(f"{len(TOPICS)} топиков + 1 Kafka CR -> {path}")
    for t in TOPICS:
        print(f"  {t.name:42s} {t.category.name:10s} partitions={partitions[t.name]}")


if __name__ == "__main__":
    main()
