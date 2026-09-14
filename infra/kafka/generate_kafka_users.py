"""
Strimzi KafkaUser CR — реальные per-service Kafka ACL, а не только SASL
credentials без enforcement за ними.

BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя HLD
требует ACL": `generate_kafka_topics.py::build_kafka_cluster_crd` уже
добавляет SASL_SCRAM+TLS листенер (порт 9094) и включает
`authorization.type: simple` на кластере. Без KafkaUser с явными ACL этот
листенер был бы аутентификацией без авторизации — принципал бы проходил
SCRAM handshake, но получал бы ровно те же права, что anonymous plaintext-
клиент (никаких, кроме superuser-исключения для User:ANONYMOUS).

Источник "кому что можно" — НЕ отдельный руками ведённый список: это ровно
`k8s/generate_manifests.py::KEDA_KAFKA_TRIGGERS`, тот же explicit
`(topic, consumerGroup)` mapping, который уже используется для KEDA
ScaledObject-триггеров (BACKOFFICE_ROADMAP.md, "Что сделано в
инфраструктурном и data-plane проходе", п.5). Если завтра сервис начнёт
читать новый топик/группу, этот же PR, что чинит KEDA-триггер, автоматически
чинит и его ACL — не два независимых места, которые могут разойтись.

Явная неполнота (see BACKOFFICE_ROADMAP.md P1 за точным списком): это ТОЛЬКО
consumer-side ACL (Read на топик + Read на consumer group). Producer-side
ACL (Write) сюда не входят — ни один из сервисов в KEDA_KAFKA_TRIGGERS не
получает Write-разрешение ни на один топик этим генератором. Из трёх
сервисов, которым реально выданы SASL-креды сегодня
(`k8s/generate_manifests.py::KAFKA_SASL_DEMO_SERVICES`), все три — чистые
consumer'ы без producer-стороны, поэтому это не блокирует пилот; но это
значит, что KafkaUser для сервисов, которые ТОЖЕ продюсят (например,
dlr-manager: consumer operator.dlr/operator.dlr.unresolved, producer
delivery.status/scheduler.background.commands/operator.dlr.dlq), сегодня
получают ACL только на потребление — миграция producer-стороны на SASL для
такого сервиса потребует отдельного расширения ACL, не сделанного здесь.
"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "k8s"))
from generate_manifests import KEDA_KAFKA_TRIGGERS, NAMESPACE, kafka_user_name  # noqa: E402

STRIMZI_CLUSTER_LABEL = "mpp-kafka"


def build_kafka_user_crd(service_name: str, triggers: list[tuple[str, str]]) -> dict:
    acls = []
    seen_topics: set[str] = set()
    seen_groups: set[str] = set()
    for topic, consumer_group in triggers:
        if topic not in seen_topics:
            acls.append({
                "resource": {"type": "topic", "name": topic, "patternType": "literal"},
                "operations": ["Read"],
                "host": "*",
            })
            seen_topics.add(topic)
        if consumer_group not in seen_groups:
            acls.append({
                "resource": {"type": "group", "name": consumer_group, "patternType": "literal"},
                "operations": ["Read"],
                "host": "*",
            })
            seen_groups.add(consumer_group)

    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaUser",
        "metadata": {
            "name": kafka_user_name(service_name),
            "namespace": NAMESPACE,
            "labels": {"strimzi.io/cluster": STRIMZI_CLUSTER_LABEL},
        },
        "spec": {
            "authentication": {"type": "scram-sha-512"},
            "authorization": {
                "type": "simple",
                "acls": acls,
            },
        },
    }


def main():
    out_dir = Path(__file__).parent / "rendered"
    out_dir.mkdir(exist_ok=True)

    docs = [
        build_kafka_user_crd(service_name, triggers)
        for service_name, triggers in sorted(KEDA_KAFKA_TRIGGERS.items())
    ]

    path = out_dir / "kafka-users.yaml"
    path.write_text(yaml.dump_all(docs, sort_keys=False))

    print(f"{len(docs)} KafkaUser -> {path}")
    for service_name, triggers in sorted(KEDA_KAFKA_TRIGGERS.items()):
        topics = sorted({t for t, _ in triggers})
        groups = sorted({g for _, g in triggers})
        print(f"  {kafka_user_name(service_name):48s} topics={topics} groups={groups}")


if __name__ == "__main__":
    main()
