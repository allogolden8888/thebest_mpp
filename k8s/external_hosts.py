"""
Загрузчик FQDN/IP managed-зависимостей вне mesh (Yandex Managed
PostgreSQL/Redis x3/ClickHouse) для network_policies.py/service_entries.py.

Почему отдельный модуль, а не константа в generate_manifests.py (как
IMAGE_REGISTRY/EXTERNAL_DOMAIN/KAFKA_BOOTSTRAP_SERVERS): те три — либо
свободно выбираемый плейсхолдер (IMAGE_REGISTRY/EXTERNAL_DOMAIN, реальный
домен/registry ещё не куплен), либо детерминированное имя, полностью
предсказуемое по конвенции оператора (KAFKA_BOOTSTRAP_SERVERS — Strimzi
всегда строит Service как <Kafka.metadata.name>-kafka-bootstrap). FQDN
managed PostgreSQL/Redis/ClickHouse в Yandex Cloud — НЕ такие: провайдер
генерирует их (например `rc1a-<случайный-id>.mdb.yandexcloud.net`), значение
существует только после реального `terraform apply` и не угадывается. Тот же
факт подтверждён схемой провайдера: `yandex_mdb_postgresql_cluster`/
`yandex_mdb_redis_cluster` host-блоки несут только `fqdn` (нет `ip_address`),
поэтому даже сам Terraform не знает IP статически — для NetworkPolicy ipBlock
(который матчит по IP, не по имени) нужен реальный DNS-резолв ПОСЛЕ apply,
из сети, где эти внутренние VPC-имена вообще резолвятся (см.
infra/terraform/export_external_hosts.sh).

Источник истины по приоритету:
1. k8s/external-hosts.json — реальный, per-environment, НЕ коммитится
   (.gitignore) — производится export_external_hosts.sh после
   `terraform apply` для конкретного окружения.
2. k8s/external-hosts.example.json — committed placeholder (тот же приём,
   что EXTERNAL_DOMAIN = "mpp.example" в generate_manifests.py: RFC 5737
   TEST-NET IP-адреса и `.example`-FQDN, заведомо нерабочие, но валидный
   YAML/JSON для тестов, kubeconform и локальной сборки без живого
   кластера).
"""

import json
import sys
from pathlib import Path

_DIR = Path(__file__).parent
REAL_PATH = _DIR / "external-hosts.json"
FIXTURE_PATH = _DIR / "external-hosts.example.json"

REQUIRED_KEYS = ("postgresql", "redis-runtime", "redis-configuration", "redis-billing", "clickhouse")


def load_external_hosts() -> dict:
    if REAL_PATH.exists():
        path = REAL_PATH
    else:
        print(
            f"WARNING: {REAL_PATH} не найден — используется placeholder-фикстура {FIXTURE_PATH} "
            "(RFC 5737 TEST-NET IP / .example FQDN, НЕ настоящие адреса). Перед реальным деплоем "
            "прогнать infra/terraform/export_external_hosts.sh против применённого окружения.",
            file=sys.stderr,
        )
        path = FIXTURE_PATH

    data = json.loads(path.read_text())
    missing = [key for key in REQUIRED_KEYS if key not in data]
    if missing:
        raise ValueError(f"{path}: отсутствуют обязательные ключи: {missing}")
    for key in REQUIRED_KEYS:
        entry = data[key]
        for field in ("fqdn", "port", "ips"):
            if field not in entry:
                raise ValueError(f"{path}: {key} не несёт обязательное поле {field!r}")
        if not entry["ips"]:
            raise ValueError(
                f"{path}: {key}.ips пуст — DNS-резолв не дал адресов (см. export_external_hosts.sh)"
            )
    return data
