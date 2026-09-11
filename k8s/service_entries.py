"""
Istio ServiceEntry для managed-зависимостей вне mesh (Yandex Managed
PostgreSQL/Redis x3/ClickHouse) — BACKOFFICE_ROADMAP.md, P1 "Внешний egress
при default-deny".

Почему ServiceEntry, не что-то ещё: кластер уже ставит Istio control plane
(infra/terraform/istio.tf — istio-base + istiod) и использует его для mTLS
(PeerAuthentication STRICT, k8s/README.md). Default-deny NetworkPolicy
закрывает egress на L3/L4 (network_policies.py), но ничего не говорит mesh'у
о том, что managed PostgreSQL/Redis/ClickHouse — легитимные внешние
destinations — это и есть роль ServiceEntry (`location: MESH_EXTERNAL`),
стандартный Istio-механизм для ровно этой задачи, а не изобретённый здесь
велосипед. NetworkPolicy остаётся фактической точкой принуждения (Istio
sidecar не подменяет исходный/целевой IP пакета для plain-TCP destinations
вроде PostgreSQL/Redis/ClickHouse, поэтому ipBlock в NetworkPolicy видит
реальный адрес управляемой БД) — ServiceEntry даёт mesh-уровню знание об этих
хостах для последующих возможностей (DestinationRule/mTLS origination
policies, телеметрия, будущий `Sidecar` c `outboundTrafficPolicy:
REGISTRY_ONLY` — см. BACKOFFICE_ROADMAP.md, этот шаг его сознательно не
включает: REGISTRY_ONLY меняет поведение egress для ВСЕХ подов namespace
разом, а без живого кластера это нельзя безопасно проверить на все 32
сервиса — отдельная, отдельно тестируемая задача).

Хосты те же самые, что и network_policies.py читает из k8s/external_hosts.py
(единственный источник — Terraform, см. комментарий там); дублирования
константы FQDN нет.
"""

from pathlib import Path

import yaml

from external_hosts import load_external_hosts

NAMESPACE = "mpp"

# db-key (k8s/external_hosts.py) -> имя ServiceEntry.
DB_SERVICE_ENTRY_NAMES = {
    "postgresql": "mpp-managed-postgresql",
    "redis-runtime": "mpp-managed-redis-runtime",
    "redis-configuration": "mpp-managed-redis-configuration",
    "redis-billing": "mpp-managed-redis-billing",
    "clickhouse": "mpp-managed-clickhouse",
}


def _service_entry(name: str, host_entry: dict) -> dict:
    port_number = host_entry["port"]
    return {
        # v1, не v1beta1: ServiceEntry GA с Istio 1.20, кластер здесь на 1.24.1
        # (infra/terraform/istio.tf) — тот же уровень API, что уже выбран для
        # DestinationRule в infra/istio/peer-authentication-strict.yaml
        # (networking.istio.io/v1), не смешиваем версии одной API-группы без
        # причины.
        "apiVersion": "networking.istio.io/v1",
        "kind": "ServiceEntry",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "hosts": [host_entry["fqdn"]],
            "location": "MESH_EXTERNAL",
            "ports": [{
                "number": port_number,
                "name": f"tcp-{port_number}",
                "protocol": "TCP",
            }],
            "resolution": "DNS",
        },
    }


def generate_all(hosts: dict | None = None) -> list[dict]:
    hosts = hosts if hosts is not None else load_external_hosts()
    docs = []
    for db_key, entry_name in DB_SERVICE_ENTRY_NAMES.items():
        assert db_key in hosts, f"external-hosts отсутствует обязательный ключ: {db_key}"
        docs.append(_service_entry(entry_name, hosts[db_key]))
    return docs


def main():
    out_dir = Path(__file__).parent / "rendered"
    out_dir.mkdir(exist_ok=True)
    docs = generate_all()
    path = out_dir / "02-service-entries.yaml"
    path.write_text(yaml.dump_all(docs, sort_keys=False))
    print(f"{len(docs)} ServiceEntry -> {path}")


if __name__ == "__main__":
    main()
