"""
Валидатор config_versions.payload / ConfigChangeEvent.payload_json.

Запускает две проверки, реально исполняемые, не декларируемые:
1. Структурная — jsonschema (Draft 2020-12) против schema-файлов в этой директории.
2. Семантическая — инварианты, невыразимые чистым JSON Schema (арифметика
   диапазонов, ссылочная целостность внутри одного документа, два хардкод-правила
   графа пайплайна из hld.md §6). Реализованы как отдельные Python-функции ниже.

Каждый example-файл размечен ожидаемым результатом по суффиксу имени:
`*.valid.json` -> должен пройти обе проверки, `*.invalid_*.json` -> должен
быть отклонён хотя бы одной. Скрипт проверяет, что фактический результат
совпал с ожидаемым по имени файла, а не просто печатает "ошибка есть/нет".

Production-потребители читают entity_type из ConfigChangeEvent и валидируют
payload_json этой же схемой до применения в свой snapshot — схемы здесь есть
источник истины для этого шага, не для документации к нему.
"""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

BASE = Path(__file__).parent
EXAMPLES = BASE / "examples"

SCHEMA_FOR_PREFIX = {
    "pipeline": "pipeline.schema.json",
    "routing_table": "routing_table.schema.json",
    "operator": "operator.schema.json",
    "partner": "partner.schema.json",
    "policy_ruleset": "policy_ruleset.schema.json",
    "billing_tariff": "billing_tariff.schema.json",
    "number_range": "number_range.schema.json",
    "policy_template": "policy_template.schema.json",
    "subscriber_consent": "subscriber_consent.schema.json",
    "category": "category.schema.json",
    "ctn": "ctn.schema.json",
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def _schema_for(example_path: Path) -> dict:
    prefix = example_path.name.split(".")[0]
    return _load(BASE / SCHEMA_FOR_PREFIX[prefix])


def structural_errors(instance: dict, schema: dict) -> list[str]:
    validator = Draft202012Validator(schema)
    return [f"{'/'.join(str(p) for p in e.path)}: {e.message}" for e in validator.iter_errors(instance)]


# ---------------------------------------------------------------------------
# Семантические проверки — то, что JSON Schema не умеет.
# ---------------------------------------------------------------------------

def validate_pipeline_graph(doc: dict) -> list[str]:
    errors = []
    nodes_by_id = {n["node_id"]: n for n in doc["nodes"]}

    entry = nodes_by_id.get(doc["entry_node_id"])
    if entry is None:
        errors.append(f"entry_node_id={doc['entry_node_id']!r} не найден среди nodes")
    elif entry["stage_name"] != "DESTINATION_RESOLUTION":
        errors.append(
            f"Правило hld.md §6: entry_node_id обязан указывать на узел DESTINATION_RESOLUTION, "
            f"а не {entry['stage_name']} (Policy не может выполниться раньше, чем известен оператор)"
        )

    for node in doc["nodes"]:
        if node["stage_name"] != "POLICY":
            continue
        rejected_target_id = node["next"].get("REJECTED")
        if rejected_target_id is None:
            errors.append(
                f"Правило hld.md §5.3.1: узел POLICY '{node['node_id']}' обязан вести REJECTED "
                f"в узел BILLING (списание BLOCKED всё равно происходит), а не терминировать напрямую"
            )
            continue
        target = nodes_by_id.get(rejected_target_id)
        if target is None or target["stage_name"] != "BILLING":
            errors.append(
                f"Правило hld.md §5.3.1: POLICY.REJECTED узла '{node['node_id']}' ведёт в "
                f"{target['stage_name'] if target else 'несуществующий узел'}, а должен — в BILLING"
            )

    for node in doc["nodes"]:
        for outcome, target_id in node["next"].items():
            if target_id is not None and target_id not in nodes_by_id:
                errors.append(f"Узел '{node['node_id']}' ссылается на несуществующий next '{target_id}' ({outcome})")

    return errors


def validate_routing_table(doc: dict) -> list[str]:
    route_ids = {r["route_id"] for r in doc["routes"]}
    if doc["active_route_id"] not in route_ids:
        return [f"active_route_id={doc['active_route_id']!r} не входит в routes[]: {sorted(route_ids)}"]
    priorities = [r["failover_priority"] for r in doc["routes"]]
    if len(priorities) != len(set(priorities)):
        return [f"failover_priority не уникален внутри routes[]: {priorities}"]
    return []


def validate_number_range(doc: dict) -> list[str]:
    if doc["range_end"] < doc["range_start"]:
        return [f"range_end ({doc['range_end']}) < range_start ({doc['range_start']}) — пустой или обратный диапазон"]
    return []


SEMANTIC_CHECKS = {
    "pipeline": validate_pipeline_graph,
    "routing_table": validate_routing_table,
    "number_range": validate_number_range,
}


def run() -> int:
    total = 0
    unexpected = 0
    for example_path in sorted(EXAMPLES.glob("*.json")):
        total += 1
        expected_valid = ".invalid" not in example_path.name
        schema = _schema_for(example_path)
        instance = _load(example_path)

        errors = structural_errors(instance, schema)
        prefix = example_path.name.split(".")[0]
        if not errors and prefix in SEMANTIC_CHECKS:
            errors = SEMANTIC_CHECKS[prefix](instance)

        actually_valid = not errors
        ok = actually_valid == expected_valid
        status = "OK" if ok else "MISMATCH"
        print(f"[{status}] {example_path.name} — ожидали {'valid' if expected_valid else 'invalid'}, "
              f"получили {'valid' if actually_valid else 'invalid'}"
              + (f" ({errors[0]})" if errors and not expected_valid else ""))
        if not ok:
            unexpected += 1
            for e in errors:
                print(f"           {e}")

    print(f"\n{total - unexpected}/{total} примеров совпали с ожиданием")
    return 1 if unexpected else 0


if __name__ == "__main__":
    sys.exit(run())
