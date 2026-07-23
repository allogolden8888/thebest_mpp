"""
Policy Engine — оркестрация всех восьми проверок (service_internal_methods.md
§1.5). `template_matching.py` (требование 1) и `banword_normalization.py`
(требование 5) уже были формализованы отдельно — этот модуль связывает их с
оставшимися шестью в том же порядке, что зафиксирован в таблице методов:

    7. validate_sender            — партнёрский allowlist sender_id
    6. check_sender_blacklist      — Runtime Redis, consent по отправителю
    1. match_template / resolve_unmatched_behavior — category
    5. check_banwords              — ПОСЛЕ template match, но независимо от
                                      его результата: permissive %w не создаёт
                                      обход контентной модерации (см. чат/
                                      data_infrastructure_spec.md §1.9b) —
                                      проверяется явным тестом ниже.
    4. check_category_blacklist    — нужна category
    3. check_time_of_day           — нужна category
    2. check_spam_frequency        — нужна category; increment только если
                                      сообщение реально прошло все проверки

`aggregate_result` здесь не отдельный метод, а естественный результат
последовательного short-circuit: как только любая проверка возвращает
Blocked/Throttled/Invalid, category принудительно "BLOCKED", reason_code
несёт конкретную причину — ровно семантика из документа ("категория для
тарификации одна, reason_code — конкретная причина").

Runtime Redis (consent-блэклисты, spam-счётчик) и local snapshot
(policy_ruleset) здесь — in-memory заглушки с той же формой данных, что
`config_schemas/policy_ruleset.schema.json` — намеренно грузится тот же
JSON-пример (`config_schemas/examples/policy_ruleset.valid.json`), чтобы
доказать, что оркестратор и JSON Schema согласованы, а не выдуманы отдельно.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from pathlib import Path

from banword_normalization import BanwordChecker
from template_matching import CompiledRuleset, Template


@dataclass(frozen=True)
class MessageContext:
    msisdn: str
    sender_id: str
    body: str
    partner_id: str
    resolved_operator_id: str


@dataclass(frozen=True)
class PolicyRulesetConfig:
    unmatched_template_behavior: str  # "CATEGORIZE_AS_UNTEMPLATED" | "REJECT"
    anti_spam_max_messages: int
    anti_spam_window_seconds: int
    anti_spam_scope: str  # "PER_MSISDN" | "PER_MSISDN_PER_CATEGORY"
    time_of_day: dict[str, tuple[time, time]]
    allowed_sender_ids: set[str]
    banwords: list[str]

    @staticmethod
    def from_config_schema_json(payload: dict) -> "PolicyRulesetConfig":
        """Парсит ровно ту форму, что описана в
        config_schemas/policy_ruleset.schema.json — не отдельный формат."""
        def parse_hhmm(s: str) -> time:
            h, m = s.split(":")
            return time(int(h), int(m))

        time_of_day = {
            entry["category"]: (parse_hhmm(entry["allowed_from"]), parse_hhmm(entry["allowed_to"]))
            for entry in payload["time_of_day"]
        }
        return PolicyRulesetConfig(
            unmatched_template_behavior=payload["unmatched_template_behavior"],
            anti_spam_max_messages=payload["anti_spam"]["max_messages"],
            anti_spam_window_seconds=payload["anti_spam"]["window_seconds"],
            anti_spam_scope=payload["anti_spam"]["scope"],
            time_of_day=time_of_day,
            allowed_sender_ids=set(payload["sender_validation"]["allowed_sender_ids"]),
            banwords=list(payload["banwords"]["words"]),
        )


@dataclass
class RuntimeState:
    """Заглушка Runtime Redis: consent-блэклисты + spam-счётчик по окну."""
    sender_blacklist: set[tuple[str, str]] = field(default_factory=set)
    category_blacklist: set[tuple[str, str]] = field(default_factory=set)
    spam_history: dict[tuple, list[datetime]] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyOutcome:
    outcome: str  # "SUCCEEDED" | "REJECTED"
    category: str  # никогда не пусто
    reason_code: str | None  # None только при чистом SUCCEEDED с совпавшим шаблоном/UNTEMPLATED


def evaluate_policy(
    ctx: MessageContext,
    ruleset: PolicyRulesetConfig,
    templates: CompiledRuleset,
    banword_checker: BanwordChecker,
    runtime: RuntimeState,
    now: datetime,
) -> PolicyOutcome:
    # Требование 7 — validate_sender
    if ruleset.allowed_sender_ids and ctx.sender_id not in ruleset.allowed_sender_ids:
        return PolicyOutcome("REJECTED", "BLOCKED", "INVALID_SENDER")

    # Требование 6 — check_sender_blacklist
    if (ctx.msisdn, ctx.sender_id) in runtime.sender_blacklist:
        return PolicyOutcome("REJECTED", "BLOCKED", "SENDER_BLACKLISTED")

    # Требование 1 — match_template / resolve_unmatched_behavior
    matched = templates.match(ctx.body)
    if matched is not None:
        category = matched.category
    elif ruleset.unmatched_template_behavior == "REJECT":
        return PolicyOutcome("REJECTED", "BLOCKED", "NO_TEMPLATE_MATCH")
    else:
        category = "UNTEMPLATED"

    # Требование 5 — check_banwords. Намеренно ПОСЛЕ определения category,
    # но результат совпадения шаблона НЕ освобождает от этой проверки —
    # %w пермиссивен структурно, но не по содержимому.
    if banword_checker.find_hits(ctx.body):
        return PolicyOutcome("REJECTED", "BLOCKED", "BANWORD_DETECTED")

    # Требование 4 — check_category_blacklist
    if (ctx.msisdn, category) in runtime.category_blacklist:
        return PolicyOutcome("REJECTED", "BLOCKED", "CATEGORY_BLACKLISTED")

    # Требование 3 — check_time_of_day
    window = ruleset.time_of_day.get(category)
    if window is not None:
        start, end = window
        if not (start <= now.time() <= end):
            return PolicyOutcome("REJECTED", "BLOCKED", "OUTSIDE_TIME_WINDOW")

    # Требование 2 — check_spam_frequency
    spam_key = ctx.msisdn if ruleset.anti_spam_scope == "PER_MSISDN" else (ctx.msisdn, category)
    window_start = now - timedelta(seconds=ruleset.anti_spam_window_seconds)
    recent = [t for t in runtime.spam_history.get(spam_key, []) if t >= window_start]
    if len(recent) >= ruleset.anti_spam_max_messages:
        return PolicyOutcome("REJECTED", "BLOCKED", "SPAM_THROTTLED")

    # increment_spam_counter — только для сообщений, реально прошедших все проверки.
    recent.append(now)
    runtime.spam_history[spam_key] = recent

    return PolicyOutcome("SUCCEEDED", category, None)


# ---------------------------------------------------------------------------
# Тесты — грузят реальный config_schemas/examples/policy_ruleset.valid.json.
# ---------------------------------------------------------------------------

CONFIG_SCHEMAS_EXAMPLES = Path(__file__).parent.parent / "config_schemas" / "examples"

REAL_TEMPLATE = Template(
    template_id="tpl-contract-payment",
    pattern="%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring",
    category="TRANSACTION",
)


def _load_real_ruleset() -> PolicyRulesetConfig:
    payload = json.loads((CONFIG_SCHEMAS_EXAMPLES / "policy_ruleset.valid.json").read_text())
    return PolicyRulesetConfig.from_config_schema_json(payload)


def _fresh_env():
    ruleset = _load_real_ruleset()
    templates = CompiledRuleset([REAL_TEMPLATE])
    banwords = BanwordChecker(ruleset.banwords)
    runtime = RuntimeState()
    return ruleset, templates, banwords, runtime


def test_happy_path_matched_template():
    ruleset, templates, banwords, runtime = _fresh_env()
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("SUCCEEDED", "TRANSACTION", None)


def test_invalid_sender_rejected_before_anything_else():
    ruleset, templates, banwords, runtime = _fresh_env()
    ctx = MessageContext(
        msisdn="998901331835", sender_id="NotClick", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "INVALID_SENDER")


def test_sender_blacklisted():
    ruleset, templates, banwords, runtime = _fresh_env()
    runtime.sender_blacklist.add(("998901331835", "Click"))
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "SENDER_BLACKLISTED")


def test_unmatched_template_categorized_as_untemplated():
    ruleset, templates, banwords, runtime = _fresh_env()
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="совершенно другой текст без шаблона",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("SUCCEEDED", "UNTEMPLATED", None)


def test_unmatched_template_reject_policy():
    ruleset, templates, banwords, runtime = _fresh_env()
    strict_ruleset = PolicyRulesetConfig(
        unmatched_template_behavior="REJECT",
        anti_spam_max_messages=ruleset.anti_spam_max_messages,
        anti_spam_window_seconds=ruleset.anti_spam_window_seconds,
        anti_spam_scope=ruleset.anti_spam_scope,
        time_of_day=ruleset.time_of_day,
        allowed_sender_ids=ruleset.allowed_sender_ids,
        banwords=ruleset.banwords,
    )
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="совершенно другой текст без шаблона",
    )
    result = evaluate_policy(ctx, strict_ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "NO_TEMPLATE_MATCH")


def test_banword_blocks_even_though_template_matches():
    """Ключевое свойство: %w пермиссивен структурно (примет что угодно
    непробельное), но это не создаёт обход банвордов — они проверяются
    по ПОЛНОМУ тексту независимо от результата match_template."""
    ruleset, templates, banwords, runtime = _fresh_env()
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="idiot shartnoma bo'yicha 123456 so'm to'lovni bugun amalga oshiring",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "BANWORD_DETECTED")


def test_category_blacklisted():
    ruleset, templates, banwords, runtime = _fresh_env()
    runtime.category_blacklist.add(("998901331835", "TRANSACTION"))
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring",
    )
    result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=datetime(2026, 7, 22, 12, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "CATEGORY_BLACKLISTED")


def test_outside_time_window():
    ruleset, templates, banwords, runtime = _fresh_env()
    ad_template = Template("tpl-ads", "%w reklama", "ADVERTISING")
    templates_with_ads = CompiledRuleset([REAL_TEMPLATE, ad_template])
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="SuperSale reklama",
    )
    # policy_ruleset.valid.json: ADVERTISING разрешена 09:00-20:00 Asia/Tashkent.
    result = evaluate_policy(ctx, ruleset, templates_with_ads, banwords, runtime, now=datetime(2026, 7, 22, 23, 0))
    assert result == PolicyOutcome("REJECTED", "BLOCKED", "OUTSIDE_TIME_WINDOW")

    result_daytime = evaluate_policy(ctx, ruleset, templates_with_ads, banwords, runtime, now=datetime(2026, 7, 22, 10, 0))
    assert result_daytime == PolicyOutcome("SUCCEEDED", "ADVERTISING", None)


def test_spam_throttled_after_limit_and_counter_not_incremented_on_rejects():
    ruleset, templates, banwords, runtime = _fresh_env()
    ctx = MessageContext(
        msisdn="998901331835", sender_id="Click", partner_id="click_uz", resolved_operator_id="beeline_uz",
        body="Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring",
    )
    base = datetime(2026, 7, 22, 12, 0)
    # policy_ruleset.valid.json: max_messages=3, window_seconds=60, PER_MSISDN_PER_CATEGORY.
    for i in range(3):
        result = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=base + timedelta(seconds=i))
        assert result.outcome == "SUCCEEDED", f"сообщение {i} должно пройти (в пределах лимита 3)"

    fourth = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=base + timedelta(seconds=3))
    assert fourth == PolicyOutcome("REJECTED", "BLOCKED", "SPAM_THROTTLED")

    # Заблокированный отправитель не должен занимать место в spam-окне —
    # increment_spam_counter вызывается только для реально пропущенных сообщений.
    runtime.sender_blacklist.add(("998901331835", "Click"))
    rejected_by_sender = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=base + timedelta(seconds=4))
    assert rejected_by_sender == PolicyOutcome("REJECTED", "BLOCKED", "SENDER_BLACKLISTED")
    runtime.sender_blacklist.clear()

    # После истечения окна (60с) лимит должен сброситься.
    after_window = evaluate_policy(ctx, ruleset, templates, banwords, runtime, now=base + timedelta(seconds=61))
    assert after_window.outcome == "SUCCEEDED", "окно анти-спама истекло — счётчик должен сброситься"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
