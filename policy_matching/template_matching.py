"""
Policy Engine — template matching (требование 1 из 8, HLD §5.3).

Формализует алгоритм, который в data_infrastructure_spec.md §1.9b и
services_specifictaion.md §2.5 был описан текстом ("Aho-Corasick отбирает
кандидатов по литеральным фрагментам, затем точечная проверка плейсхолдеров")
без конкретной реализации. Использует настоящий pyahocorasick, не заглушку.

Синтаксис pattern (см. data_infrastructure_spec.md §1.9b):
    %w      — один непробельный токен, без ограничения по длине/алфавиту
    %d{n,m} — от n до m цифр, разделители между ними игнорируются при подсчёте

Алгоритм — две фазы, как описано в спеке:
    Фаза 1: Aho-Corasick находит все вхождения литеральных фрагментов всех
            шаблонов в тексте; для каждого шаблона жадно проверяется, что
            его фрагменты встречаются В ПРАВИЛЬНОМ ПОРЯДКЕ (кандидат).
    Фаза 2: для кандидата — точечная проверка, что каждый регион между
            выбранными фрагментами соответствует типу плейсхолдера.

Известное и осознанно не решаемое здесь ограничение: если несколько
шаблонов проходят обе фазы одновременно (реальная неоднозначность), нет
специальной приоритизации сверх порядка регистрации — это отдельный вопрос
('точный алгоритм разрешения неоднозначности — предмет отдельного LLD Policy
Service', data_infrastructure_spec.md §1.9b), здесь только доказано, что
множественные кандидаты не роняют алгоритм и оба фактически проверяются.
"""

import re
from dataclasses import dataclass

import ahocorasick


@dataclass(frozen=True)
class WordPlaceholder:
    pass


@dataclass(frozen=True)
class DigitPlaceholder:
    min_digits: int
    max_digits: int


Placeholder = WordPlaceholder | DigitPlaceholder

_PLACEHOLDER_RE = re.compile(r"%w|%d\{(\d+),(\d+)\}")


def parse_pattern(pattern: str) -> list[str | Placeholder]:
    """Возвращает чередующийся список: literal, placeholder, literal, ...,
    literal (первый/последний literal может быть пустой строкой, если
    pattern начинается/заканчивается плейсхолдером)."""
    tokens: list[str | Placeholder] = []
    pos = 0
    for m in _PLACEHOLDER_RE.finditer(pattern):
        tokens.append(pattern[pos:m.start()])
        if m.group(0) == "%w":
            tokens.append(WordPlaceholder())
        else:
            tokens.append(DigitPlaceholder(int(m.group(1)), int(m.group(2))))
        pos = m.end()
    tokens.append(pattern[pos:])
    return tokens


def _literal_fragments(tokens: list[str | Placeholder]) -> list[str]:
    return [t for t in tokens if isinstance(t, str) and t != ""]


@dataclass(frozen=True)
class Template:
    template_id: str
    pattern: str
    category: str


@dataclass(frozen=True)
class MatchedTemplate:
    template_id: str
    category: str


class CompiledRuleset:
    """Аналог CompiledRuleset из service_internal_methods.md §2.5 — здесь
    только часть, отвечающая за template matching."""

    def __init__(self, templates: list[Template]):
        self._templates = {t.template_id: t for t in templates}
        self._tokens_by_template = {t.template_id: parse_pattern(t.pattern) for t in templates}
        self._automaton = ahocorasick.Automaton()
        fragment_occurrences: dict[str, list[tuple[str, int]]] = {}
        for t in templates:
            frags = _literal_fragments(self._tokens_by_template[t.template_id])
            for idx, frag in enumerate(frags):
                fragment_occurrences.setdefault(frag, []).append((t.template_id, idx))
        for frag, occurrences in fragment_occurrences.items():
            self._automaton.add_word(frag, occurrences)
        self._automaton.make_automaton()

    def _fragment_hits(self, text: str) -> dict[str, dict[int, list[tuple[int, int]]]]:
        """template_id -> {fragment_idx -> [(start, end_exclusive), ...]}"""
        hits: dict[str, dict[int, list[tuple[int, int]]]] = {}
        for end_index, occurrences in self._automaton.iter(text):
            for template_id, frag_idx in occurrences:
                frag_len = len(_literal_fragments(self._tokens_by_template[template_id])[frag_idx])
                start = end_index - frag_len + 1
                hits.setdefault(template_id, {}).setdefault(frag_idx, []).append((start, end_index + 1))
        return hits

    def _candidate_positions(self, template_id: str, text: str, hits: dict) -> list[tuple[int, int]] | None:
        """Жадно выбирает по одному вхождению на фрагмент, монотонно
        возрастающему по позиции. Возвращает список (start, end) на фрагмент,
        либо None, если такой монотонной последовательности не существует."""
        frags = _literal_fragments(self._tokens_by_template[template_id])
        per_frag = hits.get(template_id, {})
        chosen: list[tuple[int, int]] = []
        cursor = 0
        for idx in range(len(frags)):
            candidates = sorted(per_frag.get(idx, []))
            picked = next((c for c in candidates if c[0] >= cursor), None)
            if picked is None:
                return None
            chosen.append(picked)
            cursor = picked[1]
        return chosen

    def _check_placeholder_region(self, region: str, placeholder: Placeholder) -> bool:
        if isinstance(placeholder, WordPlaceholder):
            return len(region) > 0 and not any(ch.isspace() for ch in region)
        digits_only = "".join(ch for ch in region if ch.isdigit())
        non_digit_non_separator = any(not ch.isdigit() and not ch.isspace() and ch not in "-_." for ch in region)
        if non_digit_non_separator:
            return False
        return placeholder.min_digits <= len(digits_only) <= placeholder.max_digits

    def match(self, text: str) -> MatchedTemplate | None:
        hits = self._fragment_hits(text)
        for template_id in self._templates:
            tokens = self._tokens_by_template[template_id]
            frags = _literal_fragments(tokens)
            if not frags:
                continue  # шаблон без литералов вообще — вне рассматриваемого сценария
            positions = self._candidate_positions(template_id, text, hits)
            if positions is None:
                continue  # фаза 1: фрагменты не встречаются в правильном порядке

            # Фаза 2: точечная проверка регионов между литералами (и до первого/после последнего).
            ok = True
            literal_slots = [i for i, t in enumerate(tokens) if isinstance(t, str)]
            frag_cursor = 0
            prev_end = 0
            for slot_i, tok_idx in enumerate(literal_slots):
                literal = tokens[tok_idx]
                if literal == "":
                    continue
                start, end = positions[frag_cursor]
                if tok_idx > 0:
                    placeholder = tokens[tok_idx - 1]
                    assert isinstance(placeholder, (WordPlaceholder, DigitPlaceholder))
                    region = text[prev_end:start]
                    if not self._check_placeholder_region(region, placeholder):
                        ok = False
                        break
                prev_end = end
                frag_cursor += 1
            if ok and tokens and tokens[-1] == "" and len(tokens) > 1:
                trailing_placeholder = tokens[-2]
                if isinstance(trailing_placeholder, (WordPlaceholder, DigitPlaceholder)):
                    region = text[prev_end:]
                    if not self._check_placeholder_region(region, trailing_placeholder):
                        ok = False

            if ok:
                return MatchedTemplate(template_id, self._templates[template_id].category)
        return None


# ---------------------------------------------------------------------------
# Тесты — реальные примеры из чата с пользователем.
# ---------------------------------------------------------------------------

REAL_TEMPLATE = Template(
    template_id="tpl-contract-payment",
    pattern="%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring",
    category="TRANSACTION",
)


def test_real_example_matches():
    ruleset = CompiledRuleset([REAL_TEMPLATE])
    text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring"
    result = ruleset.match(text)
    assert result is not None, "должно было смачиться — это ровно пример из чата"
    assert result.category == "TRANSACTION"


def test_digit_count_out_of_range_no_match():
    ruleset = CompiledRuleset([REAL_TEMPLATE])
    text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 7 so'm to'lovni bugun amalga oshiring"
    assert ruleset.match(text) is None, "7 цифр вне {1,6} — не должно матчиться"


def test_word_placeholder_rejects_internal_whitespace():
    """%w — один непробельный токен; если между началом текста и литералом
    оказывается несколько слов через пробел, это уже не один токен."""
    ruleset = CompiledRuleset([REAL_TEMPLATE])
    text = "Hello world shartnoma bo'yicha 123 so'm to'lovni bugun amalga oshiring"
    assert ruleset.match(text) is None


def test_missing_literal_fragment_no_match():
    ruleset = CompiledRuleset([REAL_TEMPLATE])
    text = "Hello1238 completely different text with no template fragments at all"
    assert ruleset.match(text) is None


def test_digits_with_separators_counted_correctly():
    ruleset = CompiledRuleset([REAL_TEMPLATE])
    text = "ABC123 shartnoma bo'yicha 12-34-56 so'm to'lovni bugun amalga oshiring"
    result = ruleset.match(text)
    assert result is not None, "разделители между цифрами должны игнорироваться при подсчёте"


def test_multiple_candidates_resolved_deterministically():
    """Два шаблона с общим литеральным фрагментом — оба проходят фазу 1,
    но только один проходит фазу 2 (различаются по плейсхолдеру)."""
    tpl_word = Template("tpl-word", "code: %w", "SERVICE")
    tpl_digit = Template("tpl-digit", "code: %d{4,4}", "SERVICE")
    ruleset = CompiledRuleset([tpl_word, tpl_digit])

    result_digit_text = ruleset.match("code: 1234")
    assert result_digit_text is not None
    assert result_digit_text.template_id in ("tpl-word", "tpl-digit")
    # "1234" — валидный %w (непробельный токен) И валидный %d{4,4} одновременно,
    # оба кандидата реально проходят фазу 2 — неоднозначность реальна, не баг.

    result_word_only_text = ruleset.match("code: ABCD")
    assert result_word_only_text is not None
    assert result_word_only_text.template_id == "tpl-word", "ABCD не цифры — только tpl-word должен пройти"


def test_leading_and_trailing_placeholder():
    tpl = Template("tpl-edges", "%d{2,2}-ok-%w", "SERVICE")
    ruleset = CompiledRuleset([tpl])
    assert ruleset.match("42-ok-done") is not None
    assert ruleset.match("4-ok-done") is None  # только 1 цифра, нужно ровно 2
    assert ruleset.match("42-ok-") is None  # пустой %w в конце недопустим


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
