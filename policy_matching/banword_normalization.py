"""
Policy Engine — банворды с многоязычной нормализацией (требование 5 из 8, HLD §5.3).

Формализует то, что data_infrastructure_spec.md §1.9b описывал как требование
без алгоритма: "нормализация не может ограничиться одним алфавитом — нужна
unicode-нормализация (NFKC), схлопывание похожих по начертанию символов между
кириллицей и латиницей, снятие разделителей внутри слова". Реальный список
банвордов из чата: idiot, kot, qotaq, мудак, черт, терроризм — намеренно
смешивает узбекскую латиницу, русскую кириллицу и английский в одном списке.

Два независимых прохода нормализации, оба через Aho-Corasick:
1. normalized — lowercase + homoglyph fold + NFKC, пробелы сохранены.
   Ловит смешение алфавитов ("qоtaq" с кириллической "о" вместо латинской).
2. despaced — тот же normalized, но с вырезанными пробелами/дефисами/точками/
   подчёркиваниями. Ловит letter-spacing обход ("т е р р о р и з м").
   Осознанный компромисс: вырезание пробелов может случайно склеить два
   независимых слова в одну подстроку, совпадающую с банвордом — это
   принимается как false-positive risk в обмен на не false-negative по
   letter-spacing обходу (совпадение уходит на модерацию, не тихо теряется).

Таблица гомоглифов — не исчерпывающая (нет юникод-таблицы "все похожие
символы всех языков"), покрывает только явно documented Cyrillic/Latin пары,
используемые в IDN homograph / фишинг-детекции. Расширение таблицы — предмет
дальнейшего LLD Policy Service, не блокирует этот алгоритм.
"""

import re
import unicodedata

import ahocorasick

# Кириллица -> визуально идентичная латиница (нижний регистр; .lower() вызывается
# до применения таблицы, так что верхний регистр сюда попадать не должен).
HOMOGLYPH_FOLD = {
    "а": "a",  # U+0430 -> U+0061
    "е": "e",  # U+0435 -> U+0065
    "о": "o",  # U+043E -> U+006F
    "р": "p",  # U+0440 -> U+0070
    "с": "c",  # U+0441 -> U+0063
    "х": "x",  # U+0445 -> U+0078
    "у": "y",  # U+0443 -> U+0079
    "і": "i",  # U+0456 -> U+0069
    "ѕ": "s",  # U+0455 -> U+0073
    "ј": "j",  # U+0458 -> U+006A
    "һ": "h",  # U+04BB -> U+0068
}

_SEPARATOR_RE = re.compile(r"[\s\-_.]+")


def normalize_for_banwords(text: str) -> tuple[str, str]:
    """Возвращает (normalized, despaced)."""
    lowered = text.lower()
    folded = "".join(HOMOGLYPH_FOLD.get(ch, ch) for ch in lowered)
    normalized = unicodedata.normalize("NFKC", folded)
    despaced = _SEPARATOR_RE.sub("", normalized)
    return normalized, despaced


class BanwordChecker:
    def __init__(self, banwords: list[str]):
        self._automaton = ahocorasick.Automaton()
        for word in banwords:
            normalized, _ = normalize_for_banwords(word)
            self._automaton.add_word(normalized, normalized)
        self._automaton.make_automaton()

    def find_hits(self, text: str) -> set[str]:
        normalized, despaced = normalize_for_banwords(text)
        hits = {word for _, word in self._automaton.iter(normalized)}
        hits |= {word for _, word in self._automaton.iter(despaced)}
        return hits


# ---------------------------------------------------------------------------
# Тесты — реальный список банвордов из чата + типичные обходы.
# ---------------------------------------------------------------------------

REAL_BANWORDS = ["idiot", "kot", "qotaq", "мудак", "черт", "терроризм"]


def test_plain_banword_detected():
    checker = BanwordChecker(REAL_BANWORDS)
    hits = checker.find_hits("у меня есть kot дома")
    assert len(hits) == 1


def test_cyrillic_banword_detected_as_is():
    checker = BanwordChecker(REAL_BANWORDS)
    hits = checker.find_hits("он повёл себя как настоящий мудак сегодня")
    assert len(hits) == 1


def test_homoglyph_evasion_detected():
    """'qоtaq' — 'о' здесь кириллическая (U+043E), не латинская. Без fold
    это два разных байтовых представления, banword не находится."""
    evasion_text = "ты просто qоtaq, всё"
    assert ord(evasion_text[evasion_text.index("q") + 1]) == 0x043E, "тест должен содержать кириллическую 'о'"
    checker = BanwordChecker(REAL_BANWORDS)
    hits = checker.find_hits(evasion_text)
    assert len(hits) == 1, "гомоглиф-обход должен ловиться после fold"


def test_letter_spacing_evasion_detected_only_via_despaced_pass():
    checker = BanwordChecker(REAL_BANWORDS)
    spaced_text = "он т е р р о р и з м устроил"
    normalized, _ = normalize_for_banwords(spaced_text)
    assert "терроризм" not in normalized, "в normalized (пробелы сохранены) банворд НЕ должен находиться напрямую"
    hits = checker.find_hits(spaced_text)
    assert len(hits) == 1, "letter-spacing обход должен ловиться через despaced-проход"


def test_dash_separated_evasion_detected():
    checker = BanwordChecker(REAL_BANWORDS)
    hits = checker.find_hits("q-o-t-a-q написано через дефис")
    assert len(hits) == 1


def test_clean_text_no_hits():
    checker = BanwordChecker(REAL_BANWORDS)
    hits = checker.find_hits("shartnoma bo'yicha to'lovni bugun amalga oshiring")
    assert hits == set()


def test_despaced_pass_can_false_positive_by_design():
    """Задокументированный компромисс: despaced-проход может случайно склеить
    два слова в подстроку, совпадающую с банвордом, которого в исходном
    тексте не было как отдельного слова. Здесь — не баг, а принятый риск
    (см. docstring модуля): лучше лишний false positive на модерацию, чем
    пропущенный letter-spacing обход."""
    checker = BanwordChecker(["ok"])
    hits = checker.find_hits("go o k ay")  # "o k" рядом не задумывался как banword
    assert "ok" in hits, "это ожидаемый побочный эффект despacing, не регрессия"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
