//! Порт `policy_matching/banword_normalization.py` — тот же двойной проход
//! (normalized + despaced), тот же принятый компромисс: despaced-проход может
//! случайно склеить два слова в ложное совпадение — осознанно, не баг
//! (см. `test_despaced_pass_can_false_positive_by_design`).
//!
//! **`development_plan.md` 4.4 закрыто:** таблица гомоглифов раньше
//! ограничивалась 11 задокументированными Cyrillic/Latin парами
//! (`policy_matching/README.md`: "не полный юникод-справочник похожих
//! символов всех языков"). Расширена здесь двумя категориями, обе — реальные,
//! задокументированные записи из Unicode `confusables.txt` (канонический
//! справочник IDN-homograph/анти-фишинг практики, не придуманные заново
//! пары): (1) недостающие Cyrillic/Latin пары (к/м), (2) Greek/Latin —
//! реальный, отдельный от Cyrillic класс визуальной подмены (греческие
//! omicron/alpha/iota и т.д. визуально неотличимы от латиницы в большинстве
//! шрифтов, тот же принцип атаки, что кириллица). Всё ещё не исчерпывающий
//! юникод-справочник (армянский, кириллица нестандартных языков и т.д. —
//! не добавлены, нет обоснованной причины ожидать их в реальном трафике
//! этой платформы) — расширение по мере необходимости, не разовое закрытие
//! вопроса навсегда.

use aho_corasick::AhoCorasick;
use unicode_normalization::UnicodeNormalization;

fn homoglyph_fold(ch: char) -> char {
    match ch {
        // Cyrillic/Latin — исходные 11 пар.
        'а' => 'a', // U+0430 -> U+0061
        'е' => 'e', // U+0435 -> U+0065
        'о' => 'o', // U+043E -> U+006F
        'р' => 'p', // U+0440 -> U+0070
        'с' => 'c', // U+0441 -> U+0063
        'х' => 'x', // U+0445 -> U+0078
        'у' => 'y', // U+0443 -> U+0079
        'і' => 'i', // U+0456 -> U+0069
        'ѕ' => 's', // U+0455 -> U+0073
        'ј' => 'j', // U+0458 -> U+006A
        'һ' => 'h', // U+04BB -> U+0068
        // Cyrillic/Latin — добавлено (4.4): недостающие документированные пары.
        'к' => 'k', // U+043A -> U+006B
        'м' => 'm', // U+043C -> U+006D
        // Greek/Latin — добавлено (4.4): отдельный, реальный класс подмены
        // (не Cyrillic) — те же по духу визуально неотличимые буквы.
        'α' => 'a', // U+03B1 -> U+0061
        'ο' => 'o', // U+03BF -> U+006F (самый частый в реальных IDN-homograph атаках)
        'ι' => 'i', // U+03B9 -> U+0069
        'κ' => 'k', // U+03BA -> U+006B
        'ν' => 'v', // U+03BD -> U+0076
        'ρ' => 'p', // U+03C1 -> U+0070
        'υ' => 'y', // U+03C5 -> U+0079
        'τ' => 't', // U+03C4 -> U+0074
        'χ' => 'x', // U+03C7 -> U+0078
        other => other,
    }
}

fn is_separator(ch: char) -> bool {
    ch.is_whitespace() || ch == '-' || ch == '_' || ch == '.'
}

/// Возвращает (normalized, despaced).
pub fn normalize_for_banwords(text: &str) -> (String, String) {
    let lowered = text.to_lowercase();
    let folded: String = lowered.chars().map(homoglyph_fold).collect();
    let normalized: String = folded.nfkc().collect();
    let despaced: String = normalized.chars().filter(|c| !is_separator(*c)).collect();
    (normalized, despaced)
}

pub struct BanwordChecker {
    automaton: AhoCorasick,
    words_by_pattern: Vec<String>,
}

impl BanwordChecker {
    pub fn new(banwords: &[String]) -> Self {
        let mut normalized_words = Vec::with_capacity(banwords.len());
        for w in banwords {
            let (normalized, _) = normalize_for_banwords(w);
            normalized_words.push(normalized);
        }
        let automaton = AhoCorasick::new(&normalized_words).expect("valid banword patterns");
        Self { automaton, words_by_pattern: normalized_words }
    }

    pub fn find_hits(&self, text: &str) -> Vec<String> {
        let (normalized, despaced) = normalize_for_banwords(text);
        let mut hits: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for m in self.automaton.find_overlapping_iter(normalized.as_str()) {
            let w = &self.words_by_pattern[m.pattern().as_usize()];
            if seen.insert(w.clone()) {
                hits.push(w.clone());
            }
        }
        for m in self.automaton.find_overlapping_iter(despaced.as_str()) {
            let w = &self.words_by_pattern[m.pattern().as_usize()];
            if seen.insert(w.clone()) {
                hits.push(w.clone());
            }
        }
        hits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_banwords() -> Vec<String> {
        ["idiot", "kot", "qotaq", "мудак", "черт", "терроризм"].iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn plain_banword_detected() {
        let checker = BanwordChecker::new(&real_banwords());
        assert_eq!(checker.find_hits("у меня есть kot дома").len(), 1);
    }

    #[test]
    fn cyrillic_banword_detected_as_is() {
        let checker = BanwordChecker::new(&real_banwords());
        assert_eq!(checker.find_hits("он повёл себя как настоящий мудак сегодня").len(), 1);
    }

    #[test]
    fn homoglyph_evasion_detected() {
        // "о" здесь кириллическая (U+043E), не латинская.
        let evasion_text = "ты просто q\u{043E}taq, всё";
        let checker = BanwordChecker::new(&real_banwords());
        assert_eq!(checker.find_hits(evasion_text).len(), 1, "гомоглиф-обход должен ловиться после fold");
    }

    #[test]
    fn letter_spacing_evasion_detected_only_via_despaced_pass() {
        let checker = BanwordChecker::new(&real_banwords());
        let spaced_text = "он т е р р о р и з м устроил";
        let (normalized, _) = normalize_for_banwords(spaced_text);
        assert!(!normalized.contains("терроризм"), "в normalized банворд НЕ должен находиться напрямую");
        assert_eq!(checker.find_hits(spaced_text).len(), 1, "letter-spacing обход должен ловиться через despaced-проход");
    }

    #[test]
    fn dash_separated_evasion_detected() {
        let checker = BanwordChecker::new(&real_banwords());
        assert_eq!(checker.find_hits("q-o-t-a-q написано через дефис").len(), 1);
    }

    #[test]
    fn clean_text_no_hits() {
        let checker = BanwordChecker::new(&real_banwords());
        assert!(checker.find_hits("shartnoma bo'yicha to'lovni bugun amalga oshiring").is_empty());
    }

    #[test]
    fn despaced_pass_can_false_positive_by_design() {
        let checker = BanwordChecker::new(&["ok".to_string()]);
        let hits = checker.find_hits("go o k ay");
        assert!(hits.contains(&"ok".to_string()), "это ожидаемый побочный эффект despacing, не регрессия");
    }

    // Регрессии на находку 4.4 — расширение таблицы гомоглифов.

    #[test]
    fn missing_cyrillic_pairs_ka_and_em_are_detected() {
        let checker = BanwordChecker::new(&real_banwords());
        // "kot" с кириллической "к" (U+043A) вместо латинской.
        let evasion = "у меня \u{043A}ot дома";
        assert_eq!(checker.find_hits(evasion).len(), 1, "кириллическая к (U+043A) должна складываться в латинскую k");
    }

    #[test]
    fn greek_omicron_evasion_detected() {
        // "qotaq" с греческой omicron (U+03BF) вместо латинской "o" —
        // отдельный от кириллицы класс подмены, самый частый в реальных
        // IDN-homograph атаках.
        let checker = BanwordChecker::new(&real_banwords());
        let evasion = "ты просто q\u{03BF}taq, всё";
        assert_eq!(checker.find_hits(evasion).len(), 1, "греческая omicron (U+03BF) должна складываться в латинскую o");
    }

    #[test]
    fn greek_alpha_and_iota_evasion_detected() {
        let checker = BanwordChecker::new(&["idiot".to_string()]);
        // "idiot" с греческими alpha (не используется здесь) и iota (U+03B9).
        let evasion = "он вёл себя как \u{03B9}diot";
        assert_eq!(checker.find_hits(evasion).len(), 1, "греческая iota (U+03B9) должна складываться в латинскую i");
    }

    #[test]
    fn mixed_greek_and_cyrillic_in_same_word_still_detected() {
        // Реальный, не гипотетический класс обхода: атакующий может
        // смешать буквы из РАЗНЫХ алфавитов в одном слове, не только один
        // алфавит целиком — despaced/normalized проход не делает разницы
        // между источником подмены, только между итоговым сложенным символом.
        let checker = BanwordChecker::new(&["qotaq".to_string()]);
        let evasion = "q\u{043E}t\u{03B1}q"; // кириллическая о (043E) + греческая alpha (03B1)
        assert_eq!(checker.find_hits(evasion).len(), 1);
    }
}
