//! Порт `policy_matching/banword_normalization.py` — та же таблица
//! гомоглифов (не исчерпывающая, IDN-homograph-класс пар, `development_plan.md`
//! 4.4 — расширение вне скоупа этого среза), тот же двойной проход
//! (normalized + despaced), тот же принятый компромисс: despaced-проход может
//! случайно склеить два слова в ложное совпадение — осознанно, не баг
//! (см. `test_despaced_pass_can_false_positive_by_design`).

use aho_corasick::AhoCorasick;
use unicode_normalization::UnicodeNormalization;

fn homoglyph_fold(ch: char) -> char {
    match ch {
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
}
