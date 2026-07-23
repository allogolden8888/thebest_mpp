//! Порт `policy_matching/template_matching.py` на Rust — тот же алгоритм,
//! та же двухфазная схема (Aho-Corasick кандидаты -> точечная проверка
//! плейсхолдеров), тот же набор тестов на тех же данных из чата. Отличие от
//! Python-версии только в реализации Aho-Corasick: `aho-corasick` крейт не
//! поддерживает "одна строка -> много значений" как `pyahocorasick`
//! (`automaton.add_word(key, [список])`) — вместо этого каждый (template_id,
//! frag_idx) фрагмент становится отдельным паттерном в автомате (дубликаты
//! строк разрешены, у каждого свой `PatternID`), `pattern_owner` — обратный
//! маппинг `PatternID -> (template_id, frag_idx)`.
//!
//! Синтаксис pattern (data_infrastructure_spec.md §1.9b):
//!   %w      — один непробельный токен, без ограничения по длине/алфавиту
//!   %d{n,m} — от n до m цифр, разделители между ними игнорируются при подсчёте

use aho_corasick::AhoCorasick;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placeholder {
    Word,
    Digit { min: usize, max: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Literal(String),
    Placeholder(Placeholder),
}

/// Возвращает чередующийся список: Literal, Placeholder, Literal, ..., Literal
/// (первый/последний Literal может быть пустой строкой).
pub fn parse_pattern(pattern: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut pos = 0usize;
    let bytes = pattern.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if pattern[i..].starts_with("%w") {
            tokens.push(Token::Literal(pattern[pos..i].to_string()));
            tokens.push(Token::Placeholder(Placeholder::Word));
            i += 2;
            pos = i;
        } else if pattern[i..].starts_with("%d{") {
            if let Some(close) = pattern[i..].find('}') {
                let spec = &pattern[i + 3..i + close];
                if let Some((min_s, max_s)) = spec.split_once(',') {
                    if let (Ok(min), Ok(max)) = (min_s.parse::<usize>(), max_s.parse::<usize>()) {
                        tokens.push(Token::Literal(pattern[pos..i].to_string()));
                        tokens.push(Token::Placeholder(Placeholder::Digit { min, max }));
                        i += close + 1;
                        pos = i;
                        continue;
                    }
                }
            }
            i += 1;
        } else {
            i += 1;
        }
    }
    tokens.push(Token::Literal(pattern[pos..].to_string()));
    tokens
}

fn literal_fragments(tokens: &[Token]) -> Vec<&str> {
    tokens
        .iter()
        .filter_map(|t| match t {
            Token::Literal(s) if !s.is_empty() => Some(s.as_str()),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct Template {
    pub template_id: String,
    pub pattern: String,
    pub category: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedTemplate {
    pub template_id: String,
    pub category: String,
}

pub struct CompiledRuleset {
    templates: HashMap<String, Template>,
    /// Порядок регистрации — та же семантика, что `dict` в Python 3.7+
    /// (insertion order), от которой зависит `test_multiple_candidates_resolved_deterministically`.
    template_order: Vec<String>,
    tokens_by_template: HashMap<String, Vec<Token>>,
    automaton: Option<AhoCorasick>,
    pattern_owner: Vec<(String, usize)>, // PatternID (индекс) -> (template_id, frag_idx)
}

impl CompiledRuleset {
    pub fn new(templates: Vec<Template>) -> Self {
        let mut tokens_by_template = HashMap::new();
        let mut template_order = Vec::new();
        let mut patterns: Vec<String> = Vec::new();
        let mut pattern_owner: Vec<(String, usize)> = Vec::new();

        for t in &templates {
            let tokens = parse_pattern(&t.pattern);
            let frags: Vec<String> = literal_fragments(&tokens).into_iter().map(String::from).collect();
            for (idx, frag) in frags.iter().enumerate() {
                patterns.push(frag.clone());
                pattern_owner.push((t.template_id.clone(), idx));
            }
            tokens_by_template.insert(t.template_id.clone(), tokens);
            template_order.push(t.template_id.clone());
        }

        let automaton = if patterns.is_empty() { None } else { Some(AhoCorasick::new(&patterns).expect("valid patterns")) };
        let templates_map = templates.into_iter().map(|t| (t.template_id.clone(), t)).collect();

        Self { templates: templates_map, template_order, tokens_by_template, automaton, pattern_owner }
    }

    /// template_id -> {frag_idx -> [(start, end_exclusive), ...]}
    fn fragment_hits(&self, text: &str) -> HashMap<String, HashMap<usize, Vec<(usize, usize)>>> {
        let mut hits: HashMap<String, HashMap<usize, Vec<(usize, usize)>>> = HashMap::new();
        let Some(automaton) = &self.automaton else { return hits };
        for m in automaton.find_overlapping_iter(text) {
            let (template_id, frag_idx) = &self.pattern_owner[m.pattern().as_usize()];
            hits.entry(template_id.clone()).or_default().entry(*frag_idx).or_default().push((m.start(), m.end()));
        }
        hits
    }

    /// Жадно выбирает по одному вхождению на фрагмент, монотонно возрастающему
    /// по позиции. `None`, если такой последовательности не существует.
    fn candidate_positions(
        &self,
        template_id: &str,
        hits: &HashMap<String, HashMap<usize, Vec<(usize, usize)>>>,
        frag_count: usize,
    ) -> Option<Vec<(usize, usize)>> {
        let per_frag = hits.get(template_id);
        let mut chosen = Vec::with_capacity(frag_count);
        let mut cursor = 0usize;
        for idx in 0..frag_count {
            let mut candidates: Vec<(usize, usize)> =
                per_frag.and_then(|m| m.get(&idx)).cloned().unwrap_or_default();
            candidates.sort_unstable();
            let picked = candidates.into_iter().find(|c| c.0 >= cursor)?;
            cursor = picked.1;
            chosen.push(picked);
        }
        Some(chosen)
    }

    fn check_placeholder_region(region: &str, placeholder: &Placeholder) -> bool {
        match placeholder {
            Placeholder::Word => !region.is_empty() && !region.chars().any(char::is_whitespace),
            Placeholder::Digit { min, max } => {
                let non_digit_non_separator = region
                    .chars()
                    .any(|c| !c.is_ascii_digit() && !c.is_whitespace() && !"-_.".contains(c));
                if non_digit_non_separator {
                    return false;
                }
                let digit_count = region.chars().filter(|c| c.is_ascii_digit()).count();
                *min <= digit_count && digit_count <= *max
            }
        }
    }

    pub fn find_match(&self, text: &str) -> Option<MatchedTemplate> {
        let hits = self.fragment_hits(text);

        for template_id in &self.template_order {
            let tokens = &self.tokens_by_template[template_id];
            let frags = literal_fragments(tokens);
            if frags.is_empty() {
                continue;
            }
            let Some(positions) = self.candidate_positions(template_id, &hits, frags.len()) else { continue };

            let mut ok = true;
            let mut frag_cursor = 0usize;
            let mut prev_end = 0usize;
            for (tok_idx, tok) in tokens.iter().enumerate() {
                let Token::Literal(literal) = tok else { continue };
                if literal.is_empty() {
                    continue;
                }
                let (start, end) = positions[frag_cursor];
                if tok_idx > 0 {
                    if let Token::Placeholder(p) = &tokens[tok_idx - 1] {
                        let region = &text[prev_end..start];
                        if !Self::check_placeholder_region(region, p) {
                            ok = false;
                            break;
                        }
                    }
                }
                prev_end = end;
                frag_cursor += 1;
            }
            if ok {
                if let Some(Token::Literal(last)) = tokens.last() {
                    if last.is_empty() && tokens.len() > 1 {
                        if let Token::Placeholder(p) = &tokens[tokens.len() - 2] {
                            let region = &text[prev_end..];
                            if !Self::check_placeholder_region(region, p) {
                                ok = false;
                            }
                        }
                    }
                }
            }

            if ok {
                let t = &self.templates[template_id];
                return Some(MatchedTemplate { template_id: t.template_id.clone(), category: t.category.clone() });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_template() -> Template {
        Template {
            template_id: "tpl-contract-payment".to_string(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".to_string(),
            category: "TRANSACTION".to_string(),
        }
    }

    #[test]
    fn real_example_matches() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring";
        let result = ruleset.find_match(text);
        assert!(result.is_some(), "должно было смачиться — это ровно пример из чата");
        assert_eq!(result.unwrap().category, "TRANSACTION");
    }

    #[test]
    fn digit_count_out_of_range_no_match() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 7 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text).is_none(), "7 цифр вне {{1,6}} — не должно матчиться");
    }

    #[test]
    fn word_placeholder_rejects_internal_whitespace() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello world shartnoma bo'yicha 123 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text).is_none());
    }

    #[test]
    fn missing_literal_fragment_no_match() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238 completely different text with no template fragments at all";
        assert!(ruleset.find_match(text).is_none());
    }

    #[test]
    fn digits_with_separators_counted_correctly() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "ABC123 shartnoma bo'yicha 12-34-56 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text).is_some(), "разделители между цифрами должны игнорироваться");
    }

    #[test]
    fn multiple_candidates_resolved_deterministically() {
        let tpl_word = Template { template_id: "tpl-word".into(), pattern: "code: %w".into(), category: "SERVICE".into() };
        let tpl_digit = Template { template_id: "tpl-digit".into(), pattern: "code: %d{4,4}".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl_word, tpl_digit]);

        let result_digit_text = ruleset.find_match("code: 1234");
        assert!(result_digit_text.is_some());

        let result_word_only_text = ruleset.find_match("code: ABCD");
        assert_eq!(result_word_only_text.unwrap().template_id, "tpl-word", "ABCD не цифры — только tpl-word должен пройти");
    }

    #[test]
    fn leading_and_trailing_placeholder() {
        let tpl = Template { template_id: "tpl-edges".into(), pattern: "%d{2,2}-ok-%w".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("42-ok-done").is_some());
        assert!(ruleset.find_match("4-ok-done").is_none(), "только 1 цифра, нужно ровно 2");
        assert!(ruleset.find_match("42-ok-").is_none(), "пустой %w в конце недопустим");
    }
}
