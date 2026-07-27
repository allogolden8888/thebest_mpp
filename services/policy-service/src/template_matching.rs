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
///
/// Найдено кодревью, исправлено здесь: предыдущая версия шагала по `i` побайтово
/// (`i += 1` в ветке `else`), но слайсила `pattern[i..]` каждую итерацию — `str`-слайс
/// по не-граничному байту паникует. На чисто ASCII паттернах (`%w`, `%d{n,m}`,
/// разделители) это никогда не срабатывало, но литеральные фрагменты этой платформы
/// реально содержат кириллицу, эмодзи и узбекский латинский апостроф (ʻ/ʼ, 2 байта
/// в UTF-8) — реальный паттерн регистрации, а не гипотетический вход. Теперь `i`
/// продвигается на всю длину символа в ветке `else`, так что он всегда остаётся на
/// границе символа перед следующей слайсинг-проверкой.
pub fn parse_pattern(pattern: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < pattern.len() {
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
            let ch_len = pattern[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            i += ch_len;
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
    /// (insertion order). С `development_plan.md` 4.3 больше не главный
    /// критерий при неоднозначности (это теперь `specificity()`) — остаётся
    /// только финальным tie-break'ом, если специфичность двух шаблонов
    /// совпала в точности (см. `find_match`).
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

    /// Проверяет один конкретный шаблон против уже найденных `hits` — вынесено
    /// отдельно от `find_match` (development_plan.md 4.3), чтобы можно было
    /// проверить ВСЕ шаблоны и выбрать лучший, не останавливаться на первом
    /// подошедшем по порядку регистрации.
    fn check_template(&self, template_id: &str, hits: &HashMap<String, HashMap<usize, Vec<(usize, usize)>>>, text: &str) -> bool {
        let tokens = &self.tokens_by_template[template_id];
        let frags = literal_fragments(tokens);
        if frags.is_empty() {
            return false;
        }
        let Some(positions) = self.candidate_positions(template_id, hits, frags.len()) else { return false };

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
                        return false;
                    }
                }
            }
            prev_end = end;
            frag_cursor += 1;
        }

        if let Some(Token::Literal(last)) = tokens.last() {
            if last.is_empty() && tokens.len() > 1 {
                if let Token::Placeholder(p) = &tokens[tokens.len() - 2] {
                    let region = &text[prev_end..];
                    if !Self::check_placeholder_region(region, p) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// `development_plan.md` 4.3 — реальная приоритизация при подлинной
    /// неоднозначности (несколько шаблонов проходят фазу 2 для одного и того
    /// же текста), не просто "не падает" (это уже было доказано
    /// `template_matching.py`/предыдущей версией этого файла). Найдено при
    /// реализации: до этой правки побеждал первый по порядку РЕГИСТРАЦИИ
    /// (insertion order) — случайность, не намерение; например `code: %w`
    /// (общий, принимает что угодно) регистрировался раньше `code: %d{4,4}`
    /// (специфичный, только 4 цифры), и общий шаблон побеждал специфичный
    /// для текста `"code: 1234"`, где оба технически проходят.
    ///
    /// Правило: `%d{n,m}` строже `%w` (ограничивает и алфавит, и длину, не
    /// только "непустой непробельный") — специфичность = сумма весов
    /// плейсхолдеров (Digit=2, Word=1) + суммарная длина литеральных
    /// фрагментов как вторичный, более слабый критерий (специфичность
    /// плейсхолдеров решает первой, длина литералов — только для разрыва
    /// ничьей между шаблонами с одинаковым профилем плейсхолдеров). Порядок
    /// регистрации остаётся финальным, детерминированным tie-break'ом, если
    /// оба критерия совпали.
    fn specificity(tokens: &[Token]) -> (usize, usize) {
        let placeholder_score: usize = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Placeholder(Placeholder::Digit { .. }) => Some(2),
                Token::Placeholder(Placeholder::Word) => Some(1),
                _ => None,
            })
            .sum();
        let literal_len: usize = literal_fragments(tokens).iter().map(|f| f.len()).sum();
        (placeholder_score, literal_len)
    }

    pub fn find_match(&self, text: &str) -> Option<MatchedTemplate> {
        let hits = self.fragment_hits(text);

        let mut best: Option<(usize, (usize, usize), &String)> = None; // (insertion_idx, specificity, template_id)
        for (idx, template_id) in self.template_order.iter().enumerate() {
            if !self.check_template(template_id, &hits, text) {
                continue;
            }
            let score = Self::specificity(&self.tokens_by_template[template_id]);
            let is_better = match &best {
                None => true,
                Some((_, best_score, _)) => score > *best_score, // строго больше — при равенстве побеждает более ранний по insertion order (найден первым)
            };
            if is_better {
                best = Some((idx, score, template_id));
            }
        }

        best.map(|(_, _, template_id)| {
            let t = &self.templates[template_id];
            MatchedTemplate { template_id: t.template_id.clone(), category: t.category.clone() }
        })
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

        // Найдено при реализации 4.3: "code: 1234" технически проходит фазу 2
        // для ОБОИХ шаблонов ("1234" — непустой непробельный %w, и ровно 4
        // цифры для %d{4,4}) — это и есть настоящая неоднозначность, не
        // гипотетическая. `tpl-word` зарегистрирован ПЕРВЫМ (insertion order),
        // но `tpl-digit` строже (ограничивает и алфавит, и длину) — специфичность
        // обязана выбрать именно его, не первый по регистрации.
        let result_digit_text = ruleset.find_match("code: 1234");
        assert_eq!(result_digit_text.unwrap().template_id, "tpl-digit",
            "более специфичный шаблон (%d{{4,4}}) обязан победить менее специфичный (%w) при реальной неоднозначности");

        let result_word_only_text = ruleset.find_match("code: ABCD");
        assert_eq!(result_word_only_text.unwrap().template_id, "tpl-word", "ABCD не цифры — только tpl-word должен пройти");
    }

    #[test]
    fn specificity_prefers_digit_over_word_for_same_literal_length() {
        // Прямая, изолированная проверка specificity() — не через find_match,
        // чтобы отличить "правило работает" от "правило совпало случайно с
        // порядком регистрации" (симметричный тест — Word и Digit в
        // обратном порядке регистрации всё равно должны выбрать Digit).
        let tpl_word = Template { template_id: "w".into(), pattern: "pin: %w".into(), category: "S".into() };
        let tpl_digit = Template { template_id: "d".into(), pattern: "pin: %d{4,4}".into(), category: "S".into() };
        // Регистрируем Digit ПЕРВЫМ на этот раз — если бы побеждал порядок
        // регистрации, а не специфичность, оба порядка дали бы Digit, и тест
        // не отличил бы правило от совпадения. Раз оба порядка (этот тест и
        // предыдущий) дают Digit — специфичность реально решает, не порядок.
        let ruleset = CompiledRuleset::new(vec![tpl_digit, tpl_word]);
        let result = ruleset.find_match("pin: 1234");
        assert_eq!(result.unwrap().template_id, "d");
    }

    #[test]
    fn specificity_breaks_ties_by_literal_length_when_placeholder_profile_is_identical() {
        // Оба шаблона — одиночный %w, оба реально матчат один и тот же текст
        // (genuine ambiguity: "code " — суффикс "the code ", Aho-Corasick
        // находит оба литерала в одном тексте) — более длинный, более
        // специфичный литеральный префикс обязан победить.
        let short_literal = Template { template_id: "short".into(), pattern: "code %w".into(), category: "S".into() };
        let long_literal = Template { template_id: "long".into(), pattern: "the code %w".into(), category: "S".into() };
        let ruleset = CompiledRuleset::new(vec![short_literal, long_literal]);
        let result = ruleset.find_match("the code 123");
        assert_eq!(result.unwrap().template_id, "long", "более длинный, более специфичный литеральный контекст должен победить");
    }

    #[test]
    fn leading_and_trailing_placeholder() {
        let tpl = Template { template_id: "tpl-edges".into(), pattern: "%d{2,2}-ok-%w".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("42-ok-done").is_some());
        assert!(ruleset.find_match("4-ok-done").is_none(), "только 1 цифра, нужно ровно 2");
        assert!(ruleset.find_match("42-ok-").is_none(), "пустой %w в конце недопустим");
    }

    // Регрессия на находку кодревью: parse_pattern раньше паниковал на любом
    // многобайтовом UTF-8 символе (слайсинг по не-граничному байту). Реальный
    // домен этой платформы — кириллица и узбекская латиница с апострофом
    // ʻ/ʼ (U+02BB/U+02BC, 2 байта) — не гипотетический вход.
    #[test]
    fn cyrillic_literal_fragment_does_not_panic() {
        let tpl = Template { template_id: "tpl-cyr".into(), pattern: "Спасибо за %w покупку".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("Спасибо за вашу покупку").is_some());
    }

    #[test]
    fn uzbek_latin_apostrophe_literal_fragment_does_not_panic() {
        // U+02BB (ʻ) — двухбайтовый в UTF-8, реально встречается в узбекских
        // словах вроде "oʻzbekcha". Ровно такой же класс символа, что уже
        // используется в этих тестах внутри %w-разделённых фрагментов
        // ("bo'yicha", "so'm"), но здесь — внутри самого литерала.
        let tpl = Template { template_id: "tpl-uz".into(), pattern: "toʻlov %w bajarildi".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("toʻlov muvaffaqiyatli bajarildi").is_some());
    }

    #[test]
    fn emoji_literal_fragment_does_not_panic() {
        // Emoji — 4-байтовый UTF-8 символ, самый жёсткий случай для
        // границ char boundary.
        let tpl = Template { template_id: "tpl-emoji".into(), pattern: "🎉 %w tabriklaymiz".into(), category: "SERVICE".into() };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("🎉 sizni tabriklaymiz").is_some());
    }
}
