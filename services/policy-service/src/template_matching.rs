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

/// Минимальная длина литерального фрагмента (в символах, не байтах — иначе
/// граница непредсказуемо смещается в зависимости от алфавита), ниже
/// которой фрагмент считается слабым якорем для Aho-Corasick-префильтра.
const MIN_SELECTIVE_FRAGMENT_LEN: usize = 3;

/// Pre-flight-проверка одного pattern перед регистрацией шаблона в
/// конфиге — найдено при обсуждении производительности `CompiledRuleset`
/// при большом числе шаблонов на одного отправителя: все литеральные
/// фрагменты всех шаблонов лежат в одном общем автомате (`fragment_hits`),
/// поэтому короткий/частый фрагмент одного шаблона даёт совпадения почти
/// на любом сообщении и раздувает список кандидатов во второй фазе
/// (`find_match`) для ВСЕХ шаблонов реестра, не только для этого. Ловит
/// два практических случая до того, как шаблон попадёт в конфиг:
///   1. Ни одного литерального фрагмента вообще — при текущей реализации
///      такой шаблон не может совпасть никогда (`check_template`
///      безусловно отклоняет пустой `literal_fragments`).
///   2. Хотя бы один литеральный фрагмент короче `MIN_SELECTIVE_FRAGMENT_LEN`.
/// Намеренно возвращает предупреждения, а не `Result`/ошибку — у этой
/// функции нет доступа к остальному реестру шаблонов (не может знать,
/// станет ли фрагмент реальной проблемой при данном конкретном наборе
/// шаблонов сендера), поэтому решение "заблокировать/подтвердить/
/// проигнорировать" остаётся на вызывающей стороне.
pub fn check_pattern_selectivity(pattern: &str) -> Vec<String> {
    let tokens = parse_pattern(pattern);
    let frags = literal_fragments(&tokens);

    if frags.is_empty() {
        return vec![
            "шаблон не содержит ни одного литерального фрагмента — при текущей реализации \
             (Aho-Corasick-префильтр по литералам) такой шаблон не сможет совпасть ни с одним \
             сообщением, независимо от плейсхолдеров"
                .to_string(),
        ];
    }

    frags
        .iter()
        .filter(|f| f.chars().count() < MIN_SELECTIVE_FRAGMENT_LEN)
        .map(|f| {
            format!(
                "литеральный фрагмент {f:?} короче {MIN_SELECTIVE_FRAGMENT_LEN} символов — слабый \
                 якорь для префильтра: чем короче и чаще встречается фрагмент, тем больше шаблонов \
                 попадёт в кандидаты на каждое сообщение и тем медленнее фаза подтверждения для ВСЕХ \
                 шаблонов реестра, не только этого"
            )
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

/// Плотный внутренний индекс шаблона (0..N по порядку регистрации, N —
/// число загруженных шаблонов). Найдено кодревью: строковый `template_id`
/// (обычно UUID, ~36 байт) в роли ключа `HashMap` и элемента `hits` — это
/// хэширование и сравнение байт строки плюс клонирование `String` на
/// каждый лукап и на каждый хит префильтра; при большом числе шаблонов на
/// одного отправителя это реальные накладные расходы и по CPU, и по
/// памяти. `TemplateId` — просто позиция в `templates`/`tokens_by_template`
/// (см. `CompiledRuleset::new`), `Copy`, лукап — прямая индексация массива
/// без хэширования. Заодно бесплатно даёт insertion order: меньший
/// `TemplateId` зарегистрирован раньше, тай-брейк в `find_match` сравнивает
/// `TemplateId` напрямую — отдельная таблица индексов не нужна. Публичный
/// `template_id: String` остаётся только в `Template` (вход) и
/// `MatchedTemplate` (выход) — во внутренней бухгалтерии его больше нет.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TemplateId(u32);

impl TemplateId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

pub struct CompiledRuleset {
    templates: Vec<Template>,                // индекс — TemplateId
    tokens_by_template: Vec<Vec<Token>>,      // индекс — TemplateId, параллельно templates
    automaton: Option<AhoCorasick>,
    pattern_owner: Vec<(TemplateId, usize)>,  // PatternID (индекс автомата) -> (template, frag_idx)
}

impl CompiledRuleset {
    pub fn new(templates: Vec<Template>) -> Self {
        let mut tokens_by_template: Vec<Vec<Token>> = Vec::with_capacity(templates.len());
        let mut patterns: Vec<String> = Vec::new();
        let mut pattern_owner: Vec<(TemplateId, usize)> = Vec::new();

        for (order, t) in templates.iter().enumerate() {
            let template_id = TemplateId(order as u32);
            let tokens = parse_pattern(&t.pattern);
            let frags: Vec<String> = literal_fragments(&tokens).into_iter().map(String::from).collect();
            for (idx, frag) in frags.iter().enumerate() {
                patterns.push(frag.clone());
                pattern_owner.push((template_id, idx));
            }
            tokens_by_template.push(tokens);
        }

        let automaton = if patterns.is_empty() { None } else { Some(AhoCorasick::new(&patterns).expect("valid patterns")) };

        Self { templates, tokens_by_template, automaton, pattern_owner }
    }

    /// template (TemplateId) -> {frag_idx -> [(start, end_exclusive), ...]}
    fn fragment_hits(&self, text: &str) -> HashMap<TemplateId, HashMap<usize, Vec<(usize, usize)>>> {
        let mut hits: HashMap<TemplateId, HashMap<usize, Vec<(usize, usize)>>> = HashMap::new();
        let Some(automaton) = &self.automaton else { return hits };
        for m in automaton.find_overlapping_iter(text) {
            let (template_id, frag_idx) = self.pattern_owner[m.pattern().as_usize()];
            hits.entry(template_id).or_default().entry(frag_idx).or_default().push((m.start(), m.end()));
        }
        hits
    }

    /// Жадно выбирает по одному вхождению на фрагмент, монотонно возрастающему
    /// по позиции. `None`, если такой последовательности не существует.
    fn candidate_positions(
        &self,
        template_id: TemplateId,
        hits: &HashMap<TemplateId, HashMap<usize, Vec<(usize, usize)>>>,
        frag_count: usize,
    ) -> Option<Vec<(usize, usize)>> {
        let per_frag = hits.get(&template_id);
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
    fn check_template(&self, template_id: TemplateId, hits: &HashMap<TemplateId, HashMap<usize, Vec<(usize, usize)>>>, text: &str) -> bool {
        let tokens = &self.tokens_by_template[template_id.index()];
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

    /// Найдено кодревью: раньше цикл шёл по `template_order` — ВСЕМ
    /// зарегистрированным шаблонам, независимо от того, нашёл ли для них
    /// Aho-Corasick хоть один литеральный фрагмент в `text`. При миллионе
    /// шаблонов на одного отправителя это O(число_шаблонов) на каждое
    /// входящее сообщение — асимптотика Aho-Corasick (`O(длина_текста +
    /// совпадения)`, не зависит от числа загруженных шаблонов) фактически
    /// терялась на этом шаге. Теперь перебор идёт по `hits.keys()` — это и
    /// есть настоящий список кандидатов после префильтра, обычно
    /// единицы-десятки записей независимо от общего числа шаблонов.
    /// Tie-break по insertion order сравнивает `TemplateId` напрямую (сам
    /// индекс и есть порядок регистрации, см. `CompiledRuleset::new`) — без
    /// отдельного лукапа. Сравнение явное, не через порядок обхода: обход
    /// `HashMap::keys()`, в отличие от прежнего обхода `Vec` по возрастанию
    /// индекса, не детерминирован, и раньше подразумеваемая гарантия "при
    /// равенстве специфичности победит первый встреченный" держалась только
    /// на порядке обхода `Vec` — здесь она выражена явным сравнением, а не
    /// порядком итерации.
    pub fn find_match(&self, text: &str) -> Option<MatchedTemplate> {
        let hits = self.fragment_hits(text);

        let mut best: Option<(TemplateId, (usize, usize))> = None; // (template_id, specificity) — TemplateId сам по себе insertion order
        for &template_id in hits.keys() {
            if !self.check_template(template_id, &hits, text) {
                continue;
            }
            let score = Self::specificity(&self.tokens_by_template[template_id.index()]);
            let is_better = match &best {
                None => true,
                Some((best_id, best_score)) => {
                    score > *best_score || (score == *best_score && template_id < *best_id)
                }
            };
            if is_better {
                best = Some((template_id, score));
            }
        }

        best.map(|(template_id, _)| {
            let t = &self.templates[template_id.index()];
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

    // Регрессия на находку кодревью: `find_match` раньше шёл циклом по ВСЕМ
    // зарегистрированным шаблонам (`template_order`), а не только по
    // кандидатам префильтра (`hits`) — при большом числе шаблонов на одного
    // отправителя это O(число_шаблонов) на каждое сообщение вместо
    // O(длина_текста), который и обещает Aho-Corasick. Ни один из
    // "посторонних" шаблонов ниже не имеет ни одного общего литерального
    // фрагмента с проверяемым текстом — если бы перебор снова начал идти по
    // всем шаблонам подряд, а не по `hits.keys()`, этот тест остался бы
    // корректным (результат не поменялся бы), но перестал бы быть дешёвым —
    // сам факт, что 20 000 шаблонов ни разу не проверяются `check_template`,
    // здесь не проверяется явно (это деталь реализации, не наблюдаемое
    // поведение), но реальный correctness-инвариант — "посторонние шаблоны
    // не влияют на результат и не мешают найти совпадение" — тестируется.
    #[test]
    fn unrelated_templates_at_scale_do_not_prevent_or_corrupt_the_real_match() {
        let mut templates: Vec<Template> = (0..20_000)
            .map(|i| Template {
                template_id: format!("unrelated-{i}"),
                pattern: format!("совершенно другой текст номер {i} без общих слов"),
                category: "OTHER".into(),
            })
            .collect();
        templates.push(Template {
            template_id: "tpl-real".into(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".into(),
            category: "TRANSACTION".into(),
        });

        let ruleset = CompiledRuleset::new(templates);
        let result = ruleset.find_match("Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring");
        assert_eq!(result.unwrap().template_id, "tpl-real", "20 000 не связанных шаблонов не должны мешать найти реальное совпадение");
    }

    #[test]
    fn selectivity_warns_on_pattern_with_no_literal_content() {
        let warnings = check_pattern_selectivity("%w");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("не содержит ни одного литерального фрагмента"));
    }

    #[test]
    fn selectivity_warns_on_short_literal_fragment() {
        // Литеральный фрагмент "ab" — ровно 2 символа, короче порога в 3.
        let warnings = check_pattern_selectivity("%d{1,1}ab");
        assert_eq!(warnings.len(), 1, "\"ab\" короче MIN_SELECTIVE_FRAGMENT_LEN");
    }

    #[test]
    fn selectivity_counts_characters_not_bytes_for_multibyte_literals() {
        // "ок" — кириллица, 2 символа, но 4 байта в UTF-8. Порог должен
        // сработать по числу символов (2 < 3), а не байтов (4 не < 3) —
        // иначе граница непредсказуемо смещалась бы в зависимости от алфавита.
        let warnings = check_pattern_selectivity("%d{1,1}ок");
        assert_eq!(warnings.len(), 1, "2 символа короче порога независимо от того, что это 4 байта");
    }

    #[test]
    fn selectivity_no_warnings_for_real_template_pattern() {
        let warnings = check_pattern_selectivity("%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring");
        assert!(warnings.is_empty(), "все литеральные фрагменты этого шаблона длиннее порога: {warnings:?}");
    }
}
