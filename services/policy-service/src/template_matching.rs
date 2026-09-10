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
//!   %{name} — Экран 36 "Pattern Placeholders" (luminous-hugging-charm.md):
//!             именованный плейсхолдер, `name` резолвится в regex из живого
//!             реестра `PlaceholderRegistry` (config.changes, entity_type=
//!             CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER, см. config_reload.rs).
//!             В отличие от `%w`/`%d{n,m}` — именованный, конфигурируемый
//!             через бэкофис (не hardcoded здесь) и ИЗВЛЕКАЮЩИЙ: подошедшая
//!             подстрока попадает в `MatchedTemplate::extracted_values`.
//!             `name` обязан совпадать с `^[a-z][a-z0-9_]*$`
//!             (config_schemas/pattern_placeholder.schema.json) — то же
//!             ограничение, что уже применяется к самому реестру.

use aho_corasick::AhoCorasick;
use regex::Regex;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placeholder {
    Word,
    Digit { min: usize, max: usize },
    /// `%{name}` — Экран 36. Само значение `name` без ведущего/замыкающего
    /// синтаксиса, регекс резолвится позже, при матчинге, через
    /// `PlaceholderRegistry` (не здесь — `Token`/`Placeholder` не имеют
    /// доступа к живому реестру, только к синтаксису pattern).
    Named(String),
}

/// Имя внутри `%{name}` обязано совпадать с
/// `config_schemas/pattern_placeholder.schema.json` (`^[a-z][a-z0-9_]*$`) —
/// тем же ограничением, что уже применяется к `name` самого реестра. Ручной
/// ascii-цикл, а не `regex`-крейт: это парсер САМОГО СИНТАКСИСА pattern
/// (`parse_pattern`), а не матчинг против содержимого сообщения — тянуть
/// regex ради одной простой проверки формы не нужно.
fn is_valid_placeholder_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
        } else if pattern[i..].starts_with("%{") {
            // Тот же принцип отказа, что у %d{...} выше: незакрытая скобка
            // или невалидное имя — не ошибка парсинга всего pattern, просто
            // "%{" остаётся частью литерала (i продвигается на 1 байт, '%' —
            // ASCII, граница символа не нарушается) и цикл продолжает поиск
            // синтаксиса дальше по строке.
            if let Some(close) = pattern[i..].find('}') {
                let name = &pattern[i + 2..i + close];
                if is_valid_placeholder_name(name) {
                    tokens.push(Token::Literal(pattern[pos..i].to_string()));
                    tokens.push(Token::Placeholder(Placeholder::Named(name.to_string())));
                    i += close + 1;
                    pos = i;
                    continue;
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
    /// `None` — шаблон применим к любому отправителю партнёра (поведение по
    /// умолчанию, сохраняющее сегодняшнее неявное поведение). `Some(id)` —
    /// шаблон применим ТОЛЬКО к сообщениям с ровно этим `sender_id`
    /// (Фаза 2 плана, `policy.policy_template.sender_id`, nullable —
    /// null означает "все отправители партнёра", по аналогии с nullable
    /// `operator_id`).
    pub sender_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedTemplate {
    pub template_id: String,
    pub category: String,
    /// Экран 36 "Pattern Placeholders" — значения именованных плейсхолдеров
    /// `%{name}`, реально извлечённые из текста победившим шаблоном (name ->
    /// подстрока региона, удовлетворившая regex этого имени).
    ///
    /// ВАЖНО (сознательная граница объёма этого захода, luminous-hugging-
    /// charm.md): это поле СЧИТАЕТСЯ и доступно на результате матчинга, но
    /// НИКУДА дальше по пайплайну не прокидывается — ни в
    /// `StageCompletedEvent`/`stage_contract.proto`, ни в analytics-writer,
    /// ни куда-либо ещё. Прокидывание извлечённого значения дальше — ОТДЕЛЬНОЕ
    /// архитектурное решение (какой именно потребитель, зачем ему это поле,
    /// как это меняет публичный контракт стадии policy) и должно быть
    /// осознанно подтверждено пользователем, не решено молча в рамках этого
    /// изменения движка матчинга. Сегодня используется только для
    /// отладочной видимости — логируется в `kafka_io.rs`.
    pub extracted_values: HashMap<String, String>,
}

/// Живой реестр именованных плейсхолдеров `%{name}` (Экран 36 "Pattern
/// Placeholders", `config_schemas/pattern_placeholder.schema.json`) —
/// `name -> скомпилированный regex`. Собирается `config_reload.rs` из
/// `config.changes` (entity_type=CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER) и
/// живёт внутри `PolicyLiveState` рядом с `CompiledRuleset` — та же
/// ArcSwap-схема hot-reload, что уже была подключена для ruleset/шаблонов.
///
/// Каждый пользовательский regex оборачивается в `^(?:...)$` ПРИ
/// КОМПИЛЯЦИИ (см. `new`) — плейсхолдер обязан описывать форму ВСЕГО
/// региона между соседними литералами, не "где-то внутри есть совпадение".
/// Тот же принцип, что уже применяется к `%w`/`%d{n,m}` в
/// `check_placeholder_region` (region целиком, не подстрока) — без анкоринга
/// admin, вписавший `\d{4}` рассчитывая на "ровно 4 цифры", неожиданно
/// получил бы совпадение и на "12345".
///
/// Невалидный regex в конфиге — не паника и не отказ всего реестра: запись
/// пропускается с `tracing::error!`, при матчинге это неотличимо от
/// отсутствующего `name` (см. `check_placeholder_region` — fail closed).
#[derive(Debug, Clone, Default)]
pub struct PlaceholderRegistry {
    compiled: HashMap<String, Regex>,
}

impl PlaceholderRegistry {
    pub fn new(defs: &HashMap<String, String>) -> Self {
        let mut compiled = HashMap::with_capacity(defs.len());
        for (name, pattern) in defs {
            match Regex::new(&format!("^(?:{pattern})$")) {
                Ok(re) => {
                    compiled.insert(name.clone(), re);
                }
                Err(e) => tracing::error!(
                    "pattern_placeholder '{name}': невалидный regex {pattern:?}, запись проигнорирована ({e}) — \
                     любой %{{{name}}} в шаблонах не будет матчиться, пока конфиг не исправят (fail closed)"
                ),
            }
        }
        Self { compiled }
    }

    fn get(&self, name: &str) -> Option<&Regex> {
        self.compiled.get(name)
    }
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
    sender_id_by_template: Vec<Option<String>>, // индекс — TemplateId, параллельно templates (Фаза 2 — sender-scoping)
    automaton: Option<AhoCorasick>,
    pattern_owner: Vec<(TemplateId, usize)>,  // PatternID (индекс автомата) -> (template, frag_idx)
}

impl CompiledRuleset {
    pub fn new(templates: Vec<Template>) -> Self {
        let mut tokens_by_template: Vec<Vec<Token>> = Vec::with_capacity(templates.len());
        let mut sender_id_by_template: Vec<Option<String>> = Vec::with_capacity(templates.len());
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
            sender_id_by_template.push(t.sender_id.clone());
        }

        let automaton = if patterns.is_empty() { None } else { Some(AhoCorasick::new(&patterns).expect("valid patterns")) };

        Self { templates, tokens_by_template, sender_id_by_template, automaton, pattern_owner }
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

    /// Возвращает `None`, если регион не подходит под плейсхолдер (структурная
    /// проверка `%w`/`%d{n,m}` — как и раньше — или значение не совпало с
    /// regex именованного `%{name}`, либо `name` вообще не найдено в
    /// `registry` — намеренно fail closed: конфиг реестра может отставать
    /// или быть неполным, лучше не смачить, чем смачить непроверенным
    /// значением). `Some(None)` — регион подошёл, извлекать нечего (`%w`/
    /// `%d{n,m}` структурны, не именованы). `Some(Some((name, value)))` —
    /// регион подошёл под именованный плейсхолдер, `value` — реально
    /// извлечённая подстрока.
    fn check_placeholder_region(
        region: &str,
        placeholder: &Placeholder,
        registry: &PlaceholderRegistry,
    ) -> Option<Option<(String, String)>> {
        match placeholder {
            Placeholder::Word => {
                let ok = !region.is_empty() && !region.chars().any(char::is_whitespace);
                ok.then_some(None)
            }
            Placeholder::Digit { min, max } => {
                let non_digit_non_separator = region
                    .chars()
                    .any(|c| !c.is_ascii_digit() && !c.is_whitespace() && !"-_.".contains(c));
                if non_digit_non_separator {
                    return None;
                }
                let digit_count = region.chars().filter(|c| c.is_ascii_digit()).count();
                (*min <= digit_count && digit_count <= *max).then_some(None)
            }
            Placeholder::Named(name) => {
                // `registry.get` уже отсутствует и для несуществующего
                // имени, и для имени с невалидным regex (`PlaceholderRegistry::new`
                // пропускает такие записи при сборке) — оба случая здесь
                // неотличимы и оба обязаны провалить матчинг, не паниковать.
                let re = registry.get(name)?;
                re.is_match(region).then(|| Some((name.clone(), region.to_string())))
            }
        }
    }

    /// Проверяет один конкретный шаблон против уже найденных `hits` — вынесено
    /// отдельно от `find_match` (development_plan.md 4.3), чтобы можно было
    /// проверить ВСЕ шаблоны и выбрать лучший, не останавливаться на первом
    /// подошедшем по порядку регистрации.
    ///
    /// Возвращает `None` при провале (как раньше `false`) или
    /// `Some(извлечённые_значения)` при успехе — Экран 36: значения именованных
    /// плейсхолдеров `%{name}`, накопленные по ходу проверки регионов между
    /// литералами (для `%w`/`%d{n,m}` карта остаётся пустой).
    fn check_template(
        &self,
        template_id: TemplateId,
        hits: &HashMap<TemplateId, HashMap<usize, Vec<(usize, usize)>>>,
        text: &str,
        registry: &PlaceholderRegistry,
    ) -> Option<HashMap<String, String>> {
        let tokens = &self.tokens_by_template[template_id.index()];
        let frags = literal_fragments(tokens);
        if frags.is_empty() {
            return None;
        }
        let positions = self.candidate_positions(template_id, hits, frags.len())?;

        let mut extracted = HashMap::new();
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
                    match Self::check_placeholder_region(region, p, registry) {
                        None => return None,
                        Some(Some((name, value))) => {
                            extracted.insert(name, value);
                        }
                        Some(None) => {}
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
                    match Self::check_placeholder_region(region, p, registry) {
                        None => return None,
                        Some(Some((name, value))) => {
                            extracted.insert(name, value);
                        }
                        Some(None) => {}
                    }
                }
            }
        }
        Some(extracted)
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
    /// плейсхолдеров (Named=3, Digit=2, Word=1) + суммарная длина литеральных
    /// фрагментов как вторичный, более слабый критерий (специфичность
    /// плейсхолдеров решает первой, длина литералов — только для разрыва
    /// ничьей между шаблонами с одинаковым профилем плейсхолдеров). Порядок
    /// регистрации остаётся финальным, детерминированным tie-break'ом, если
    /// оба критерия совпали.
    ///
    /// `Named` (Экран 36, `%{name}`) весит СТРОЖЕ `Digit` — в отличие от
    /// `%w`/`%d{n,m}` (фиксированная, hardcoded форма), regex `%{name}`
    /// admin-задан явно под конкретный случай (например, ровно 6-значный
    /// OTP, а не "любые 1-6 цифр") — по построению это как минимум не менее
    /// специфичная форма, чем встроенные плейсхолдеры этого движка.
    fn specificity(tokens: &[Token]) -> (usize, usize) {
        let placeholder_score: usize = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Placeholder(Placeholder::Named(_)) => Some(3),
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
    ///
    /// Фаза 2 (`sender_id` в модели шаблонов) — `sender_id` сужает кандидатов
    /// до шаблонов, применимых к ЭТОМУ отправителю: шаблон с
    /// `Template.sender_id == None` подходит любому отправителю (сегодняшнее
    /// поведение по умолчанию), шаблон с `Some(id)` — только отправителю с
    /// ровно этим `sender_id`. Проверка — дешёвое сравнение `Option<String>`,
    /// сделанное ВНУТРИ цикла по `hits.keys()` (то есть только для
    /// кандидатов, уже прошедших Aho-Corasick-префильтр), а не отдельным
    /// проходом по всем зарегистрированным шаблонам — иначе это вернуло бы
    /// ровно ту O(число_шаблонов) регрессию, которая была найдена кодревью и
    /// исправлена переходом на `hits.keys()` (см. комментарий выше). Фильтр
    /// применяется ДО сравнения специфичности — шаблон с несовпадающим
    /// sender_id не участвует в тай-брейке вообще, независимо от того,
    /// насколько он специфичнее.
    /// Совместимая обёртка над `find_match_with_placeholders` для вызывающих,
    /// которым не нужны именованные плейсхолдеры Экрана 36 (весь
    /// предсуществующий набор тестов в этом файле — `%w`/`%d{n,m}` подряд,
    /// ни один не ссылается на `%{name}`). Пустой `PlaceholderRegistry`
    /// эквивалентен полному отсутствию registry: любой `%{name}` в pattern
    /// просто никогда не смачится (fail closed, см.
    /// `check_placeholder_region`), поведение для `%w`/`%d{n,m}` не меняется
    /// ни на бит — переписывать десятки уже задокументированных тестов ради
    /// добавления параметра, который им не нужен, только шум в диффе.
    pub fn find_match(&self, text: &str, sender_id: &str) -> Option<MatchedTemplate> {
        self.find_match_with_placeholders(text, sender_id, &PlaceholderRegistry::default())
    }

    /// Экран 36 "Pattern Placeholders" — тот же алгоритм `find_match`, плюс
    /// резолвинг `%{name}` через живой `registry` и накопление извлечённых
    /// значений в `MatchedTemplate::extracted_values` для шаблона-победителя.
    /// `registry` резолвится независимо для КАЖДОГО кандидата в цикле ниже
    /// (не только для финального победителя) — иначе тай-брейк по
    /// специфичности мог бы выбрать шаблон, который на самом деле проваливает
    /// проверку именованного плейсхолдера.
    pub fn find_match_with_placeholders(
        &self,
        text: &str,
        sender_id: &str,
        registry: &PlaceholderRegistry,
    ) -> Option<MatchedTemplate> {
        let hits = self.fragment_hits(text);

        // (template_id, specificity, извлечённые значения) — TemplateId сам по себе insertion order.
        let mut best: Option<(TemplateId, (usize, usize), HashMap<String, String>)> = None;
        for &template_id in hits.keys() {
            if let Some(required) = &self.sender_id_by_template[template_id.index()] {
                if required != sender_id {
                    continue;
                }
            }
            let Some(extracted) = self.check_template(template_id, &hits, text, registry) else { continue };
            let score = Self::specificity(&self.tokens_by_template[template_id.index()]);
            let is_better = match &best {
                None => true,
                Some((best_id, best_score, _)) => {
                    score > *best_score || (score == *best_score && template_id < *best_id)
                }
            };
            if is_better {
                best = Some((template_id, score, extracted));
            }
        }

        best.map(|(template_id, _, extracted_values)| {
            let t = &self.templates[template_id.index()];
            MatchedTemplate { template_id: t.template_id.clone(), category: t.category.clone(), extracted_values }
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
            sender_id: None,
        }
    }

    #[test]
    fn real_example_matches() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring";
        let result = ruleset.find_match(text, "any-sender");
        assert!(result.is_some(), "должно было смачиться — это ровно пример из чата");
        assert_eq!(result.unwrap().category, "TRANSACTION");
    }

    #[test]
    fn digit_count_out_of_range_no_match() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 7 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text, "any-sender").is_none(), "7 цифр вне {{1,6}} — не должно матчиться");
    }

    #[test]
    fn word_placeholder_rejects_internal_whitespace() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello world shartnoma bo'yicha 123 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text, "any-sender").is_none());
    }

    #[test]
    fn missing_literal_fragment_no_match() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "Hello1238 completely different text with no template fragments at all";
        assert!(ruleset.find_match(text, "any-sender").is_none());
    }

    #[test]
    fn digits_with_separators_counted_correctly() {
        let ruleset = CompiledRuleset::new(vec![real_template()]);
        let text = "ABC123 shartnoma bo'yicha 12-34-56 so'm to'lovni bugun amalga oshiring";
        assert!(ruleset.find_match(text, "any-sender").is_some(), "разделители между цифрами должны игнорироваться");
    }

    #[test]
    fn multiple_candidates_resolved_deterministically() {
        let tpl_word = Template { template_id: "tpl-word".into(), pattern: "code: %w".into(), category: "SERVICE".into(), sender_id: None };
        let tpl_digit = Template { template_id: "tpl-digit".into(), pattern: "code: %d{4,4}".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl_word, tpl_digit]);

        // Найдено при реализации 4.3: "code: 1234" технически проходит фазу 2
        // для ОБОИХ шаблонов ("1234" — непустой непробельный %w, и ровно 4
        // цифры для %d{4,4}) — это и есть настоящая неоднозначность, не
        // гипотетическая. `tpl-word` зарегистрирован ПЕРВЫМ (insertion order),
        // но `tpl-digit` строже (ограничивает и алфавит, и длину) — специфичность
        // обязана выбрать именно его, не первый по регистрации.
        let result_digit_text = ruleset.find_match("code: 1234", "any-sender");
        assert_eq!(result_digit_text.unwrap().template_id, "tpl-digit",
            "более специфичный шаблон (%d{{4,4}}) обязан победить менее специфичный (%w) при реальной неоднозначности");

        let result_word_only_text = ruleset.find_match("code: ABCD", "any-sender");
        assert_eq!(result_word_only_text.unwrap().template_id, "tpl-word", "ABCD не цифры — только tpl-word должен пройти");
    }

    #[test]
    fn specificity_prefers_digit_over_word_for_same_literal_length() {
        // Прямая, изолированная проверка specificity() — не через find_match,
        // чтобы отличить "правило работает" от "правило совпало случайно с
        // порядком регистрации" (симметричный тест — Word и Digit в
        // обратном порядке регистрации всё равно должны выбрать Digit).
        let tpl_word = Template { template_id: "w".into(), pattern: "pin: %w".into(), category: "S".into(), sender_id: None };
        let tpl_digit = Template { template_id: "d".into(), pattern: "pin: %d{4,4}".into(), category: "S".into(), sender_id: None };
        // Регистрируем Digit ПЕРВЫМ на этот раз — если бы побеждал порядок
        // регистрации, а не специфичность, оба порядка дали бы Digit, и тест
        // не отличил бы правило от совпадения. Раз оба порядка (этот тест и
        // предыдущий) дают Digit — специфичность реально решает, не порядок.
        let ruleset = CompiledRuleset::new(vec![tpl_digit, tpl_word]);
        let result = ruleset.find_match("pin: 1234", "any-sender");
        assert_eq!(result.unwrap().template_id, "d");
    }

    #[test]
    fn specificity_breaks_ties_by_literal_length_when_placeholder_profile_is_identical() {
        // Оба шаблона — одиночный %w, оба реально матчат один и тот же текст
        // (genuine ambiguity: "code " — суффикс "the code ", Aho-Corasick
        // находит оба литерала в одном тексте) — более длинный, более
        // специфичный литеральный префикс обязан победить.
        let short_literal = Template { template_id: "short".into(), pattern: "code %w".into(), category: "S".into(), sender_id: None };
        let long_literal = Template { template_id: "long".into(), pattern: "the code %w".into(), category: "S".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![short_literal, long_literal]);
        let result = ruleset.find_match("the code 123", "any-sender");
        assert_eq!(result.unwrap().template_id, "long", "более длинный, более специфичный литеральный контекст должен победить");
    }

    #[test]
    fn leading_and_trailing_placeholder() {
        let tpl = Template { template_id: "tpl-edges".into(), pattern: "%d{2,2}-ok-%w".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("42-ok-done", "any-sender").is_some());
        assert!(ruleset.find_match("4-ok-done", "any-sender").is_none(), "только 1 цифра, нужно ровно 2");
        assert!(ruleset.find_match("42-ok-", "any-sender").is_none(), "пустой %w в конце недопустим");
    }

    // Регрессия на находку кодревью: parse_pattern раньше паниковал на любом
    // многобайтовом UTF-8 символе (слайсинг по не-граничному байту). Реальный
    // домен этой платформы — кириллица и узбекская латиница с апострофом
    // ʻ/ʼ (U+02BB/U+02BC, 2 байта) — не гипотетический вход.
    #[test]
    fn cyrillic_literal_fragment_does_not_panic() {
        let tpl = Template { template_id: "tpl-cyr".into(), pattern: "Спасибо за %w покупку".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("Спасибо за вашу покупку", "any-sender").is_some());
    }

    #[test]
    fn uzbek_latin_apostrophe_literal_fragment_does_not_panic() {
        // U+02BB (ʻ) — двухбайтовый в UTF-8, реально встречается в узбекских
        // словах вроде "oʻzbekcha". Ровно такой же класс символа, что уже
        // используется в этих тестах внутри %w-разделённых фрагментов
        // ("bo'yicha", "so'm"), но здесь — внутри самого литерала.
        let tpl = Template { template_id: "tpl-uz".into(), pattern: "toʻlov %w bajarildi".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("toʻlov muvaffaqiyatli bajarildi", "any-sender").is_some());
    }

    #[test]
    fn emoji_literal_fragment_does_not_panic() {
        // Emoji — 4-байтовый UTF-8 символ, самый жёсткий случай для
        // границ char boundary.
        let tpl = Template { template_id: "tpl-emoji".into(), pattern: "🎉 %w tabriklaymiz".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("🎉 sizni tabriklaymiz", "any-sender").is_some());
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
                sender_id: None,
            })
            .collect();
        templates.push(Template {
            template_id: "tpl-real".into(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".into(),
            category: "TRANSACTION".into(),
            sender_id: None,
        });

        let ruleset = CompiledRuleset::new(templates);
        // sender_id аргумент здесь безразличен для сути теста — все 20 000
        // "посторонних" шаблонов и сам "tpl-real" имеют sender_id: None
        // (применимы к любому отправителю), поэтому передаём произвольную
        // строку: sender-scoping (Фаза 2) не должен влиять на этот сценарий.
        let result = ruleset.find_match("Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring", "any-sender");
        assert_eq!(result.unwrap().template_id, "tpl-real", "20 000 не связанных шаблонов не должны мешать найти реальное совпадение");
    }

    // Фаза 2 плана (sender_id в модели шаблонов) — `policy.policy_template.sender_id`
    // (nullable, null = все отправители партнёра) теперь должен реально сужать,
    // какие шаблоны применимы к сообщению от конкретного отправителя, не только
    // содержательно совпадать по тексту.
    #[test]
    fn template_scoped_to_sender_only_matches_that_sender() {
        let tpl = Template {
            template_id: "tpl-scoped".into(),
            pattern: "code: %w".into(),
            category: "SERVICE".into(),
            sender_id: Some("sender-a".into()),
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);

        // Тот же текст, тот же шаблон технически проходит фазу 2 матчинга —
        // единственная разница между вызовами ниже — переданный sender_id.
        let matched = ruleset.find_match("code: 1234", "sender-a");
        assert_eq!(matched.unwrap().template_id, "tpl-scoped", "sender_id совпадает с шаблоном — должен матчиться");

        let not_matched = ruleset.find_match("code: 1234", "sender-b");
        assert!(not_matched.is_none(), "тот же текст, но чужой sender_id — шаблон не должен быть eligible вообще");
    }

    #[test]
    fn sender_eligibility_checked_before_specificity_not_after() {
        // Оба шаблона матчат один и тот же текст, у tpl-b профиль плейсхолдеров
        // строже (Digit=2 против Word=1) — если бы specificity() решала раньше
        // sender-фильтра, "sender-a" получил бы результат от tpl-b (чужого
        // отправителя). Правильно: tpl-b должен быть исключён из кандидатов ДО
        // сравнения специфичности, независимо от того, что он "выиграл бы" по
        // очкам, если бы участвовал.
        let tpl_a = Template {
            template_id: "tpl-a".into(),
            pattern: "code: %w".into(),
            category: "SERVICE".into(),
            sender_id: Some("a".into()),
        };
        let tpl_b = Template {
            template_id: "tpl-b".into(),
            pattern: "code: %d{4,4}".into(),
            category: "SERVICE".into(),
            sender_id: Some("b".into()),
        };
        let ruleset = CompiledRuleset::new(vec![tpl_a, tpl_b]);

        let result = ruleset.find_match("code: 1234", "a");
        assert_eq!(result.unwrap().template_id, "tpl-a", "sender=a не должен получить матч от шаблона, привязанного к sender=b, даже если тот специфичнее");
    }

    #[test]
    fn template_with_no_sender_id_matches_any_sender() {
        // Дефолт/обратная совместимость: sender_id: None — сегодняшнее неявное
        // поведение (шаблон применим к любому отправителю партнёра), явно
        // подтверждённое здесь для нескольких разных значений sender_id.
        let tpl = Template {
            template_id: "tpl-any".into(),
            pattern: "code: %w".into(),
            category: "SERVICE".into(),
            sender_id: None,
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);

        assert_eq!(ruleset.find_match("code: 1234", "sender-a").unwrap().template_id, "tpl-any");
        assert_eq!(ruleset.find_match("code: 1234", "sender-b").unwrap().template_id, "tpl-any");
        assert_eq!(ruleset.find_match("code: 1234", "completely-different-sender").unwrap().template_id, "tpl-any");
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

    // ------------------------------------------------------------------
    // Экран 36 "Pattern Placeholders" — `%{name}`.
    // ------------------------------------------------------------------

    fn registry_with(defs: &[(&str, &str)]) -> PlaceholderRegistry {
        PlaceholderRegistry::new(&defs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
    }

    #[test]
    fn parse_pattern_recognizes_named_placeholder() {
        let tokens = parse_pattern("code: %{otp_code} spasibo");
        assert_eq!(
            tokens,
            vec![
                Token::Literal("code: ".to_string()),
                Token::Placeholder(Placeholder::Named("otp_code".to_string())),
                Token::Literal(" spasibo".to_string()),
            ]
        );
    }

    #[test]
    fn parse_pattern_rejects_invalid_placeholder_name_as_literal() {
        // Имя не совпадает с `^[a-z][a-z0-9_]*$` (заглавная буква) — тот же
        // принцип отказа, что у незакрытого `%d{...}`: конструкция остаётся
        // частью литерала, парсинг pattern в целом не падает.
        let tokens = parse_pattern("code: %{OtpCode} spasibo");
        assert_eq!(tokens, vec![Token::Literal("code: %{OtpCode} spasibo".to_string())]);
    }

    #[test]
    fn parse_pattern_rejects_unterminated_named_placeholder_as_literal() {
        let tokens = parse_pattern("code: %{otp_code spasibo");
        assert_eq!(tokens, vec![Token::Literal("code: %{otp_code spasibo".to_string())]);
    }

    #[test]
    fn named_placeholder_matches_and_extracts_value() {
        let tpl = Template {
            template_id: "tpl-otp".into(),
            pattern: "Your code is %{otp_code}, do not share it".into(),
            category: "SERVICE".into(),
            sender_id: None,
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        let registry = registry_with(&[("otp_code", r"\d{6}")]);

        let matched = ruleset
            .find_match_with_placeholders("Your code is 482913, do not share it", "any-sender", &registry)
            .expect("должно смачиться — 6 цифр удовлетворяют \\d{6}");
        assert_eq!(matched.category, "SERVICE");
        assert_eq!(matched.extracted_values.get("otp_code"), Some(&"482913".to_string()));
    }

    #[test]
    fn named_placeholder_region_must_match_regex_in_full_not_just_contain_it() {
        // `\d{4}` без якорей нашёл бы совпадение ВНУТРИ "482913" (например
        // "4829") — компиляция реестра оборачивает пользовательский regex в
        // `^(?:...)$`, поэтому регион обязан удовлетворять regex ЦЕЛИКОМ, а
        // не содержать где-то внутри подходящую подстроку.
        let tpl = Template {
            template_id: "tpl-otp4".into(),
            pattern: "code %{otp_code} end".into(),
            category: "SERVICE".into(),
            sender_id: None,
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        let registry = registry_with(&[("otp_code", r"\d{4}")]);

        assert!(
            ruleset.find_match_with_placeholders("code 482913 end", "any-sender", &registry).is_none(),
            "регион \"482913\" — 6 цифр, не ровно 4, weak-match без якорей был бы багом"
        );
        assert!(ruleset.find_match_with_placeholders("code 4829 end", "any-sender", &registry).is_some());
    }

    #[test]
    fn unknown_placeholder_name_fails_closed_not_panics() {
        let tpl = Template {
            template_id: "tpl-unknown".into(),
            pattern: "code %{does_not_exist_in_registry} end".into(),
            category: "SERVICE".into(),
            sender_id: None,
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        let empty_registry = PlaceholderRegistry::default();

        assert!(
            ruleset.find_match_with_placeholders("code 12345 end", "any-sender", &empty_registry).is_none(),
            "имя не найдено в реестре — обязан быть non-match (fail closed), не паника"
        );
    }

    #[test]
    fn invalid_regex_in_registry_is_skipped_not_panicking_and_behaves_like_unknown_name() {
        // `(` без закрывающей скобки — невалидный regex. `PlaceholderRegistry::new`
        // обязан пропустить эту запись (с логом), не паниковать при сборке
        // реестра — а матчинг обязан вести себя так, будто имени вообще нет.
        let registry = registry_with(&[("broken", "(")]);
        let tpl = Template { template_id: "tpl-broken".into(), pattern: "code %{broken} end".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match_with_placeholders("code 12345 end", "any-sender", &registry).is_none());
    }

    #[test]
    fn backward_compatible_find_match_treats_named_placeholder_as_always_failing() {
        // `find_match` (без реестра) — тонкая обёртка над
        // `find_match_with_placeholders` с пустым `PlaceholderRegistry`, для
        // вызывающих, которым `%{name}` не нужен (весь остальной набор
        // тестов в этом файле). Явная проверка: `%{name}` в pattern не
        // приводит ни к панике, ни к ложному совпадению через этот путь.
        let tpl = Template { template_id: "tpl-otp".into(), pattern: "code %{otp_code} end".into(), category: "SERVICE".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        assert!(ruleset.find_match("code 123456 end", "any-sender").is_none());
    }

    #[test]
    fn named_placeholder_specificity_beats_digit_and_word() {
        // Все три шаблона матчат "code: 1234" — %{four_digits} (Named, regex
        // именно "ровно 4 цифры") обязан победить и %d{1,6} (Digit, шире по
        // допустимой длине), и %w (самый общий).
        let tpl_word = Template { template_id: "w".into(), pattern: "code: %w".into(), category: "S".into(), sender_id: None };
        let tpl_digit = Template { template_id: "d".into(), pattern: "code: %d{1,6}".into(), category: "S".into(), sender_id: None };
        let tpl_named = Template { template_id: "n".into(), pattern: "code: %{four_digits}".into(), category: "S".into(), sender_id: None };
        let ruleset = CompiledRuleset::new(vec![tpl_word, tpl_digit, tpl_named]);
        let registry = registry_with(&[("four_digits", r"\d{4}")]);

        let result = ruleset.find_match_with_placeholders("code: 1234", "any-sender", &registry).unwrap();
        assert_eq!(result.template_id, "n", "именованный плейсхолдер (admin-заданный regex) обязан быть строже встроенных %w/%d{{n,m}}");
        assert_eq!(result.extracted_values.get("four_digits"), Some(&"1234".to_string()));
    }

    #[test]
    fn multiple_named_placeholders_in_one_template_are_all_extracted() {
        let tpl = Template {
            template_id: "tpl-two".into(),
            pattern: "user %{username} code %{otp_code} confirm".into(),
            category: "SERVICE".into(),
            sender_id: None,
        };
        let ruleset = CompiledRuleset::new(vec![tpl]);
        let registry = registry_with(&[("username", r"[a-zA-Z0-9_]+"), ("otp_code", r"\d{6}")]);

        let matched = ruleset
            .find_match_with_placeholders("user Alice42 code 100200 confirm", "any-sender", &registry)
            .expect("оба именованных плейсхолдера должны пройти");
        assert_eq!(matched.extracted_values.get("username"), Some(&"Alice42".to_string()));
        assert_eq!(matched.extracted_values.get("otp_code"), Some(&"100200".to_string()));
    }
}
