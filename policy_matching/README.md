# Policy Engine — LLD

**Основание:** `platform_contracts.md` §4 ("алгоритм template matching с нормализацией обхода банвордов") + roadmap-пункт "Policy Engine как отдельный вертикальный сервис".

**Статус:** реализовано на настоящем `pyahocorasick` (не заглушка), 23/23 теста проходят на реальных данных из чата (шаблон про `shartnoma`, банворды `idiot, kot, qotaq, мудак, черт, терроризм`) и на реальном JSON-примере конфига (`config_schemas/examples/policy_ruleset.valid.json`).

```bash
pip3 install pyahocorasick
python3 policy_matching/template_matching.py
python3 policy_matching/banword_normalization.py
python3 policy_matching/policy_engine.py
```

## `template_matching.py` — требование 1 (шаблоны + категоризация)

Двухфазный алгоритм, ровно как описан в `data_infrastructure_spec.md` §1.9b:

1. **Фаза 1 (кандидаты).** Настоящий Aho-Corasick находит все вхождения литеральных фрагментов всех шаблонов в тексте; для каждого шаблона жадно проверяется, что его фрагменты идут в правильном порядке, без наложений.
2. **Фаза 2 (точечная проверка).** Для кандидата — регион текста между соседними литералами проверяется на соответствие `%w` (непустой, без пробелов) или `%d{n,m}` (после удаления нецифровых разделителей — количество цифр в диапазоне).

Реальный пример из чата (`%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring` против `Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm...`) матчится корректно — это `test_real_example_matches`.

Также протестировано и осознанно не решено: `test_multiple_candidates_resolved_deterministically` показывает, что при реальной неоднозначности (два шаблона одновременно валидны для одного текста) алгоритм не падает и матчит оба корректно по своим правилам — точная приоритизация при конфликте остаётся отдельным вопросом LLD Policy Service, как и было зафлагано изначально.

## `banword_normalization.py` — требование 5 (банворды, многоязычная нормализация)

Два независимых прохода через один и тот же Aho-Corasick-автомат:

1. **`normalized`** — lowercase + гомоглиф-fold (таблица `HOMOGLYPH_FOLD`, кириллица↔латиница по конкретным визуально идентичным парам: а/e/о/р/с/х/у/і/ѕ/ј/һ) + NFKC. Ловит смешение алфавитов (`qоtaq` с кириллической "о").
2. **`despaced`** — тот же normalized, но без пробелов/дефисов/точек/подчёркиваний. Ловит letter-spacing обход (`т е р р о р и з м`, `q-o-t-a-q`).

**Осознанный компромисс, доказанный тестом, не спрятанный:** `test_despaced_pass_can_false_positive_by_design` показывает, что despaced-проход может случайно склеить два независимых слова в подстроку, совпадающую с банвордом. Это принято как false-positive risk в обмен на отсутствие false-negative по spacing-обходу — совпадение уходит на модерацию/лог, а не тихо теряется.

## `policy_engine.py` — оркестрация всех восьми проверок

Связывает `template_matching.py` (требование 1) и `banword_normalization.py` (требование 5) с оставшимися шестью проверками в порядке зависимостей, зафиксированном в `service_internal_methods.md` §1.5:

```
7 validate_sender → 6 check_sender_blacklist → 1 match_template/resolve_unmatched_behavior
  → 5 check_banwords → 4 check_category_blacklist → 3 check_time_of_day
  → 2 check_spam_frequency (increment только если сообщение реально прошло)
```

`RuntimeState` — in-memory заглушка Runtime Redis (consent-блэклисты, spam-счётчик по скользящему окну); production-реализация — те же операции на реальном Redis. `PolicyRulesetConfig.from_config_schema_json` парсит **ровно** ту форму, что описана в `config_schemas/policy_ruleset.schema.json` — тесты грузят настоящий `config_schemas/examples/policy_ruleset.valid.json`, не отдельно выдуманный fixture, это доказывает, что JSON Schema (прошлый LLD-шаг) и оркестратор (этот шаг) реально согласованы друг с другом.

Ключевые доказанные тестами свойства:

* **`test_banword_blocks_even_though_template_matches`** — permissive `%w` (принимает любой непробельный токен) не создаёт обход контентной модерации: банворды проверяются по полному тексту независимо от исхода `match_template`. Это было архитектурным решением из общего чата, здесь впервые проверено сквозным тестом, а не только заявлено.
* **`test_spam_throttled_after_limit_and_counter_not_incremented_on_rejects`** — счётчик анти-спама инкрементируется только для сообщений, реально прошедших все проверки (`increment_spam_counter` per `service_internal_methods.md`); отклонённое по другой причине сообщение не "тратит" место в окне; окно корректно истекает.
* **`test_outside_time_window`** — категория для time-of-day берётся из результата `match_template`, не задаётся отдельно, и тот же message пропускается в разрешённое время и блокируется вне его.

## Что осталось LLD Policy Service сверх этого

* Таблица гомоглифов не исчерпывающая — только documented Cyrillic/Latin пары из IDN-homograph/анти-фишинг практики, не полный юникод-справочник похожих символов всех языков.
* Точная приоритизация при реальной неоднозначности нескольких прошедших фазу 2 шаблонов.
* Расширение на EMAIL/PUSH каналы (сейчас оба модуля канало-агностичны по своей природе — работают с произвольным текстом — но не протестированы против email-специфичных шаблонов вроде HTML-разметки).
