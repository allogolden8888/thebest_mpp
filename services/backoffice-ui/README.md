# Backoffice UI

**Основание:** `development_plan.md` — Субагент 1, последний (21-й) сервис моего периметра. Реализует `service_internal_methods.md` §7.5: "Frontend-приложение, собственных методов обработки данных не имеет — все операции проксируются в Backoffice API через OpenAPI-сгенерированный клиент." Один экран на каждый из 7 методов `service_internal_methods.md` §7.3 (см. `services/backoffice-api`).

**Статус:** реально собирается и тестируется — `npm run build` (реальная типизированная production-сборка, `vue-tsc -b && vite build`) и `npm test` (Vitest, 42/42 тестов проходят), не псевдокод.

```bash
cd services/backoffice-ui
npm install --legacy-peer-deps   # см. "Известная проблема с npm audit" ниже
npm run generate:api             # openapi.yaml -> src/api/schema.d.ts
npm run build
npm test
```

## CODE_REVIEW.md — что исправлено после первого прохода ревью

Прямое продолжение находки авторизации в `backoffice-api` (см. его README): бэкенд раньше не проверял роль вообще, и этот UI тоже не имел концепции роли — единственной проверкой на всём пути от браузера до платформа-останавливающего side effect было "есть ли у человека вообще валидный Keycloak-токен".

* **CRITICAL — полное отсутствие client-side авторизации.** `src/router/index.ts`'s guard проверял только `auth.isAuthenticated` (не-null токен); меню в `src/App.vue` было статическим массивом, показанным одинаково любому аутентифицированному пользователю — ни одна ветка не скрывала Execution Control/Force Scheduler Command по роли. Любой сотрудник с любым валидным токеном мог поставить платформу на паузу через UI. Исправлено зеркалированием той же модели, что теперь есть на бэкенде (`backoffice-api/internal/auth/jwt.go`): `src/stores/jwtRoles.ts` разбирает стандартный Keycloak-claim `realm_access.roles` из JWT (без проверки подписи — это не security-граница сама по себе, реальная проверка прав остаётся на сервере при каждом запросе; здесь только UX-слой), `src/stores/auth.ts` даёт `hasRole()`/`isAdmin()`. `App.vue` теперь фильтрует пункты меню (`adminOnly`) по роли `backoffice-admin`, а `src/components/RequireAdmin.vue` — defense-in-depth гейт на уровне самих `ExecutionControlView`/`SchedulerForceCommandView` (плюс точечное отключение деструктивных кнопок в `ConfigView`/`DlqBrowseView`), показывающий понятное "Недостаточно прав" вместо формы, если не-admin дошёл до раздела напрямую по URL/закладке. Покрыто тестами: `src/stores/auth.test.ts`, `src/stores/jwtRoles.test.ts`, `src/components/RequireAdmin.test.ts`, `src/App.test.ts`.
* **HIGH — ни одного подтверждения перед деструктивным действием.** "Apply Override" (может выставить GLOBAL PAUSED), "Clear Override", "Archive config version", DLQ "Replay" срабатывали в один клик. Добавлен `useDialog().warning(...)` (naive-ui `NDialogProvider`) перед всеми четырьмя с явным подтверждением; для Apply Override и Force Scheduler Command добавлена клиентская проверка непустого `reason` (зеркалит серверную валидацию — `backoffice-api/internal/httpapi/executioncontrol.go`/`scheduler.go`) с понятным сообщением до отправки запроса, а не сырой 400 от сервера. Покрыто тестами `src/views/ExecutionControlView.test.ts`, `src/views/SchedulerForceCommandView.test.ts` — реальный клик по кнопке подтверждения в диалоге, реальная отправка через мокнутый `api.POST`, не просто проверка, что диалог "должен" появиться.
* **HIGH — токен в `localStorage`, без TTL/шифрования.** Рассмотрено и осознанно оставлено как есть (см. комментарий в `src/stores/auth.ts`): полноценный OIDC redirect flow вне периметра этого среза (нет LLD), а in-memory-only вариант ломает F5 при отсутствии такого flow (пришлось бы вставлять JWT заново на каждую перезагрузку — хуже для инструмента, которым пользуются во время инцидента, чем задокументированный риск). XSS sink по-прежнему отсутствует (`v-html`/`innerHTML` не используются нигде, включая весь новый код этого прохода) — это остаётся единственной реальной компенсирующей мерой и должно сохраняться при любых будущих изменениях.
* **HIGH — нет обработки истёкшего/невалидного токена.** `src/api/client.ts` не имел response-интерсептора вовсе: 401 отправлялся молча, каждый следующий mutating-запрос падал без понятной причины. Добавлен `onResponse` middleware — 401 → `auth.clearToken()` + редирект на `/login?sessionExpired=1` (баннер на `LoginView.vue`), вне зависимости от того GET это или мутация. 403 (валидный токен, не хватает роли) намеренно НЕ триггерит редирект — токен рабочий, это просто "недостаточно прав" (см. `RequireAdmin.vue`/`extractErrorMessage`), разлогинивать не нужно. Тесты: `src/api/client.test.ts`.
* **MEDIUM — `String(err)` на JSON-объекте гарантированно печатал `"[object Object]"`.** Корень: `openapi.yaml` не описывает 4xx/5xx-схемы ни для одной операции, так что `error` из `openapi-fetch` нетипизирован. По факту `backoffice-api` сейчас возвращает во всех путях ошибок plain-text через `http.Error`/`internalError` (`backoffice-api/internal/httpapi/errors.go`) — но ничто не гарантирует, что так будет всегда (сетевые ошибки кидают настоящий `Error`, будущий хендлер может начать отдавать JSON). Добавлен `src/api/errorMessage.ts` (`extractErrorMessage`) — корректно обрабатывает строку/`Error`/произвольный объект (пробует `message`/`error`/`detail`/`reason` поля, никогда не даёт буквальный `"[object Object]"`), применён во всех 4 упомянутых в CODE_REVIEW.md view (`ConfigView`, `ExecutionControlView`, `SchedulerForceCommandView`, `DlqBrowseView`), а заодно и в `ReconciliationView`/`ReportsView` для отображения ошибок `useQuery` (те раньше падали вообще без индикации — таблица просто оставалась пустой, без единого сигнала). Тесты: `src/api/errorMessage.test.ts`.
* **MEDIUM — нет security-заголовков.** `nginx.conf` теперь отдаёт `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `Content-Security-Policy` (`frame-ancestors 'none'`, `script-src 'self'` и т.д.) — специально против clickjacking на этой странице (она может поставить весь трафик платформы на паузу). HSTS оставлен закомментированным с пояснением — эта нода не терминирует TLS сама, включать его здесь до подтверждения TLS-терминации перед ней бессмысленно.
* **MEDIUM — `ExecutionControlView`'s `scope`/`state` были свободным текстом.** Единственная форма, способная выставить `PAUSED` на `GLOBAL`, требовала набирать enum-строку по памяти без автодополнения — опечатка либо тихо no-op'ается, либо ведёт себя непредсказуемо. Заменены на `NSelect`-дропдауны с реальными значениями из `platform-contracts/common/enums.proto` (`ExecutionControlScope`/`ExecutionControlState`), тот же паттерн, что уже был у `SchedulerForceCommandView.vue`. Добавлен явный `NAlert`, когда выбрана комбинация GLOBAL+PAUSED.
* **MEDIUM — `nginx.conf`/Dockerfile без proxy route к API.** Инфраструктурный пробел вне периметра этого прохода (`infra/`, `k8s/generate_manifests.py` — не трогаем по правилам). `nginx.conf` теперь документирует ожидаемый `location /v1/ { proxy_pass ...; }` прямо в комментарии с примером, чтобы тот, кто будет разворачивать сервис, не наступил на эту яму молча — тот же пробел честно продублирован здесь, а не спрятан (см. "Что НЕ реализовано" ниже).
* **Побочно найдено при реальном запуске (не в CODE_REVIEW.md, но реальный краш): `App.vue` не оборачивал дерево в `<n-message-provider>`/`<n-dialog-provider>`.** Каждый view, вызывающий `useMessage()` в `setup()` (все, кроме `ReconciliationView`/`ReportsView`), падал бы рантайм-исключением naive-ui ("No outer `<n-message-provider />` founded") сразу при монтировании — то есть ни один экран с мутацией фактически не открывался бы в реальном приложении, несмотря на чистый `npm run build`/`vue-tsc` (эта категория багов невидима на этапе типов). Найдено запуском `npm run dev` + headless Chrome через CDP, не просто чтением кода — см. "Ручная проверка" ниже. Исправлено: `App.vue` теперь оборачивает layout в `<NMessageProvider><NDialogProvider>...</NDialogProvider></NMessageProvider>`.

### Ручная проверка (не только сборка/тесты)

`npm run dev` реально запускался и проверялся headless Chrome через CDP (`Runtime.evaluate`/`Page.navigate`) на этом шаге:
* без токена → редирект на `/login`, ноль ошибок/исключений в консоли;
* с JWT, несущим `realm_access.roles: ["backoffice-admin"]` → меню показывает все 6 пунктов, `/execution-control` открывается, при GLOBAL/PAUSED виден предупреждающий `NAlert`, клик "Apply Override" с заполненным `reason` открывает подтверждающий диалог с текстом "это остановит обработку ВСЕГО трафика платформы", и запрос не уходит до явного подтверждения в диалоге;
* с JWT без `backoffice-admin` → в меню нет Execution Control/Force Scheduler Command, прямой переход на `/execution-control` по URL показывает "Недостаточно прав" вместо формы, а не форму с последующим сырым 403.

### Что из CODE_REVIEW.md сознательно не сделано на этом шаге (честно, не спрятано)

* **Пагинация** (`ConfigView`/`DlqBrowseView`/`ReconciliationView`) — API и типы её поддерживают (`page_token`/`limit`/`offset`), UI по-прежнему не даёт способа их менять. Не сделано по времени; риск ограничен (CODE_REVIEW.md Low #9) — при большом бэклоге DLQ видна только первая страница без индикации, что есть ещё.
* **JSON Schema-валидация payload конфигурации** (`ConfigView`) — по-прежнему только "это валидный JSON", не сверка со схемой из `configuration-service/internal/validate/schemas/*.schema.json`. Требует добавления библиотеки JSON-Schema валидации в зависимости — решено, что не оправдано в рамках этого прохода: конфиг всё равно валидируется на бэкенде перед принятием, здесь это была бы только более ранняя обратная связь, не защита.

## Стек (services_specifictaion.md §8.5) — что реально использовано

| Из стека | Использовано как |
|---|---|
| Vue 3 | `<script setup lang="ts">` везде, Composition API |
| TypeScript | строгий `tsconfig` (унаследован от `@vue/tsconfig`, `noUnusedLocals`/`noUnusedParameters` включены) |
| Vite | сборка (`npm create vite@latest . -- --template vue-ts`) |
| Pinia | `src/stores/auth.ts` — JWT в памяти + `localStorage`, плюс `hasRole()`/`isAdmin()` поверх `realm_access.roles` (см. "Аутентификация и авторизация") |
| Vue Router | `src/router/index.ts` — 1 маршрут на экран + `beforeEach`-guard (редирект на `/login` без токена); role-gating сделан на уровне компонентов (`RequireAdmin.vue`), не роутера, — см. "CODE_REVIEW.md" ниже |
| Naive UI | все компоненты интерфейса (`NDataTable`, `NForm`, `NCard` и т.д.) |
| TanStack Query | `@tanstack/vue-query` — `useQuery`/`useMutation` в каждом view, кеширование и инвалидация после мутаций |
| OpenAPI-generated client | `openapi.yaml` (вручную составлен, отражает реальные маршруты `backoffice-api/internal/httpapi`) → `openapi-typescript` → `src/api/schema.d.ts` → `openapi-fetch` (`src/api/client.ts`) — типобезопасный `client.GET("/dlq", ...)` и т.п., не свободный `fetch` |

## Открытый вопрос — у Backoffice API нет собственного генератора OpenAPI

`service_internal_methods.md` §7.5 и `services_specifictaion.md` §8.5 предполагают "OpenAPI-generated client", но `services/backoffice-api` (Go + chi) не генерирует OpenAPI-спецификацию автоматически — ни один документ этой сессии не специфицировал формат для этого. `openapi.yaml` в этой директории составлен вручную, вручную сверен с каждым маршрутом/JSON-полем в `backoffice-api/internal/httpapi/*.go` на момент написания. **Это единственный источник рассинхронизации, который может возникнуть тихо**: если `backoffice-api` изменит форму запроса/ответа, `openapi.yaml` не обновится сам — нужно вручную поддерживать оба файла синхронными (или, в дальнейшем срезе, сгенерировать `openapi.yaml` из Go-кода через `swaggo`/аналог).

## Экраны — соответствие service_internal_methods.md §7.3

| Экран | Метод backoffice-api | Что показывает |
|---|---|---|
| `ConfigView.vue` (`/config`) | `handle_config_crud` | форма CreateVersion, таблица ListVersions с кнопкой Archive |
| `ExecutionControlView.vue` (`/execution-control`) | `handle_execution_control_override` | форма ApplyOverride + кнопка ClearOverride |
| `SchedulerForceCommandView.vue` (`/scheduler`) | `handle_force_scheduler_command` | форма с выпадающим списком `task_type` (только `FORCE_RETRY`/`FORCE_TIMEOUT` — не свободный ввод, та же защита от открытого редиректа, что на backend) |
| `DlqBrowseView.vue` (`/dlq`) | `handle_dlq_browse` + `handle_replay_request` | таблица DLQ с фильтрами + кнопка "Replay" на каждой строке (естественный UX-поток: сначала посмотреть, потом реплеить) |
| `ReconciliationView.vue` (`/reconciliation`) | `handle_reconciliation_browse` | таблица reconciliation cases с фильтрами |
| `ReportsView.vue` (`/reports`) | `handle_report_query` | таблица почасовых агрегатов (partner/stage/outcome) |

`GetActiveVersion` (часть `handle_config_crud`) сгенерирован в `schema.d.ts`, но не вызывается ни из одного view в этом срезе — см. "Что НЕ реализовано".

## Аутентификация и авторизация

`LoginView.vue` — ручной ввод уже выпущенного JWT (вставка токена в текстовое поле), не полноценный Keycloak OIDC redirect flow (`services_specifictaion.md` §8.3 "Keycloak OIDC" описывает это для Backoffice API; Backoffice UI в этом срезе не реализует свою половину OIDC-редиректа). Токен хранится в Pinia + `localStorage` (`src/stores/auth.ts`), прикрепляется к каждому запросу через `openapi-fetch` middleware (`src/api/client.ts`), которое также редиректит на `/login` при 401 (см. "CODE_REVIEW.md" выше).

Авторизация — `src/stores/jwtRoles.ts` разбирает `realm_access.roles` из JWT (тот же claim, что проверяет `backoffice-api/internal/auth/jwt.go` на сервере); `src/stores/auth.ts` даёт `hasRole(role)`/`isAdmin()`. Роль `backoffice-admin` требуется в UI для тех же 6 маршрутов, что и на бэкенде (config CRUD/archive, execution-control override/clear, force-scheduler-command, replay) — меню и кнопки для них скрыты/задизейблены для не-admin (`App.vue`, `ConfigView.vue`, `DlqBrowseView.vue`), а `ExecutionControlView.vue`/`SchedulerForceCommandView.vue` целиком обёрнуты в `RequireAdmin.vue`. Это UX-слой, не security-граница — подпись токена UI не проверяет, реальная авторизация остаётся на сервере при каждом запросе.

## Тесты — что реально проверено

42/42 тестов, `npm test` (Vitest + jsdom + `@vue/test-utils`):

* `src/stores/auth.test.ts` — реальный Pinia store, реальный `localStorage` (jsdom), не мок; включая `hasRole`/`isAdmin` на реальных JWT-подобных токенах
* `src/stores/jwtRoles.test.ts` — разбор `realm_access.roles` из JWT payload, включая мусорные/неполные токены (не должны кидать исключение или притворяться admin'ом)
* `src/api/client.test.ts` — реальный `openapi-fetch` клиент (`createApiClient`), подменяется только `global.fetch` (граница системы — то же самое, что подмена `http.Client` в Go-тестах этой сессии); проверяет `Authorization: Bearer <token>` middleware и новый 401/403-интерсептор (401 → `onUnauthorized` + очистка токена, 403 → НЕ триггерит редирект)
* `src/api/errorMessage.test.ts` — `extractErrorMessage` на строке/`Error`/произвольном объекте, явно проверяет отсутствие буквального `"[object Object]"`
* `src/router/index.test.ts` — реальный `vue-router` инстанс, проверяет `beforeEach`-guard: редирект на `/login` с `?redirect=`, пропуск на публичный `/login`, пропуск на защищённый маршрут с токеном
* `src/views/LoginView.test.ts` — реальный mount через `@vue/test-utils` + `naive-ui`, ввод в `<textarea>`, клик по кнопке, проверка навигации и записи токена
* `src/components/RequireAdmin.test.ts` — не-admin видит "Недостаточно прав", admin видит защищённый слот
* `src/App.test.ts` — меню реально скрывает/показывает Execution Control и Force Scheduler Command по роли
* `src/views/ExecutionControlView.test.ts` — не-admin гейтится, admin: пустой `reason` блокирует отправку без единого запроса, Apply/Clear Override реально требуют клика по кнопке подтверждения в диалоге перед тем, как `api.POST` вызывается хоть раз
* `src/views/SchedulerForceCommandView.test.ts` — тот же паттерн для Force Scheduler Command

**Известная проблема с localStorage в Node 26** — при запуске `vitest` в этом окружении (Node 26.5.0) глобальный `localStorage`, который сама Node.js предоставляет экспериментально (флаг `--localstorage-file`), конфликтует с `localStorage`, который должен предоставлять jsdom-окружение Vitest: без явного отключения через `NODE_OPTIONS=--no-experimental-webstorage` все тесты, трогающие `localStorage`, падают с `Cannot read properties of undefined`. Диагностировано и исправлено в этом срезе — `package.json` `"test"` скрипт уже включает этот флаг.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon; `npm run build` реально выполнялся локально (не в Docker), это подтверждено.
* **Keycloak OIDC redirect flow** — только ручной ввод токена (см. "Аутентификация и авторизация"). Роль читается из уже введённого токена, но сам вход по-прежнему не через настоящий OIDC redirect.
* **Полная RBAC-модель** — как и на бэкенде, реализован только один барьер (роль `backoffice-admin`). Гранулярные права по методам/сущностям, несколько ролей, UI управления ролями — по-прежнему не реализованы, ждут отдельного LLD (см. `backoffice-api/internal/auth/jwt.go` package doc).
* **`GetActiveVersion`** — тип сгенерирован, вызов не добавлен ни в один экран.
* **Пагинация** (`page_token`/`next_page_token` для ConfigView, `limit`/`offset` для остальных браузеров) — параметры существуют в API и в сгенерированных типах, но UI не даёт способа их менять (всегда используется значение по умолчанию сервера). CODE_REVIEW.md Low #9.
* **JSON Schema-валидация payload конфигурации** — только проверка "это валидный JSON", не сверка со схемой из `configuration-service/internal/validate/schemas/*.schema.json`. CODE_REVIEW.md Low #10.
* **Реальный proxy_pass к backoffice-api в `nginx.conf`** — `location /v1/ { proxy_pass ...; }` не добавлен (нужен реальный k8s Service host/port, инфраструктурная часть вне периметра этого прохода — `infra/`/`k8s/generate_manifests.py` не трогаем). `nginx.conf` документирует ожидаемую форму этого блока комментарием. CODE_REVIEW.md Medium #8.
* **`openapi.yaml` поддерживается вручную** — см. "Открытый вопрос" выше, риск рассинхронизации с реальными маршрутами `backoffice-api`, если тот изменится независимо.
* **`npm audit`** — 8 high-severity предупреждений, все транзитивные dev-зависимости (`@redocly/openapi-core` внутри `openapi-typescript`, `js-beautify` внутри `@vue/test-utils`) — `brace-expansion`/`js-yaml` ReDoS-класс уязвимостей. Не эксплуатируемо в контексте использования (локальный кодоген с фиксированным входом, не парсинг недоверенных данных в проде), но не пофикшено, поскольку безопасный фикс требует breaking-даунгрейда `@vue/test-utils` до 2.4.0 — оставлено как есть, а не тихо продавлено. Не тронуто в этом проходе (не относится к CODE_REVIEW.md находкам).
* **Code-splitting** — production-бандл собирается в один чанк >500KB (предупреждение Vite при сборке); не оптимизировано (`dynamic import()` по маршруту не настроен сверх ленивой загрузки роутов, которая уже есть).
