# Backoffice UI

**Основание:** `development_plan.md` — Субагент 1, последний (21-й) сервис моего периметра. Реализует `service_internal_methods.md` §7.5: "Frontend-приложение, собственных методов обработки данных не имеет — все операции проксируются в Backoffice API через OpenAPI-сгенерированный клиент." Один экран на каждый из 7 методов `service_internal_methods.md` §7.3 (см. `services/backoffice-api`).

**Статус:** реально собирается и тестируется — `npm run build` (реальная типизированная production-сборка, `vue-tsc -b && vite build`) и `npm test` (Vitest, 11/11 тестов проходят), не псевдокод.

```bash
cd services/backoffice-ui
npm install --legacy-peer-deps   # см. "Известная проблема с npm audit" ниже
npm run generate:api             # openapi.yaml -> src/api/schema.d.ts
npm run build
npm test
```

## Стек (services_specifictaion.md §8.5) — что реально использовано

| Из стека | Использовано как |
|---|---|
| Vue 3 | `<script setup lang="ts">` везде, Composition API |
| TypeScript | строгий `tsconfig` (унаследован от `@vue/tsconfig`, `noUnusedLocals`/`noUnusedParameters` включены) |
| Vite | сборка (`npm create vite@latest . -- --template vue-ts`) |
| Pinia | `src/stores/auth.ts` — JWT в памяти + `localStorage` |
| Vue Router | `src/router/index.ts` — 1 маршрут на экран + `beforeEach`-guard (редирект на `/login` без токена) |
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

## Аутентификация

`LoginView.vue` — ручной ввод уже выпущенного JWT (вставка токена в текстовое поле), не полноценный Keycloak OIDC redirect flow (`services_specifictaion.md` §8.3 "Keycloak OIDC" описывает это для Backoffice API; Backoffice UI в этом срезе не реализует свою половину OIDC-редиректа). Токен хранится в Pinia + `localStorage` (`src/stores/auth.ts`), прикрепляется к каждому запросу через `openapi-fetch` middleware (`src/api/client.ts`).

## Тесты — что реально проверено

11/11 тестов, `npm test` (Vitest + jsdom + `@vue/test-utils`):

* `src/stores/auth.test.ts` — реальный Pinia store, реальный `localStorage` (jsdom), не мок
* `src/api/client.test.ts` — реальный `openapi-fetch` клиент (`createApiClient`), подменяется только `global.fetch` (граница системы — то же самое, что подмена `http.Client` в Go-тестах этой сессии), проверяет, что middleware реально добавляет `Authorization: Bearer <token>` из auth store
* `src/router/index.test.ts` — реальный `vue-router` инстанс, проверяет `beforeEach`-guard: редирект на `/login` с `?redirect=`, пропуск на публичный `/login`, пропуск на защищённый маршрут с токеном
* `src/views/LoginView.test.ts` — реальный mount через `@vue/test-utils` + `naive-ui`, ввод в `<textarea>`, клик по кнопке, проверка навигации и записи токена

**Известная проблема с localStorage в Node 26** — при запуске `vitest` в этом окружении (Node 26.5.0) глобальный `localStorage`, который сама Node.js предоставляет экспериментально (флаг `--localstorage-file`), конфликтует с `localStorage`, который должен предоставлять jsdom-окружение Vitest: без явного отключения через `NODE_OPTIONS=--no-experimental-webstorage` все тесты, трогающие `localStorage`, падают с `Cannot read properties of undefined`. Диагностировано и исправлено в этом срезе — `package.json` `"test"` скрипт уже включает этот флаг.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon; `npm run build` реально выполнялся локально (не в Docker), это подтверждено.
* **Keycloak OIDC redirect flow** — только ручной ввод токена (см. "Аутентификация").
* **`GetActiveVersion`** — тип сгенерирован, вызов не добавлен ни в один экран.
* **Пагинация** (`page_token`/`next_page_token` для ConfigView, `limit`/`offset` для остальных браузеров) — параметры существуют в API и в сгенерированных типах, но UI не даёт способа их менять (всегда используется значение по умолчанию сервера).
* **`openapi.yaml` поддерживается вручную** — см. "Открытый вопрос" выше, риск рассинхронизации с реальными маршрутами `backoffice-api`, если тот изменится независимо.
* **`npm audit`** — 8 high-severity предупреждений, все транзитивные dev-зависимости (`@redocly/openapi-core` внутри `openapi-typescript`, `js-beautify` внутри `@vue/test-utils`) — `brace-expansion`/`js-yaml` ReDoS-класс уязвимостей. Не эксплуатируемо в контексте использования (локальный кодоген с фиксированным входом, не парсинг недоверенных данных в проде), но не пофикшено, поскольку безопасный фикс требует breaking-даунгрейда `@vue/test-utils` до 2.4.0 — оставлено как есть, а не тихо продавлено.
* **Code-splitting** — production-бандл собирается в один чанк >500KB (предупреждение Vite при сборке); не оптимизировано (`dynamic import()` по маршруту не настроен сверх ленивой загрузки роутов, которая уже есть).
