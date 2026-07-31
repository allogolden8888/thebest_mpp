// Типизированный клиент к Backoffice API поверх openapi-fetch + сгенерированных
// типов (src/api/schema.d.ts, npm run generate:api) — "OpenAPI-generated
// client" из services_specifictaion.md §8.5. UI не обращается к PostgreSQL/
// ClickHouse напрямую (service_internal_methods.md §7.5) — весь ввод/вывод
// идёт через этот клиент.
//
// CODE_REVIEW.md HIGH finding (subagent-1 / backoffice-ui #4) — раньше не
// было вообще никакого response-интерсептора: истёкший токен просто
// отправлялся вечно, и каждый следующий mutating-запрос молча падал с
// нечитаемой ошибкой, без единого сигнала "перелогинься". `onUnauthorized`
// вызывается на каждый 401 (токен невалиден/истёк — backoffice-api's
// auth.Validator.Middleware) вне зависимости от того, GET это или мутация,
// и очищает токен + уводит на /login. 403 (валидный токен, но не хватает
// роли backoffice-admin — auth.RequireRole на стороне backend) намеренно НЕ
// обрабатывается здесь редиректом — токен рабочий, пользователю просто не
// хватает прав; это осознанно оставлено вызывающему коду (см.
// src/api/errorMessage.ts + RequireAdmin.vue), чтобы показать понятное
// "недостаточно прав", а не разлогинивать человека с валидной сессией.
import createClient from "openapi-fetch";
import type { paths } from "./schema";
import { useAuthStore } from "../stores/auth";

export interface ApiClientOptions {
  /** Вызывается на каждый HTTP 401 (истёкший/невалидный токен). */
  onUnauthorized?: () => void;
}

export function createApiClient(baseUrl: string, options: ApiClientOptions = {}) {
  const client = createClient<paths>({ baseUrl });

  client.use({
    onRequest({ request }) {
      const auth = useAuthStore();
      if (auth.token) {
        request.headers.set("Authorization", `Bearer ${auth.token}`);
      }
      return request;
    },
    onResponse({ response }) {
      if (response.status === 401) {
        const auth = useAuthStore();
        auth.clearToken();
        options.onUnauthorized?.();
      }
      return response;
    },
  });

  return client;
}

export type ApiClient = ReturnType<typeof createApiClient>;
