// Типизированный клиент поверх openapi-fetch + сгенерированных типов, тот
// же паттерн, что services/backoffice-ui/src/api/client.ts (см.
// doc-комментарий там за полным обоснованием onUnauthorized-интерсептора).
//
// Отличие от backoffice-ui: этот портал говорит с ДВУМЯ отдельными backend-
// сервисами (partner-self-service-api, billing-self-service-api — разные
// деплоймые сервисы, разные OpenAPI-схемы), не с одним — поэтому фабрика
// параметризована типом схемы (`Paths`), а не завязана на один конкретный
// `paths` импорт. См. api/clients.ts за двумя реальными инстансами.
import createClient from "openapi-fetch";
import { useAuthStore } from "../stores/auth";

export interface ApiClientOptions {
  /** Вызывается на каждый HTTP 401 (истёкший/невалидный токен). */
  onUnauthorized?: () => void;
}

export function createApiClient<Paths extends object>(baseUrl: string, options: ApiClientOptions = {}) {
  const client = createClient<Paths>({ baseUrl });

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
