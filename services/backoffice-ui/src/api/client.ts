// Типизированный клиент к Backoffice API поверх openapi-fetch + сгенерированных
// типов (src/api/schema.d.ts, npm run generate:api) — "OpenAPI-generated
// client" из services_specifictaion.md §8.5. UI не обращается к PostgreSQL/
// ClickHouse напрямую (service_internal_methods.md §7.5) — весь ввод/вывод
// идёт через этот клиент.
import createClient from "openapi-fetch";
import type { paths } from "./schema";
import { useAuthStore } from "../stores/auth";

export function createApiClient(baseUrl: string) {
  const client = createClient<paths>({ baseUrl });

  client.use({
    onRequest({ request }) {
      const auth = useAuthStore();
      if (auth.token) {
        request.headers.set("Authorization", `Bearer ${auth.token}`);
      }
      return request;
    },
  });

  return client;
}

export type ApiClient = ReturnType<typeof createApiClient>;
