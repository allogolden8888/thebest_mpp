import { inject, type InjectionKey } from "vue";
import type { ApiClient } from "./client";

export const apiClientKey: InjectionKey<ApiClient> = Symbol("apiClient");

export function useApi(): ApiClient {
  const client = inject(apiClientKey);
  if (!client) {
    throw new Error("apiClient не предоставлен — забыли app.provide(apiClientKey, ...) в main.ts?");
  }
  return client;
}
