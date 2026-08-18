import { inject, type InjectionKey } from "vue";
import type { ApiClients } from "./clients";

export const apiClientsKey: InjectionKey<ApiClients> = Symbol("apiClients");

export function useApi(): ApiClients {
  const clients = inject(apiClientsKey);
  if (!clients) {
    throw new Error("apiClients не предоставлены — забыли app.provide(apiClientsKey, ...) в main.ts?");
  }
  return clients;
}
