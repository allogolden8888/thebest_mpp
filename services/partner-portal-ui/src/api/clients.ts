// Два отдельных typed-клиента — partner-self-service-api (applications/
// senders/credentials/webhook/templates-прокси, Ф3) и billing-self-service-api
// (ledger/tariff/recurring, Ф5). Собраны в один объект, инжектируемый одним
// ключом (см. useApi.ts) — каждый view импортирует ровно тот клиент,
// который ему реально нужен.
import { createApiClient, type ApiClientOptions } from "./client";
import type { paths as PartnerPaths } from "./schema-partner";
import type { paths as BillingPaths } from "./schema-billing";

export function createApiClients(
  partnerBaseUrl: string,
  billingBaseUrl: string,
  options: ApiClientOptions = {},
) {
  return {
    partner: createApiClient<PartnerPaths>(partnerBaseUrl, options),
    billing: createApiClient<BillingPaths>(billingBaseUrl, options),
  };
}

export type ApiClients = ReturnType<typeof createApiClients>;
