<script setup lang="ts">
// BACKOFFICE_ROADMAP.md раздел 1 (Billing) — лента списаний +
// сводка по партнёрам. GET /v1/billing/ledger, GET /v1/billing/summary
// (backoffice-api/internal/httpapi/billing.go, читает billing.billing_ledger
// напрямую, тот же класс прямого чтения, что DlqBrowse/ReconciliationBrowse).
//
// Тарифы (billing_tariff) и Сверка (reconciliation) сознательно НЕ здесь —
// первое уже редактируется через generic ConfigView.vue (entity_type=
// billing_tariff), второе уже своим отдельным экраном (ReconciliationView.vue,
// пункт меню "Reconciliation") — не дублируем.
import { ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NSelect, NButton, NDataTable, NAlert, NSpace, NText, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema";

type LedgerEntry = components["schemas"]["LedgerEntry"];
type LedgerSummaryRow = components["schemas"]["LedgerSummaryRow"];

const api = useApi();

// --- Лента списаний ---
const partnerId = ref("");
const entryType = ref<string | null>(null);

const ledgerQuery = useQuery({
  queryKey: ["billing-ledger", partnerId, entryType],
  queryFn: async () => {
    const { data, error } = await api.GET("/billing/ledger", {
      params: { query: { partner_id: partnerId.value || undefined, entry_type: entryType.value ?? undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const ledgerColumns: DataTableColumns<LedgerEntry> = [
  { title: "created_at", key: "created_at" },
  { title: "partner_id", key: "partner_id" },
  { title: "charge_id", key: "charge_id" },
  {
    title: "тип",
    key: "entry_type",
    render: (row) => (row.entry_type === "compensating" ? "компенсация" : "списание"),
  },
  {
    title: "сумма",
    key: "amount",
    render: (row) => `${row.entry_type === "compensating" ? "−" : ""}${row.amount} ${row.currency}`,
  },
  {
    title: "источник",
    key: "source_charge_id",
    render: (row) => row.source_charge_id ?? "—",
  },
];

// --- Сводка по партнёрам ---
const groupBy = ref<"partner" | "day">("partner");

const summaryQuery = useQuery({
  queryKey: ["billing-summary", groupBy],
  queryFn: async () => {
    const { data, error } = await api.GET("/billing/summary", {
      params: { query: { group_by: groupBy.value } },
    });
    if (error) throw error;
    return data;
  },
});

const summaryColumns: DataTableColumns<LedgerSummaryRow> = [
  { title: groupBy.value === "day" ? "день" : "партнёр", key: "key" },
  { title: "списания", key: "charges" },
  { title: "компенсации", key: "compensations" },
  { title: "нетто", key: "net" },
  { title: "сообщений", key: "entry_count" },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Биллинг — лента списаний">
      <NForm inline label-placement="top">
        <NFormItem label="partner_id">
          <NInput v-model:value="partnerId" style="width: 200px" />
        </NFormItem>
        <NFormItem label="entry_type">
          <NSelect
            v-model:value="entryType"
            clearable
            style="width: 200px"
            :options="[
              { label: 'списание', value: 'charge' },
              { label: 'компенсация', value: 'compensating' },
            ]"
          />
        </NFormItem>
        <NFormItem label=" ">
          <NButton @click="ledgerQuery.refetch()">Обновить</NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="ledgerQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(ledgerQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="ledgerColumns"
        :data="ledgerQuery.data.value?.entries ?? []"
        :loading="ledgerQuery.isLoading.value"
        :row-key="(row: LedgerEntry) => row.id"
      />
    </NCard>

    <NCard title="Биллинг — сводка по партнёрам">
      <NForm inline label-placement="top">
        <NFormItem label="группировка">
          <NSelect
            v-model:value="groupBy"
            style="width: 160px"
            :options="[
              { label: 'по партнёру', value: 'partner' },
              { label: 'по дню', value: 'day' },
            ]"
          />
        </NFormItem>
      </NForm>
      <NAlert v-if="summaryQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(summaryQuery.error.value) }}
      </NAlert>
      <NText v-if="summaryQuery.data.value" depth="3" style="display: block; margin-bottom: 8px">
        Валюта: {{ summaryQuery.data.value.currency }}
      </NText>
      <NDataTable
        :columns="summaryColumns"
        :data="summaryQuery.data.value?.rows ?? []"
        :loading="summaryQuery.isLoading.value"
        :row-key="(row: LedgerSummaryRow) => row.key"
      />
    </NCard>
  </NSpace>
</template>
