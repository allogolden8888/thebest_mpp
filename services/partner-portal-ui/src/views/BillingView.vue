<script setup lang="ts">
// handle_billing_self_service (Ф5, агент "Billing") — GET /v1/self-service/
// billing/{ledger,summary,tariff,recurring}. Полностью read-only: нет
// мутаций, нет admin-гейтинга (billing-self-service-api не требует особой
// роли ни на одном из этих маршрутов — см. router.go/billing.go).
//
// Денежные значения (LedgerEntry.amount, SpendSummary.total) приходят как
// строки (NUMERIC(18,4) в Postgres) — намеренно, чтобы не терять точность
// через JS float. Рендерим их как есть (с опциональной обрезкой хвостовых
// нулей строковой операцией), никогда не пропускаем через Number()/parseFloat
// и обратно.
//
// TariffResponse.custom=false и RecurringChargePreview.fee_known=false — оба
// НЕ ошибки, а осознанный "не знаю точных цифр" ответ backend'а (см. doc-
// комментарии в services/billing-self-service-api/internal/httpapi/
// configreads.go): custom=false значит "используется платформенный дефолт,
// его цифры этому сервису недоступны"; fee_known=false значит "тариф партнёра
// не опубликован". В обоих случаях показываем информативное сообщение и НЕ
// подставляем угаданные/нулевые значения.
import { h, computed, ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NDatePicker, NDataTable, NSpace, NAlert, NTag, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema-billing";

type LedgerEntry = components["schemas"]["LedgerEntry"];
type SpendSummary = components["schemas"]["SpendSummary"];
type RecurringChargePreview = components["schemas"]["RecurringChargePreview"];

const api = useApi();

// Общий фильтр диапазона дат для ledger и summary (проще для пользователя,
// чем два независимых виджета — обе секции обычно смотрят на один и тот же
// период). tariff и recurring фильтра по датам не имеют — это снимки
// текущего состояния, не история.
const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
function defaultRange(): [number, number] {
  const now = Date.now();
  return [now - THIRTY_DAYS_MS, now];
}
const dateRange = ref<[number, number] | null>(defaultRange());

const fromIso = computed(() => new Date((dateRange.value ?? defaultRange())[0]).toISOString());
const toIso = computed(() => new Date((dateRange.value ?? defaultRange())[1]).toISOString());

// Строковая обрезка хвостовых нулей — никакого Number()/parseFloat.
function formatAmount(raw: string): string {
  if (!raw.includes(".")) return raw;
  const trimmed = raw.replace(/0+$/, "").replace(/\.$/, "");
  return trimmed === "" ? "0" : trimmed;
}

function formatDateTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

const ledgerQuery = useQuery({
  queryKey: ["billing-ledger", fromIso, toIso],
  queryFn: async () => {
    const { data, error } = await api.billing.GET("/ledger", {
      params: { query: { from: fromIso.value, to: toIso.value, limit: 200 } },
    });
    if (error) throw error;
    return data;
  },
});

const summaryQuery = useQuery({
  queryKey: ["billing-summary", fromIso, toIso],
  queryFn: async () => {
    const { data, error } = await api.billing.GET("/summary", {
      params: { query: { from: fromIso.value, to: toIso.value } },
    });
    if (error) throw error;
    return data;
  },
});

const tariffQuery = useQuery({
  queryKey: ["billing-tariff"],
  queryFn: async () => {
    const { data, error } = await api.billing.GET("/tariff", {});
    if (error) throw error;
    return data;
  },
});

const recurringQuery = useQuery({
  queryKey: ["billing-recurring"],
  queryFn: async () => {
    const { data, error } = await api.billing.GET("/recurring", {});
    if (error) throw error;
    return data;
  },
});

const priceRows = computed(() => {
  const tariff = tariffQuery.data.value?.tariff;
  if (!tariff) return [];
  return Object.entries(tariff.price_per_segment).map(([category, price]) => ({ category, price }));
});

const ledgerColumns: DataTableColumns<LedgerEntry> = [
  { title: "Дата", key: "created_at", render: (row) => formatDateTime(row.created_at) },
  {
    title: "Тип",
    key: "entry_type",
    render: (row) =>
      h(
        NTag,
        { type: row.entry_type === "compensating" ? "warning" : "default", size: "small" },
        () => (row.entry_type === "compensating" ? "Компенсация" : "Списание"),
      ),
  },
  { title: "Сумма", key: "amount", render: (row) => formatAmount(row.amount) },
  { title: "Валюта", key: "currency" },
  { title: "charge_id", key: "charge_id" },
];

const summaryColumns: DataTableColumns<SpendSummary> = [
  { title: "Валюта", key: "currency" },
  { title: "Итого (netto)", key: "total", render: (row) => formatAmount(row.total) },
];

const priceRowColumns: DataTableColumns<{ category: string; price: number }> = [
  { title: "Категория", key: "category" },
  { title: "Цена за сегмент", key: "price" },
];

const recurringColumns: DataTableColumns<RecurringChargePreview> = [
  { title: "sender_id", key: "sender_id" },
  { title: "Тип", key: "type" },
  {
    title: "Ожидаемая плата",
    key: "fee",
    render: (row) => (row.fee_known ? `${row.fee} ${row.currency ?? ""}`.trim() : "уточняется"),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Фильтр периода">
      <NForm inline label-placement="top">
        <NFormItem label="Период (для истории и сводки ниже)">
          <NDatePicker v-model:value="dateRange" type="datetimerange" clearable style="width: 380px" />
        </NFormItem>
      </NForm>
    </NCard>

    <NCard title="История списаний">
      <NAlert v-if="ledgerQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(ledgerQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="ledgerColumns"
        :data="ledgerQuery.data.value ?? []"
        :loading="ledgerQuery.isLoading.value"
        :row-key="(row: LedgerEntry) => row.id"
      />
    </NCard>

    <NCard title="Сводка расходов">
      <NAlert v-if="summaryQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(summaryQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="summaryColumns"
        :data="summaryQuery.data.value ?? []"
        :loading="summaryQuery.isLoading.value"
        :row-key="(row: SpendSummary) => row.currency"
      />
    </NCard>

    <NCard title="Текущий тариф">
      <NAlert v-if="tariffQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(tariffQuery.error.value) }}
      </NAlert>
      <template v-else-if="!tariffQuery.isLoading.value">
        <NAlert v-if="!tariffQuery.data.value?.custom" type="info">
          Индивидуальный тариф для партнёра не опубликован — применяется тариф платформы по умолчанию. Точные цифры
          дефолтного тарифа этот сервис показать не может (они зашиты в billing-service).
        </NAlert>
        <template v-else>
          <p>
            Валюта: <strong>{{ tariffQuery.data.value?.tariff?.currency }}</strong> · Категория по умолчанию:
            <strong>{{ tariffQuery.data.value?.tariff?.default_category }}</strong>
          </p>
          <NDataTable :columns="priceRowColumns" :data="priceRows" :row-key="(row: { category: string }) => row.category" />
          <div v-if="tariffQuery.data.value?.tariff?.recurring_charges" style="margin-top: 12px">
            <p v-if="tariffQuery.data.value?.tariff?.recurring_charges?.alphaname_monthly_fee != null">
              Ежемесячная плата за alphaname: {{ tariffQuery.data.value.tariff.recurring_charges.alphaname_monthly_fee }}
              {{ tariffQuery.data.value.tariff.currency }}
            </p>
            <p v-if="tariffQuery.data.value?.tariff?.recurring_charges?.service_sms_package">
              Пакет service SMS: {{ tariffQuery.data.value.tariff.recurring_charges.service_sms_package.segments }} сегментов
              за {{ tariffQuery.data.value.tariff.recurring_charges.service_sms_package.price }}
              {{ tariffQuery.data.value.tariff.currency }}
            </p>
          </div>
        </template>
      </template>
    </NCard>

    <NCard title="Ожидаемые ежемесячные платежи (recurring)">
      <NAlert v-if="recurringQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(recurringQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="recurringColumns"
        :data="recurringQuery.data.value ?? []"
        :loading="recurringQuery.isLoading.value"
        :row-key="(row: RecurringChargePreview) => row.sender_id"
      />
    </NCard>
  </NSpace>
</template>
