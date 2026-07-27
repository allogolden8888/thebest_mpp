<script setup lang="ts">
// handle_report_query (service_internal_methods.md §7.3) — почасовые
// агрегаты по партнёру/стадии/исходу (ClickHouse read через Backoffice API).
import { ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import type { components } from "../api/schema";

type ReportRow = components["schemas"]["ReportRow"];

const api = useApi();
const partnerId = ref("");
const stageName = ref("");

const reportQuery = useQuery({
  queryKey: ["report", partnerId, stageName],
  queryFn: async () => {
    const { data, error } = await api.GET("/reports", {
      params: { query: { partner_id: partnerId.value || undefined, stage_name: stageName.value || undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const columns: DataTableColumns<ReportRow> = [
  { title: "hour", key: "hour" },
  { title: "partner_id", key: "partner_id" },
  { title: "stage_name", key: "stage_name" },
  { title: "outcome", key: "outcome" },
  { title: "event_count", key: "event_count" },
];
</script>

<template>
  <NCard title="Reports">
    <NForm inline label-placement="top">
      <NFormItem label="partner_id">
        <NInput v-model:value="partnerId" placeholder="пусто — все партнёры" style="width: 220px" />
      </NFormItem>
      <NFormItem label="stage_name">
        <NInput v-model:value="stageName" style="width: 220px" />
      </NFormItem>
      <NFormItem label=" ">
        <NButton @click="reportQuery.refetch()">Обновить</NButton>
      </NFormItem>
    </NForm>
    <NDataTable
      :columns="columns"
      :data="reportQuery.data.value?.rows ?? []"
      :loading="reportQuery.isLoading.value"
      :row-key="(row: ReportRow) => `${row.hour}-${row.partner_id}-${row.stage_name}-${row.outcome}`"
    />
  </NCard>
</template>