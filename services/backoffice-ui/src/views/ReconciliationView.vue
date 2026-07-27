<script setup lang="ts">
// handle_reconciliation_browse (service_internal_methods.md §7.3).
import { ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import type { components } from "../api/schema";

type ReconciliationCase = components["schemas"]["ReconciliationCase"];

const api = useApi();
const status = ref("");
const operatorId = ref("");

const listQuery = useQuery({
  queryKey: ["reconciliation-cases", status, operatorId],
  queryFn: async () => {
    const { data, error } = await api.GET("/reconciliation", {
      params: { query: { status: status.value || undefined, operator_id: operatorId.value || undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const columns: DataTableColumns<ReconciliationCase> = [
  { title: "case_id", key: "case_id" },
  { title: "message_id", key: "message_id" },
  { title: "operator_id", key: "operator_id" },
  { title: "status", key: "status" },
  { title: "opened_at", key: "opened_at" },
  { title: "deadline_at", key: "deadline_at" },
];
</script>

<template>
  <NCard title="Reconciliation Cases">
    <NForm inline label-placement="top">
      <NFormItem label="status">
        <NInput v-model:value="status" placeholder="open / resolved / unresolved" style="width: 220px" />
      </NFormItem>
      <NFormItem label="operator_id">
        <NInput v-model:value="operatorId" style="width: 220px" />
      </NFormItem>
      <NFormItem label=" ">
        <NButton @click="listQuery.refetch()">Обновить</NButton>
      </NFormItem>
    </NForm>
    <NDataTable
      :columns="columns"
      :data="listQuery.data.value?.cases ?? []"
      :loading="listQuery.isLoading.value"
      :row-key="(row: ReconciliationCase) => row.case_id"
    />
  </NCard>
</template>
