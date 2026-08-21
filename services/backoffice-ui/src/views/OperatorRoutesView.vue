<script setup lang="ts">
// BACKOFFICE_ROADMAP.md §2 (Операторы/SMPP routes) — read-only снимок
// operator_route:* из Runtime Redis (GET /v1/operators/routes,
// backoffice-api/internal/httpapi/operators.go). Живое состояние, кто
// сейчас держит какой route и когда последний heartbeat — ttl_seconds
// падает к 0, если владелец перестал слать heartbeat (ключ сам исчезнет
// из Redis, отдельного статуса "мёртв" нет). Route table (primary/reserve/
// failover конфиг) — отдельная сущность, уже редактируется через
// ConfigView.vue (entity_type=route_table), здесь не дублируется.
import { h } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NDataTable, NAlert, NButton, NTag, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema";

type OperatorRoute = components["schemas"]["OperatorRoute"];

const api = useApi();

const routesQuery = useQuery({
  queryKey: ["operator-routes"],
  queryFn: async () => {
    const { data, error } = await api.GET("/operators/routes", {});
    if (error) throw error;
    return data;
  },
});

const columns: DataTableColumns<OperatorRoute> = [
  { title: "operator_id", key: "operator_id" },
  { title: "route_id", key: "route_id" },
  {
    title: "protocol",
    key: "protocol",
    render: (row) => h(NTag, { size: "small" }, () => row.protocol),
  },
  { title: "owning_instance_id", key: "owning_instance_id" },
  { title: "endpoint", key: "endpoint" },
  { title: "route_epoch", key: "route_epoch" },
  { title: "heartbeat", key: "heartbeat" },
  {
    title: "ttl",
    key: "ttl_seconds",
    render: (row) => `${row.ttl_seconds}s`,
  },
];
</script>

<template>
  <NCard title="Операторы — живые SMPP/HTTP routes">
    <template #header-extra>
      <NButton size="small" @click="routesQuery.refetch()">Обновить</NButton>
    </template>
    <NAlert v-if="routesQuery.isError.value" type="error" style="margin-bottom: 12px">
      {{ extractErrorMessage(routesQuery.error.value) }}
    </NAlert>
    <NDataTable
      :columns="columns"
      :data="routesQuery.data.value?.routes ?? []"
      :loading="routesQuery.isLoading.value"
      :row-key="(row: OperatorRoute) => `${row.operator_id}:${row.route_id}`"
    />
  </NCard>
</template>
