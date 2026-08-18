<script setup lang="ts">
// luminous-hugging-charm.md Ф8 — "Ops Health" screen: Kafka consumer-group
// lag + per-service /readyz grid in one place, sourced from
// GET /v1/ops/snapshot (backoffice-api's ops.go, a plain HTTP proxy — NOT
// gRPC — to ops-visibility-service's own GET /snapshot, which itself just
// reads two Redis keys populated by background poll loops; see that
// service's README.md). Gated by `ops:read`, same pattern as
// AccessControlView.vue/AuditLogView.vue.
//
// `kafka_lag_available`/`readyz_available` are explicit flags in the
// response (not inferred from null vs empty) — a missing/expired Redis key
// (poll loop just started, or has been down past SNAPSHOT_TTL) must be
// visibly different from "polled fine, zero groups/services", so both are
// surfaced as their own NAlert rather than just rendering an empty table.
import { computed, h } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NButton, NDataTable, NSpace, NAlert, NTag, NText, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type ConsumerGroupLag = components["schemas"]["ConsumerGroupLag"];
type PartitionLag = components["schemas"]["PartitionLag"];
type ReadyzResult = components["schemas"]["ReadyzResult"];

const api = useApi();
const auth = useAuthStore();

// Same reasoning as AccessControlView.vue/AuditLogView.vue — `<script setup>`
// runs unconditionally regardless of the `<RequirePermission>` v-if below,
// so `enabled` keeps this query from firing (and 403ing) before `ops:read`
// is confirmed.
const hasOpsRead = computed(() => auth.hasPermission("ops:read"));

const snapshotQuery = useQuery({
  queryKey: ["ops-snapshot"],
  enabled: hasOpsRead,
  queryFn: async () => {
    const { data, error } = await api.GET("/ops/snapshot");
    if (error) throw error;
    return data;
  },
});

const groupSummaryColumns: DataTableColumns<ConsumerGroupLag> = [
  { title: "group", key: "group" },
  { title: "state", key: "state" },
  { title: "total_lag", key: "total_lag" },
  {
    title: "error",
    key: "error",
    render: (row) => (row.error ? h(NText, { type: "error" }, () => row.error) : ""),
  },
];

type PartitionRow = PartitionLag & { group: string };

const partitionRows = computed<PartitionRow[]>(() => {
  const groups = snapshotQuery.data.value?.kafka_lag?.groups ?? [];
  return groups.flatMap((g) => g.partitions.map((p) => ({ ...p, group: g.group })));
});

const partitionColumns: DataTableColumns<PartitionRow> = [
  { title: "group", key: "group" },
  { title: "topic", key: "topic" },
  { title: "partition", key: "partition" },
  { title: "commit_offset", key: "commit_offset" },
  { title: "end_offset", key: "end_offset" },
  { title: "lag", key: "lag" },
  {
    title: "error",
    key: "error",
    render: (row) => (row.error ? h(NText, { type: "error" }, () => row.error) : ""),
  },
];

const readyzColumns: DataTableColumns<ReadyzResult> = [
  { title: "service", key: "service" },
  {
    title: "ready",
    key: "ready",
    render: (row) => h(NTag, { type: row.ready ? "success" : "error", size: "small", round: true }, () => (row.ready ? "ready" : "not ready")),
  },
  { title: "http_status", key: "http_status" },
  { title: "latency_ms", key: "latency_ms" },
  {
    title: "error",
    key: "error",
    render: (row) => (row.error ? h(NText, { type: "error" }, () => row.error) : ""),
  },
];
</script>

<template>
  <RequirePermission permission="ops:read">
    <NSpace vertical size="large">
      <NCard title="Ops Health">
        <NButton :loading="snapshotQuery.isFetching.value" @click="snapshotQuery.refetch()">Обновить</NButton>
        <NAlert v-if="snapshotQuery.isError.value" type="error" style="margin-top: 12px">
          {{ extractErrorMessage(snapshotQuery.error.value) }}
        </NAlert>
      </NCard>

      <NCard title="Kafka consumer lag">
        <NAlert v-if="snapshotQuery.data.value && !snapshotQuery.data.value.kafka_lag_available" type="warning" style="margin-bottom: 12px">
          Снапшот lag недоступен — либо ops-visibility-service ещё не завершил первый цикл опроса, либо TTL ключа в Redis истёк.
        </NAlert>
        <NAlert v-if="snapshotQuery.data.value?.kafka_lag?.error" type="error" style="margin-bottom: 12px">
          {{ snapshotQuery.data.value.kafka_lag.error }}
        </NAlert>
        <template v-if="snapshotQuery.data.value?.kafka_lag_available">
          <NText depth="3" style="display: block; margin-bottom: 8px">
            generated_at: {{ snapshotQuery.data.value?.kafka_lag?.generated_at }} · brokers:
            {{ snapshotQuery.data.value?.kafka_lag?.bootstrap_servers.join(", ") }}
          </NText>
          <NDataTable
            :columns="groupSummaryColumns"
            :data="snapshotQuery.data.value?.kafka_lag?.groups ?? []"
            :row-key="(row: ConsumerGroupLag) => row.group"
            style="margin-bottom: 16px"
          />
          <NDataTable
            :columns="partitionColumns"
            :data="partitionRows"
            :row-key="(row: PartitionRow) => `${row.group}-${row.topic}-${row.partition}`"
          />
        </template>
      </NCard>

      <NCard title="Readyz по сервисам">
        <NAlert v-if="snapshotQuery.data.value && !snapshotQuery.data.value.readyz_available" type="warning" style="margin-bottom: 12px">
          Снапшот readyz недоступен — либо ops-visibility-service ещё не завершил первый цикл опроса, либо TTL ключа в Redis истёк.
        </NAlert>
        <template v-if="snapshotQuery.data.value?.readyz_available">
          <NText depth="3" style="display: block; margin-bottom: 8px">
            generated_at: {{ snapshotQuery.data.value?.readyz?.generated_at }}
          </NText>
          <NDataTable
            :columns="readyzColumns"
            :data="snapshotQuery.data.value?.readyz?.services ?? []"
            :loading="snapshotQuery.isLoading.value"
            :row-key="(row: ReadyzResult) => row.service"
          />
        </template>
      </NCard>
    </NSpace>
  </RequirePermission>
</template>
