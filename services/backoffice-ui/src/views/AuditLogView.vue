<script setup lang="ts">
// Phase 0 (iam-service) — "Audit Log" screen: paginated browse over
// GET /v1/audit. Gated by the `audit:read` permission (RequirePermission.vue,
// server-side via GET /v1/me).
//
// No existing view in this codebase does cursor-style "load more" pagination
// (ReportsView/ReconciliationView/DlqBrowseView all just refetch a single
// page on filter change) — this is a new pattern, built by accumulating
// pages into a local `entries` ref and tracking `next_offset` from the
// response (null/omitted => no more rows, matches the API contract).
// Otherwise follows the same TanStack Query + extractErrorMessage()
// conventions as the rest of the app.
import { computed, h, ref, watch } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NSelect, NButton, NDataTable, NAlert, NTag, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type AuditEntry = components["schemas"]["AuditEntry"];

const PAGE_SIZE = 50;

const api = useApi();
const auth = useAuthStore();

// Same reasoning as AccessControlView.vue — `<script setup>` runs
// unconditionally regardless of the `<RequirePermission>` v-if below, so
// `enabled` keeps this query from firing (and 403ing) before `audit:read`
// is confirmed.
const hasAuditRead = computed(() => auth.hasPermission("audit:read"));

const sourceOptions = [
  { label: "Все источники", value: "" },
  { label: "replay", value: "replay" },
  { label: "execution_control", value: "execution_control" },
  { label: "billing_reconciliation", value: "billing_reconciliation" },
  { label: "identity", value: "identity" },
];

const sourceColor: Record<string, "default" | "info" | "success" | "warning" | "error"> = {
  replay: "info",
  execution_control: "warning",
  billing_reconciliation: "success",
  identity: "error",
};

const source = ref("");
const offset = ref(0);
const entries = ref<AuditEntry[]>([]);
const nextOffset = ref<number | null>(null);

const auditQuery = useQuery({
  queryKey: ["audit", source, offset],
  enabled: hasAuditRead,
  queryFn: async () => {
    const { data, error } = await api.GET("/audit", {
      params: { query: { source: (source.value || undefined) as AuditEntry["source"] | undefined, limit: PAGE_SIZE, offset: offset.value } },
    });
    if (error) throw error;
    return data;
  },
});

watch(auditQuery.data, (data) => {
  if (!data) return;
  entries.value = offset.value === 0 ? data.entries : [...entries.value, ...data.entries];
  nextOffset.value = data.next_offset ?? null;
});

function changeSource(value: string) {
  source.value = value;
  offset.value = 0;
  entries.value = [];
  nextOffset.value = null;
}

function loadMore() {
  if (nextOffset.value != null) {
    offset.value = nextOffset.value;
  }
}

const columns: DataTableColumns<AuditEntry> = [
  {
    title: "source",
    key: "source",
    render: (row) => h(NTag, { type: sourceColor[row.source] ?? "default", size: "small", round: true }, () => row.source),
  },
  { title: "actor", key: "actor" },
  { title: "action", key: "action" },
  { title: "target", key: "target" },
  { title: "created_at", key: "created_at" },
];
</script>

<template>
  <RequirePermission permission="audit:read">
    <NCard title="Audit Log">
      <NForm inline label-placement="top">
        <NFormItem label="source">
          <NSelect :value="source" :options="sourceOptions" style="width: 260px" @update:value="changeSource" />
        </NFormItem>
      </NForm>
      <NAlert v-if="auditQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(auditQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="entries"
        :loading="auditQuery.isLoading.value"
        :row-key="(row: AuditEntry) => `${row.source}-${row.actor}-${row.action}-${row.target}-${row.created_at}`"
      />
      <div style="margin-top: 12px; text-align: center">
        <NButton :disabled="nextOffset == null" :loading="auditQuery.isFetching.value && offset > 0" @click="loadMore">
          {{ nextOffset == null ? "Больше нет записей" : "Загрузить ещё" }}
        </NButton>
      </div>
    </NCard>
  </RequirePermission>
</template>
