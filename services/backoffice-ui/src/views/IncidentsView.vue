<script setup lang="ts">
// luminous-hugging-charm.md Ф7 — "Incidents" screen: open/list/inspect/
// resolve incidents via backoffice-api's /incidents/* proxy
// (incidents.go -> IncidentService). Gated by `incident:manage`
// (RequirePermission.vue, server-side via GET /v1/me), same pattern as
// AccessControlView.vue/AuditLogView.vue.
//
// No dedicated detail route (`/incidents/:id`) exists in this app's router —
// every other view here is a single flat page, not a list+detail route
// pair. Follows that convention: clicking "Просмотр" on a row sets
// `selectedIncidentId`, which drives a second query (enabled only once an
// id is selected) rendered in a card below the table, instead of
// navigating away.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NForm,
  NFormItem,
  NInput,
  NSelect,
  NButton,
  NDataTable,
  NSpace,
  NAlert,
  NTag,
  NDescriptions,
  NDescriptionsItem,
  useMessage,
  useDialog,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type Incident = components["schemas"]["Incident"];
type TimelineEntry = components["schemas"]["TimelineEntry"];
type IncidentNote = components["schemas"]["IncidentNote"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

// Same reasoning as AccessControlView.vue/AuditLogView.vue — `<script setup>`
// runs unconditionally regardless of the `<RequirePermission>` v-if below,
// so `enabled` keeps these queries from firing (and 403ing) before
// `incident:manage` is confirmed.
const hasIncidentManage = computed(() => auth.hasPermission("incident:manage"));

const severityOptions = [
  { label: "LOW", value: "LOW" },
  { label: "MEDIUM", value: "MEDIUM" },
  { label: "HIGH", value: "HIGH" },
  { label: "CRITICAL", value: "CRITICAL" },
];

const severityColor: Record<string, "default" | "info" | "success" | "warning" | "error"> = {
  LOW: "default",
  MEDIUM: "info",
  HIGH: "warning",
  CRITICAL: "error",
};

const statusColor: Record<string, "default" | "warning" | "success"> = {
  OPEN: "warning",
  RESOLVED: "success",
};

const statusFilter = ref("");
const statusFilterOptions = [
  { label: "Все", value: "" },
  { label: "OPEN", value: "OPEN" },
  { label: "RESOLVED", value: "RESOLVED" },
];

const listQuery = useQuery({
  queryKey: ["incidents", statusFilter],
  enabled: hasIncidentManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/incidents", {
      params: { query: { status: (statusFilter.value || undefined) as Incident["status"] | undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const newTitle = ref("");
const newSeverity = ref<string | null>(null);
const openFormInvalid = computed(() => !newTitle.value.trim() || !newSeverity.value);

const openMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/incidents", {
      body: { title: newTitle.value.trim(), severity: newSeverity.value as Incident["severity"] },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Инцидент открыт");
    newTitle.value = "";
    newSeverity.value = null;
    queryClient.invalidateQueries({ queryKey: ["incidents"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const selectedIncidentId = ref<number | null>(null);

const detailQuery = useQuery({
  queryKey: ["incident-detail", selectedIncidentId],
  enabled: computed(() => hasIncidentManage.value && selectedIncidentId.value != null),
  queryFn: async () => {
    const { data, error } = await api.GET("/incidents/{incident_id}", {
      params: { path: { incident_id: selectedIncidentId.value as number } },
    });
    if (error) throw error;
    return data;
  },
});

function selectIncident(row: Incident) {
  selectedIncidentId.value = row.id;
}

const newNote = ref("");

const addNoteMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/incidents/{incident_id}/notes", {
      params: { path: { incident_id: selectedIncidentId.value as number } },
      body: { note: newNote.value.trim() },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Заметка добавлена");
    newNote.value = "";
    queryClient.invalidateQueries({ queryKey: ["incident-detail", selectedIncidentId.value] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const postmortemNotes = ref("");

const resolveMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/incidents/{incident_id}/resolve", {
      params: { path: { incident_id: selectedIncidentId.value as number } },
      body: { postmortem_notes: postmortemNotes.value.trim() },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Инцидент закрыт");
    postmortemNotes.value = "";
    queryClient.invalidateQueries({ queryKey: ["incidents"] });
    queryClient.invalidateQueries({ queryKey: ["incident-detail", selectedIncidentId.value] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmResolve() {
  dialog.warning({
    title: "Закрыть инцидент",
    content: "Инцидент будет помечен RESOLVED. Постмортем обязателен и не может быть изменён из этой формы позже.",
    positiveText: "Закрыть",
    negativeText: "Отмена",
    onPositiveClick: () => resolveMutation.mutate(),
  });
}

const columns: DataTableColumns<Incident> = [
  { title: "id", key: "id" },
  { title: "title", key: "title" },
  {
    title: "severity",
    key: "severity",
    render: (row) => h(NTag, { type: severityColor[row.severity] ?? "default", size: "small", round: true }, () => row.severity),
  },
  {
    title: "status",
    key: "status",
    render: (row) => h(NTag, { type: statusColor[row.status] ?? "default", size: "small", round: true }, () => row.status),
  },
  { title: "opened_by", key: "opened_by" },
  { title: "opened_at", key: "opened_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) => h(NButton, { size: "small", onClick: () => selectIncident(row) }, () => "Просмотр"),
  },
];

const timelineColumns: DataTableColumns<TimelineEntry> = [
  { title: "scope", key: "scope" },
  { title: "scope_id", key: "scope_id" },
  { title: "state", key: "state" },
  { title: "admission_rate", key: "admission_rate" },
  { title: "reason", key: "reason" },
  { title: "requested_by", key: "requested_by" },
  { title: "created_at", key: "created_at" },
  { title: "expires_at", key: "expires_at" },
];

const noteColumns: DataTableColumns<IncidentNote> = [
  { title: "author", key: "author" },
  { title: "note", key: "note" },
  { title: "created_at", key: "created_at" },
];
</script>

<template>
  <RequirePermission permission="incident:manage">
    <NSpace vertical size="large">
      <NCard title="Incidents — открыть инцидент">
        <NForm inline label-placement="top">
          <NFormItem label="title">
            <NInput v-model:value="newTitle" style="width: 320px" />
          </NFormItem>
          <NFormItem label="severity">
            <NSelect v-model:value="newSeverity" :options="severityOptions" style="width: 180px" placeholder="выберите" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton type="primary" :disabled="openFormInvalid" :loading="openMutation.isPending.value" @click="openMutation.mutate()">
              Открыть
            </NButton>
          </NFormItem>
        </NForm>
      </NCard>

      <NCard title="Список инцидентов">
        <NForm inline label-placement="top">
          <NFormItem label="status">
            <NSelect v-model:value="statusFilter" :options="statusFilterOptions" style="width: 200px" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton @click="listQuery.refetch()">Обновить</NButton>
          </NFormItem>
        </NForm>
        <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
          {{ extractErrorMessage(listQuery.error.value) }}
        </NAlert>
        <NDataTable
          :columns="columns"
          :data="listQuery.data.value?.incidents ?? []"
          :loading="listQuery.isLoading.value"
          :row-key="(row: Incident) => row.id"
        />
      </NCard>

      <NCard v-if="selectedIncidentId != null" :title="`Инцидент #${selectedIncidentId}`">
        <NAlert v-if="detailQuery.isError.value" type="error" style="margin-bottom: 12px">
          {{ extractErrorMessage(detailQuery.error.value) }}
        </NAlert>
        <template v-if="detailQuery.data.value">
          <NDescriptions :column="2" bordered size="small" style="margin-bottom: 16px">
            <NDescriptionsItem label="title">{{ detailQuery.data.value.incident.title }}</NDescriptionsItem>
            <NDescriptionsItem label="severity">{{ detailQuery.data.value.incident.severity }}</NDescriptionsItem>
            <NDescriptionsItem label="status">{{ detailQuery.data.value.incident.status }}</NDescriptionsItem>
            <NDescriptionsItem label="opened_by">{{ detailQuery.data.value.incident.opened_by }}</NDescriptionsItem>
            <NDescriptionsItem label="opened_at">{{ detailQuery.data.value.incident.opened_at }}</NDescriptionsItem>
            <NDescriptionsItem label="resolved_by">{{ detailQuery.data.value.incident.resolved_by ?? "—" }}</NDescriptionsItem>
            <NDescriptionsItem label="resolved_at">{{ detailQuery.data.value.incident.resolved_at ?? "—" }}</NDescriptionsItem>
            <NDescriptionsItem label="postmortem_notes">{{ detailQuery.data.value.incident.postmortem_notes ?? "—" }}</NDescriptionsItem>
          </NDescriptions>

          <NCard title="Таймлайн (execution-control)" size="small" style="margin-bottom: 16px" embedded>
            <NDataTable
              :columns="timelineColumns"
              :data="detailQuery.data.value.timeline"
              :row-key="(row: TimelineEntry) => row.id"
            />
          </NCard>

          <NCard title="Заметки" size="small" embedded>
            <NDataTable :columns="noteColumns" :data="detailQuery.data.value.notes" :row-key="(row: IncidentNote) => row.id" style="margin-bottom: 12px" />
            <NForm inline label-placement="top">
              <NFormItem label="новая заметка">
                <NInput v-model:value="newNote" style="width: 360px" />
              </NFormItem>
              <NFormItem label=" ">
                <NButton :disabled="!newNote.trim()" :loading="addNoteMutation.isPending.value" @click="addNoteMutation.mutate()">
                  Добавить
                </NButton>
              </NFormItem>
            </NForm>
          </NCard>

          <NCard v-if="detailQuery.data.value.incident.status === 'OPEN'" title="Закрыть инцидент" size="small" style="margin-top: 16px" embedded>
            <NForm inline label-placement="top">
              <NFormItem label="postmortem_notes">
                <NInput v-model:value="postmortemNotes" type="textarea" style="width: 420px" :autosize="{ minRows: 2 }" />
              </NFormItem>
              <NFormItem label=" ">
                <NButton type="error" :disabled="!postmortemNotes.trim()" :loading="resolveMutation.isPending.value" @click="confirmResolve">
                  Закрыть
                </NButton>
              </NFormItem>
            </NForm>
          </NCard>
        </template>
      </NCard>
    </NSpace>
  </RequirePermission>
</template>
