<script setup lang="ts">
// GET/POST/PUT /v1/self-service/applications — см. openapi-partner.yaml и
// services/partner-self-service-api/internal/httpapi/applications.go
// (mountApplications) за авторитетной формой запросов/ответов.
//
// Паттерн — тот же, что services/backoffice-ui/src/views/ConfigView.vue:
// useQuery/useMutation/useQueryClient, extractErrorMessage() для всех
// ошибок, auth.isAdmin() дизейблит мутирующие кнопки (не прячет весь
// экран — GET /applications открыт любому авторизованному партнёрскому
// токену, только POST/PUT требуют RequireAdmin на бэкенде, см. router.go).
//
// auth.credential_ref НЕ редактируется здесь намеренно — ротация секрета
// живёт на отдельном экране Credentials (см. doc-комментарий в
// updateApplicationRequest, applications.go:127-130): выставлять
// credential_ref напрямую отсюда разошлось бы с тем, что реально лежит в
// Vault. Удаления/delete-эндпоинта у applications нет — кнопки нет.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NForm,
  NFormItem,
  NInput,
  NInputNumber,
  NSelect,
  NButton,
  NDataTable,
  NSpace,
  NAlert,
  NModal,
  useMessage,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema-partner";

type Application = components["schemas"]["Application"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();
const auth = useAuthStore();

const authTypeOptions = [
  { label: "API_KEY", value: "API_KEY" },
  { label: "MTLS", value: "MTLS" },
  { label: "SMPP_BIND", value: "SMPP_BIND" },
];
const channelOptions = [
  { label: "SMS", value: "SMS" },
  { label: "EMAIL", value: "EMAIL" },
  { label: "PUSH", value: "PUSH" },
];

function joinArray(values: string[] | undefined): string {
  return (values ?? []).join(", ");
}

function parseCsv(text: string): string[] {
  return text
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

const listQuery = useQuery({
  queryKey: ["applications"],
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/applications", {});
    if (error) throw error;
    return data ?? [];
  },
});

// --- Создание ---
const applicationId = ref("");
const displayName = ref("");
const authType = ref<string>("API_KEY");
const credentialRef = ref("");
const ipAllowlistText = ref("");
const rateLimitTps = ref<number | null>(10);
const allowedChannels = ref<string[]>([]);

const createInvalid = computed(
  () =>
    !applicationId.value.trim() ||
    !displayName.value.trim() ||
    !credentialRef.value.trim() ||
    !rateLimitTps.value ||
    rateLimitTps.value <= 0 ||
    allowedChannels.value.length === 0,
);

const createMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.partner.POST("/applications", {
      body: {
        application_id: applicationId.value.trim(),
        display_name: displayName.value.trim(),
        auth: { type: authType.value as "API_KEY" | "MTLS" | "SMPP_BIND", credential_ref: credentialRef.value.trim() },
        ip_allowlist: parseCsv(ipAllowlistText.value),
        rate_limit_tps: rateLimitTps.value ?? 0,
        allowed_channels: allowedChannels.value as ("SMS" | "EMAIL" | "PUSH")[],
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Application создан");
    applicationId.value = "";
    displayName.value = "";
    credentialRef.value = "";
    ipAllowlistText.value = "";
    rateLimitTps.value = 10;
    allowedChannels.value = [];
    queryClient.invalidateQueries({ queryKey: ["applications"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

// --- Редактирование (PUT — без auth.credential_ref) ---
const editTarget = ref<Application | null>(null);
const showEditModal = ref(false);
const editDisplayName = ref("");
const editIpAllowlistText = ref("");
const editRateLimitTps = ref<number | null>(0);
const editAllowedChannels = ref<string[]>([]);
const editNotificationCallbackUrl = ref("");

const editInvalid = computed(
  () => !editDisplayName.value.trim() || !editRateLimitTps.value || editRateLimitTps.value <= 0 || editAllowedChannels.value.length === 0,
);

function openEdit(app: Application) {
  editTarget.value = app;
  editDisplayName.value = app.display_name;
  editIpAllowlistText.value = joinArray(app.ip_allowlist);
  editRateLimitTps.value = app.rate_limit_tps;
  editAllowedChannels.value = [...(app.allowed_channels ?? [])];
  editNotificationCallbackUrl.value = app.notification_callback_url ?? "";
  showEditModal.value = true;
}

const updateMutation = useMutation({
  mutationFn: async () => {
    if (!editTarget.value) throw new Error("нет application для редактирования");
    const { data, error } = await api.partner.PUT("/applications/{application_id}", {
      params: { path: { application_id: editTarget.value.application_id } },
      body: {
        display_name: editDisplayName.value.trim(),
        ip_allowlist: parseCsv(editIpAllowlistText.value),
        rate_limit_tps: editRateLimitTps.value ?? 0,
        allowed_channels: editAllowedChannels.value as ("SMS" | "EMAIL" | "PUSH")[],
        notification_callback_url: editNotificationCallbackUrl.value.trim(),
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Application обновлён");
    showEditModal.value = false;
    editTarget.value = null;
    queryClient.invalidateQueries({ queryKey: ["applications"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const columns: DataTableColumns<Application> = [
  { title: "application_id", key: "application_id" },
  { title: "display_name", key: "display_name" },
  { title: "auth.type", key: "auth_type", render: (row) => row.auth.type },
  { title: "auth.credential_ref", key: "auth_credential_ref", render: (row) => row.auth.credential_ref },
  { title: "ip_allowlist", key: "ip_allowlist", render: (row) => joinArray(row.ip_allowlist) || "—" },
  { title: "rate_limit_tps", key: "rate_limit_tps" },
  { title: "allowed_channels", key: "allowed_channels", render: (row) => joinArray(row.allowed_channels) },
  { title: "notification_callback_url", key: "notification_callback_url", render: (row) => row.notification_callback_url || "—" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(
        NButton,
        { size: "small", disabled: !auth.isAdmin(), onClick: () => openEdit(row) },
        () => "Изменить",
      ),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Applications — создать">
      <NForm inline label-placement="top">
        <NFormItem label="application_id">
          <NInput v-model:value="applicationId" style="width: 200px" />
        </NFormItem>
        <NFormItem label="display_name">
          <NInput v-model:value="displayName" style="width: 200px" />
        </NFormItem>
        <NFormItem label="auth.type">
          <NSelect v-model:value="authType" :options="authTypeOptions" style="width: 160px" />
        </NFormItem>
        <NFormItem label="auth.credential_ref">
          <NInput v-model:value="credentialRef" style="width: 200px" />
        </NFormItem>
        <NFormItem label="ip_allowlist">
          <NInput v-model:value="ipAllowlistText" placeholder="через запятую, необязательно" style="width: 220px" />
        </NFormItem>
        <NFormItem label="rate_limit_tps">
          <NInputNumber v-model:value="rateLimitTps" :min="1" style="width: 140px" />
        </NFormItem>
        <NFormItem label="allowed_channels">
          <NSelect v-model:value="allowedChannels" multiple :options="channelOptions" style="width: 220px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton
            type="primary"
            :disabled="!auth.isAdmin() || createInvalid"
            :loading="createMutation.isPending.value"
            @click="createMutation.mutate()"
          >
            Создать
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-top: 12px">
        Создание/изменение applications требует роль partner-admin. Просмотр списка ниже доступен.
      </NAlert>
    </NCard>

    <NCard title="Applications">
      <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(listQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: Application) => row.application_id"
      />
    </NCard>

    <NModal v-model:show="showEditModal" preset="card" title="Изменить application" style="width: 560px">
      <NForm v-if="editTarget" label-placement="top">
        <NFormItem label="application_id">
          <NInput :value="editTarget.application_id" disabled />
        </NFormItem>
        <NFormItem label="display_name">
          <NInput v-model:value="editDisplayName" />
        </NFormItem>
        <NFormItem label="ip_allowlist">
          <NInput v-model:value="editIpAllowlistText" placeholder="через запятую, необязательно" />
        </NFormItem>
        <NFormItem label="rate_limit_tps">
          <NInputNumber v-model:value="editRateLimitTps" :min="1" style="width: 100%" />
        </NFormItem>
        <NFormItem label="allowed_channels">
          <NSelect v-model:value="editAllowedChannels" multiple :options="channelOptions" />
        </NFormItem>
        <NFormItem label="notification_callback_url">
          <NInput v-model:value="editNotificationCallbackUrl" placeholder="необязательно" />
        </NFormItem>
        <NAlert type="info" style="margin-bottom: 12px">
          auth.credential_ref здесь не меняется — используйте экран Credentials для ротации секрета.
        </NAlert>
        <NSpace justify="end">
          <NButton @click="showEditModal = false">Отмена</NButton>
          <NButton
            type="primary"
            :disabled="editInvalid"
            :loading="updateMutation.isPending.value"
            @click="updateMutation.mutate()"
          >
            Сохранить
          </NButton>
        </NSpace>
      </NForm>
    </NModal>
  </NSpace>
</template>
