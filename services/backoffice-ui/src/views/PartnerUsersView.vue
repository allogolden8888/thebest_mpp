<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 35 "Partner Users" — browse/grant/revoke
// iam.partner_portal_role_assignments (V025, already used by
// partner-self-service-api's JWT layer, но до сих пор без админского
// эндпоинта). Same table/form/mutation conventions as AccessControlView.vue
// (Users & Roles, staff-assignments) — deliberately NOT merged into that
// screen: different table, different fixed two-value role set (not the
// open iam.roles catalog staff uses), different FK (external_id ->
// iam.partner_portal_users, not free-standing).
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
  useMessage,
  useDialog,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type PartnerPortalAssignment = components["schemas"]["PartnerPortalAssignment"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

// Same gating pattern as AccessControlView.vue — see its comment for why
// `enabled` matters here (script setup runs before RequirePermission's v-if
// resolves).
const hasIamManage = computed(() => auth.hasPermission("iam:manage"));

const filterExternalId = ref("");

const assignmentsQuery = useQuery({
  queryKey: ["iam-partner-portal-assignments", filterExternalId],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/partner-portal-assignments", {
      params: { query: { external_id: filterExternalId.value || undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const roleOptions = [
  { label: "partner-admin", value: "partner-admin" },
  { label: "partner-viewer", value: "partner-viewer" },
];

const newExternalId = ref("");
const newRole = ref<string | null>(null);

const grantFormInvalid = computed(() => !newExternalId.value.trim() || !newRole.value);

const grantMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/iam/partner-portal-assignments", {
      body: { external_id: newExternalId.value.trim(), role: newRole.value as "partner-admin" | "partner-viewer" },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Роль назначена");
    newExternalId.value = "";
    newRole.value = null;
    queryClient.invalidateQueries({ queryKey: ["iam-partner-portal-assignments"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const revokeMutation = useMutation({
  mutationFn: async (row: PartnerPortalAssignment) => {
    const { data, error } = await api.DELETE("/iam/partner-portal-assignments/{external_id}/{role}", {
      params: { path: { external_id: row.external_id, role: row.role } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    if (data?.revoked) {
      message.success("Роль отозвана");
    } else {
      message.info("Нечего отзывать — активное назначение уже не найдено");
    }
    queryClient.invalidateQueries({ queryKey: ["iam-partner-portal-assignments"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmRevoke(row: PartnerPortalAssignment) {
  dialog.warning({
    title: "Подтвердите отзыв роли",
    content: `Роль "${row.role}" будет отозвана у ${row.external_id}.`,
    positiveText: "Отозвать",
    negativeText: "Отмена",
    onPositiveClick: () => revokeMutation.mutate(row),
  });
}

const columns: DataTableColumns<PartnerPortalAssignment> = [
  { title: "external_id", key: "external_id" },
  { title: "role", key: "role" },
  { title: "granted_by", key: "granted_by" },
  { title: "granted_at", key: "granted_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(
        NButton,
        { size: "small", type: "error", loading: revokeMutation.isPending.value, onClick: () => confirmRevoke(row) },
        () => "Отозвать",
      ),
  },
];
</script>

<template>
  <RequirePermission permission="iam:manage">
    <NSpace vertical size="large">
      <NCard title="Partner Users — назначить роль">
        <NForm inline label-placement="top">
          <NFormItem label="external_id">
            <NInput v-model:value="newExternalId" style="width: 240px" placeholder="Keycloak sub партнёрского пользователя" />
          </NFormItem>
          <NFormItem label="role">
            <NSelect v-model:value="newRole" :options="roleOptions" style="width: 220px" placeholder="выберите роль" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton
              type="primary"
              :disabled="grantFormInvalid"
              :loading="grantMutation.isPending.value"
              @click="grantMutation.mutate()"
            >
              Назначить
            </NButton>
          </NFormItem>
        </NForm>
        <NAlert type="info" style="margin-top: 8px">
          external_id должен уже существовать в iam.partner_portal_users (заводится при первом логине пользователя в partner-portal-ui) — назначение роли неизвестному пользователю вернёт 404.
        </NAlert>
      </NCard>

      <NCard title="Активные назначения">
        <NForm inline label-placement="top">
          <NFormItem label="external_id (фильтр)">
            <NInput v-model:value="filterExternalId" placeholder="пусто — все" style="width: 240px" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton @click="assignmentsQuery.refetch()">Обновить</NButton>
          </NFormItem>
        </NForm>
        <NAlert v-if="assignmentsQuery.isError.value" type="error" style="margin-bottom: 12px">
          {{ extractErrorMessage(assignmentsQuery.error.value) }}
        </NAlert>
        <NDataTable
          :columns="columns"
          :data="assignmentsQuery.data.value?.assignments ?? []"
          :loading="assignmentsQuery.isLoading.value"
          :row-key="(row: PartnerPortalAssignment) => row.id"
        />
      </NCard>
    </NSpace>
  </RequirePermission>
</template>
