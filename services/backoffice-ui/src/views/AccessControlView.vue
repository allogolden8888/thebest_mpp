<script setup lang="ts">
// Phase 0 (iam-service) — "Users & Roles" screen: browse/grant/revoke staff
// role assignments (iam.staff_role_assignments via backoffice-api ->
// iam-service). Gated by the `iam:manage` permission (RequirePermission.vue,
// server-side via GET /v1/me — NOT the JWT `backoffice-admin` realm role,
// see stores/auth.ts doc comment).
//
// Follows the same table/form/TanStack-Query-mutation conventions as
// ConfigView.vue/DlqBrowseView.vue: NDataTable + NForm, confirm dialog
// (useDialog) before the destructive revoke action fires, errors rendered
// via extractErrorMessage() (never `String(errObject)`).
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
  NText,
  useMessage,
  useDialog,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type StaffAssignment = components["schemas"]["StaffAssignment"];
type IamRole = components["schemas"]["IamRole"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

// `<script setup>` runs unconditionally regardless of the `<RequirePermission>`
// v-if in the template below, so without `enabled` these queries would fire
// (and 403) even for a user who can't see this page's content at all — gate
// them on the same permission RequirePermission itself checks. Reactive: if
// GET /v1/me (App.vue's fetchMe) resolves after this view is already
// mounted, the query flips to enabled automatically once permission is
// confirmed.
const hasIamManage = computed(() => auth.hasPermission("iam:manage"));

const filterExternalId = ref("");

const assignmentsQuery = useQuery({
  queryKey: ["iam-staff-assignments", filterExternalId],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/staff-assignments", {
      params: { query: { external_id: filterExternalId.value || undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const rolesQuery = useQuery({
  queryKey: ["iam-roles"],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/roles");
    if (error) throw error;
    return data;
  },
});

const roleOptions = computed(
  () => rolesQuery.data.value?.roles.map((r: IamRole) => ({ label: r.name, value: r.name })) ?? [],
);

const newExternalId = ref("");
const newRole = ref<string | null>(null);

const selectedRolePermissions = computed(() => {
  if (!newRole.value) return [];
  return rolesQuery.data.value?.roles.find((r: IamRole) => r.name === newRole.value)?.permissions ?? [];
});

const grantFormInvalid = computed(() => !newExternalId.value.trim() || !newRole.value);

const grantMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/iam/staff-assignments", {
      body: { external_id: newExternalId.value.trim(), role: newRole.value as string },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Роль назначена");
    newExternalId.value = "";
    newRole.value = null;
    queryClient.invalidateQueries({ queryKey: ["iam-staff-assignments"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const revokeMutation = useMutation({
  mutationFn: async (row: StaffAssignment) => {
    const { data, error } = await api.DELETE("/iam/staff-assignments/{external_id}/{role}", {
      params: { path: { external_id: row.external_id, role: row.role } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    // `revoked: false` is a normal outcome (nothing active to revoke —
    // e.g. already revoked by someone else concurrently), not an error —
    // shown as an inline info message, not an error toast.
    if (data?.revoked) {
      message.success("Роль отозвана");
    } else {
      message.info("Нечего отзывать — активное назначение уже не найдено");
    }
    queryClient.invalidateQueries({ queryKey: ["iam-staff-assignments"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmRevoke(row: StaffAssignment) {
  dialog.warning({
    title: "Подтвердите отзыв роли",
    content: `Роль "${row.role}" будет отозвана у ${row.external_id}.`,
    positiveText: "Отозвать",
    negativeText: "Отмена",
    onPositiveClick: () => revokeMutation.mutate(row),
  });
}

const columns: DataTableColumns<StaffAssignment> = [
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
      <NCard title="Users & Roles — назначить роль">
        <NForm inline label-placement="top">
          <NFormItem label="external_id">
            <NInput v-model:value="newExternalId" style="width: 240px" />
          </NFormItem>
          <NFormItem label="role">
            <NSelect
              v-model:value="newRole"
              :options="roleOptions"
              :loading="rolesQuery.isLoading.value"
              style="width: 220px"
              placeholder="выберите роль"
            />
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
        <NAlert v-if="newRole && selectedRolePermissions.length" type="info" style="margin-top: 8px">
          Права роли "{{ newRole }}": <NText code>{{ selectedRolePermissions.join(", ") }}</NText>
        </NAlert>
        <NAlert v-if="rolesQuery.isError.value" type="error" style="margin-top: 12px">
          {{ extractErrorMessage(rolesQuery.error.value) }}
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
          :row-key="(row: StaffAssignment) => row.id"
        />
      </NCard>
    </NSpace>
  </RequirePermission>
</template>
