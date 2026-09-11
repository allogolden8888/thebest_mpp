<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 35 "Partner Users" — browse/grant/revoke
// iam.partner_portal_role_assignments (V025, already used by
// partner-self-service-api's JWT layer, но до сих пор без админского
// эндпоинта). Same table/form/mutation conventions as AccessControlView.vue
// (Users & Roles, staff-assignments) — deliberately NOT merged into that
// screen: different table, different fixed two-value role set (not the
// open iam.roles catalog staff uses), different FK (external_id ->
// iam.partner_portal_users, not free-standing).
//
// BACKOFFICE_ROADMAP.md Production Readiness Review P0#5 — this screen used
// to ONLY grant/revoke roles to an external_id it assumed already existed
// ("заводится при первом логине", a comment that relied on a Keycloak
// JIT-provisioning flow that was never built — grep for INSERT into
// iam.partner_portal_users found zero real rows). The "Partner Portal Users"
// card below is the actual missing piece: it creates the row itself
// (iam.partner_portal_users, username+password+partner_id), mirroring
// AdminUsersView.vue's create-staff-account form. Kept as a SEPARATE card on
// this same screen rather than a new view — same screen already owns
// "everything about a partner-portal person", just was missing the
// account-creation half of it.
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
type PartnerPortalUser = components["schemas"]["PartnerPortalUser"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

// Same gating pattern as AccessControlView.vue — see its comment for why
// `enabled` matters here (script setup runs before RequirePermission's v-if
// resolves).
const hasIamManage = computed(() => auth.hasPermission("iam:manage"));

const usersQuery = useQuery({
  queryKey: ["iam-partner-portal-users"],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/partner-portal-users", { params: { query: {} } });
    if (error) throw error;
    return data;
  },
});

const newUserUsername = ref("");
const newUserPassword = ref("");
const newUserPartnerId = ref("");
const newUserDisplayName = ref("");
const createUserFormInvalid = computed(
  () => !newUserUsername.value.trim() || !newUserPassword.value || !newUserPartnerId.value.trim() || !newUserDisplayName.value.trim(),
);

const createUserMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/iam/partner-portal-users", {
      body: {
        username: newUserUsername.value.trim(),
        password: newUserPassword.value,
        partner_id: newUserPartnerId.value.trim(),
        display_name: newUserDisplayName.value.trim(),
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Партнёрский пользователь создан");
    newUserUsername.value = "";
    newUserPassword.value = "";
    newUserPartnerId.value = "";
    newUserDisplayName.value = "";
    queryClient.invalidateQueries({ queryKey: ["iam-partner-portal-users"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const deactivateUserMutation = useMutation({
  mutationFn: async (row: PartnerPortalUser) => {
    const { data, error } = await api.POST("/iam/partner-portal-users/{external_id}/deactivate", {
      params: { path: { external_id: row.external_id } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    if (data?.deactivated) {
      message.success("Пользователь деактивирован");
    } else {
      message.info("Пользователь уже был неактивен");
    }
    queryClient.invalidateQueries({ queryKey: ["iam-partner-portal-users"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmDeactivateUser(row: PartnerPortalUser) {
  dialog.warning({
    title: "Подтвердите деактивацию",
    content: `Пользователь "${row.username}" (${row.partner_id}) будет деактивирован — вход по паролю станет невозможен.`,
    positiveText: "Деактивировать",
    negativeText: "Отмена",
    onPositiveClick: () => deactivateUserMutation.mutate(row),
  });
}

const userColumns: DataTableColumns<PartnerPortalUser> = [
  { title: "external_id", key: "external_id" },
  { title: "username", key: "username" },
  { title: "partner_id", key: "partner_id" },
  { title: "display_name", key: "display_name" },
  {
    title: "статус",
    key: "active",
    render: (row) => h(NTag, { type: row.active ? "success" : "default", size: "small" }, () => (row.active ? "активен" : "деактивирован")),
  },
  { title: "created_at", key: "created_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      row.active
        ? h(
            NButton,
            { size: "small", type: "error", loading: deactivateUserMutation.isPending.value, onClick: () => confirmDeactivateUser(row) },
            () => "Деактивировать",
          )
        : null,
  },
];

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
      <NCard title="Partner Portal Users — создать пользователя">
        <NForm inline label-placement="top">
          <NFormItem label="username">
            <NInput v-model:value="newUserUsername" style="width: 200px" />
          </NFormItem>
          <NFormItem label="пароль">
            <NInput v-model:value="newUserPassword" type="password" show-password-on="click" style="width: 200px" />
          </NFormItem>
          <NFormItem label="partner_id">
            <NInput v-model:value="newUserPartnerId" style="width: 160px" />
          </NFormItem>
          <NFormItem label="display_name">
            <NInput v-model:value="newUserDisplayName" style="width: 220px" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton
              type="primary"
              :disabled="createUserFormInvalid"
              :loading="createUserMutation.isPending.value"
              @click="createUserMutation.mutate()"
            >
              Создать
            </NButton>
          </NFormItem>
        </NForm>
        <NAlert type="info" style="margin-top: 8px">
          Создаёт логин-пару в iam.partner_portal_users — единственный способ произвести реально логинящегося в partner-portal-ui пользователя. Роли назначаются отдельно, в карточке ниже.
        </NAlert>
      </NCard>

      <NCard title="Partner Portal Users — список">
        <NAlert v-if="usersQuery.isError.value" type="error" style="margin-bottom: 12px">
          {{ extractErrorMessage(usersQuery.error.value) }}
        </NAlert>
        <NDataTable
          :columns="userColumns"
          :data="usersQuery.data.value?.users ?? []"
          :loading="usersQuery.isLoading.value"
          :row-key="(row: PartnerPortalUser) => row.external_id"
        />
      </NCard>

      <NCard title="Partner Users — назначить роль">
        <NForm inline label-placement="top">
          <NFormItem label="external_id">
            <NInput v-model:value="newExternalId" style="width: 240px" placeholder="external_id из списка выше (= username)" />
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
          external_id должен уже существовать в iam.partner_portal_users (создаётся в карточке выше) — назначение роли неизвестному пользователю вернёт 404.
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
