<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 33 "Admin users" — полноценное локальное
// управление аккаунтами сотрудников бэкофиса (создать/деактивировать),
// заменяет отсутствовавшую ранее возможность (LDAP пропущен по явной
// просьбе, реальный Keycloak — сильно позже, см. BACKOFFICE_ROADMAP.md).
// Роли этим сотрудникам назначаются отдельно, через уже существующий
// экран "Users & Roles" (AccessControlView.vue, iam.staff_role_
// assignments) — этот экран только про сам аккаунт (логин/пароль/
// активность), не про то, что ему разрешено.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, NAlert, NTag, useMessage, useDialog, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type StaffAccount = components["schemas"]["StaffAccount"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

const hasIamManage = computed(() => auth.hasPermission("iam:manage"));

const accountsQuery = useQuery({
  queryKey: ["staff-accounts"],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/staff-accounts", { params: { query: {} } });
    if (error) throw error;
    return data;
  },
});

const newUsername = ref("");
const newPassword = ref("");
const newDisplayName = ref("");
const createFormInvalid = computed(() => !newUsername.value.trim() || !newPassword.value || !newDisplayName.value.trim());

const createMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/iam/staff-accounts", {
      body: { username: newUsername.value.trim(), password: newPassword.value, display_name: newDisplayName.value.trim() },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Сотрудник создан");
    newUsername.value = "";
    newPassword.value = "";
    newDisplayName.value = "";
    queryClient.invalidateQueries({ queryKey: ["staff-accounts"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const deactivateMutation = useMutation({
  mutationFn: async (row: StaffAccount) => {
    const { data, error } = await api.POST("/iam/staff-accounts/{external_id}/deactivate", {
      params: { path: { external_id: row.external_id } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    if (data?.deactivated) {
      message.success("Аккаунт деактивирован");
    } else {
      message.info("Аккаунт уже был неактивен");
    }
    queryClient.invalidateQueries({ queryKey: ["staff-accounts"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmDeactivate(row: StaffAccount) {
  dialog.warning({
    title: "Подтвердите деактивацию",
    content: `Аккаунт "${row.username}" будет деактивирован — вход по паролю станет невозможен.`,
    positiveText: "Деактивировать",
    negativeText: "Отмена",
    onPositiveClick: () => deactivateMutation.mutate(row),
  });
}

const columns: DataTableColumns<StaffAccount> = [
  { title: "username", key: "username" },
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
            { size: "small", type: "error", loading: deactivateMutation.isPending.value, onClick: () => confirmDeactivate(row) },
            () => "Деактивировать",
          )
        : null,
  },
];
</script>

<template>
  <RequirePermission permission="iam:manage">
    <NSpace vertical size="large">
      <NCard title="Admin Users — создать сотрудника">
        <NForm inline label-placement="top">
          <NFormItem label="username">
            <NInput v-model:value="newUsername" style="width: 200px" />
          </NFormItem>
          <NFormItem label="пароль">
            <NInput v-model:value="newPassword" type="password" show-password-on="click" style="width: 200px" />
          </NFormItem>
          <NFormItem label="display_name">
            <NInput v-model:value="newDisplayName" style="width: 220px" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton type="primary" :disabled="createFormInvalid" :loading="createMutation.isPending.value" @click="createMutation.mutate()">
              Создать
            </NButton>
          </NFormItem>
        </NForm>
        <NAlert type="info" style="margin-top: 8px">
          Роли новому сотруднику назначаются отдельно, на экране "Users & Roles".
        </NAlert>
      </NCard>

      <NCard title="Admin Users — список">
        <NAlert v-if="accountsQuery.isError.value" type="error" style="margin-bottom: 12px">
          {{ extractErrorMessage(accountsQuery.error.value) }}
        </NAlert>
        <NDataTable
          :columns="columns"
          :data="accountsQuery.data.value?.accounts ?? []"
          :loading="accountsQuery.isLoading.value"
          :row-key="(row: StaffAccount) => row.external_id"
        />
      </NCard>
    </NSpace>
  </RequirePermission>
</template>
