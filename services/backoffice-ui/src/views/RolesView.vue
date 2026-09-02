<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 34 "Roles" — тот же GET /v1/iam/roles,
// что уже использует AccessControlView.vue ("Users & Roles" — матрица
// прав), просто другой UI-паттерн: список ролей слева, дуал-лист
// "Доступно / Назначено" справа для выбранной роли. 0 backend — read-only
// визуализация, не редактор. Права роли сегодня фиксированы seed-данными
// миграции (migrations/V025__iam.sql), никакого RPC изменить состав прав
// роли не существует — построить здесь интерактивный drag&drop
// "перенеси право между списками" означало бы либо фейковый (не
// сохраняющийся) виджет, либо архитектурное решение (новые RPC на
// AssignPermissionToRole/RevokePermissionFromRole, миграция под
// append/revoke вместо статичного seed) — то самое, что план явно просит
// не додумывать за пользователя. "Доступно" здесь — все права,
// встречающиеся хоть у одной другой роли, которых нет у выбранной, не
// исчерпывающий каталог всех возможных прав в системе (такого списка
// нигде не существует отдельно от факта "какая-то роль его несёт").
import { computed, ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NAlert, NList, NListItem, NEmpty, NText } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type IamRole = components["schemas"]["IamRole"];

const api = useApi();
const auth = useAuthStore();
const hasIamManage = computed(() => auth.hasPermission("iam:manage"));

const rolesQuery = useQuery({
  queryKey: ["iam-roles"],
  enabled: hasIamManage,
  queryFn: async () => {
    const { data, error } = await api.GET("/iam/roles");
    if (error) throw error;
    return data;
  },
});

const selectedRoleName = ref<string | null>(null);

const roles = computed(() => rolesQuery.data.value?.roles ?? []);

const selectedRole = computed<IamRole | undefined>(() => {
  if (selectedRoleName.value) {
    return roles.value.find((r) => r.name === selectedRoleName.value);
  }
  return roles.value[0];
});

const allKnownPermissions = computed(() => {
  const set = new Set<string>();
  for (const r of roles.value) {
    for (const p of r.permissions) set.add(p);
  }
  return set;
});

const assignedPermissions = computed(() => selectedRole.value?.permissions ?? []);

const availablePermissions = computed(() => {
  const assigned = new Set(assignedPermissions.value);
  return [...allKnownPermissions.value].filter((p) => !assigned.has(p)).sort();
});
</script>

<template>
  <RequirePermission permission="iam:manage">
    <NCard title="Roles">
      <NAlert v-if="rolesQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(rolesQuery.error.value) }}
      </NAlert>
      <div style="display: grid; grid-template-columns: 240px minmax(0, 1fr); gap: 16px">
        <NList bordered :show-divider="true" style="max-height: 480px; overflow-y: auto">
          <NListItem
            v-for="role in roles"
            :key="role.name"
            style="cursor: pointer; padding: 10px 14px"
            :style="{ background: (selectedRole?.name === role.name) ? 'rgba(255,255,255,0.06)' : undefined }"
            @click="selectedRoleName = role.name"
          >
            <NText :strong="selectedRole?.name === role.name">{{ role.name }}</NText>
          </NListItem>
          <NEmpty v-if="!rolesQuery.isLoading.value && roles.length === 0" description="Ролей нет" style="padding: 24px" />
        </NList>

        <div v-if="selectedRole">
          <NText depth="3" style="display: block; margin-bottom: 12px">
            Права роли "{{ selectedRole.name }}"<template v-if="selectedRole.description"> — {{ selectedRole.description }}</template>
          </NText>
          <div style="display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px">
            <NCard title="Доступно" size="small">
              <NEmpty v-if="availablePermissions.length === 0" description="Нет других прав в системе" />
              <div v-for="p in availablePermissions" :key="p" style="padding: 6px 0; font-size: 13px">
                <NText code>{{ p }}</NText>
              </div>
            </NCard>
            <NCard title="Назначено" size="small">
              <NEmpty v-if="assignedPermissions.length === 0" description="У роли нет прав" />
              <div v-for="p in assignedPermissions" :key="p" style="padding: 6px 0; font-size: 13px">
                <NText code type="success">{{ p }}</NText>
              </div>
            </NCard>
          </div>
        </div>
      </div>
    </NCard>
  </RequirePermission>
</template>
