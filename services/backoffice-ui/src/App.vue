<script setup lang="ts">
import { computed, h } from "vue";
import { RouterLink, RouterView, useRoute } from "vue-router";
import {
  NConfigProvider,
  NLayout,
  NLayoutSider,
  NLayoutHeader,
  NMenu,
  NButton,
  NTag,
  NMessageProvider,
  NDialogProvider,
  darkTheme,
} from "naive-ui";
import { useAuthStore, ADMIN_ROLE } from "./stores/auth";

const auth = useAuthStore();
const route = useRoute();

// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1a) — раньше
// это меню было статическим массивом, показанным одинаково любому
// аутентифицированному пользователю: ни одна ветка не скрывала Execution
// Control/Force Scheduler Command по роли. `adminOnly` пункты теперь
// отфильтровываются для не-admin токенов (defense in depth дополняется
// RequireAdmin.vue на уровне самих views — на случай прямого перехода по
// URL/закладке, минуя меню).
const allMenuOptions = [
  { label: () => h(RouterLink, { to: "/config" }, () => "Configuration"), key: "config", adminOnly: false },
  {
    label: () => h(RouterLink, { to: "/execution-control" }, () => "Execution Control"),
    key: "execution-control",
    adminOnly: true,
  },
  {
    label: () => h(RouterLink, { to: "/scheduler" }, () => "Force Scheduler Command"),
    key: "scheduler",
    adminOnly: true,
  },
  { label: () => h(RouterLink, { to: "/dlq" }, () => "DLQ / Replay"), key: "dlq", adminOnly: false },
  { label: () => h(RouterLink, { to: "/reconciliation" }, () => "Reconciliation"), key: "reconciliation", adminOnly: false },
  { label: () => h(RouterLink, { to: "/reports" }, () => "Reports"), key: "reports", adminOnly: false },
];

const menuOptions = computed(() => allMenuOptions.filter((o) => !o.adminOnly || auth.isAdmin()));

const activeKey = computed(() => (route.name as string) ?? null);
</script>

<template>
  <NConfigProvider :theme="darkTheme">
    <NMessageProvider>
      <NDialogProvider>
        <NLayout v-if="auth.isAuthenticated" style="height: 100vh">
          <NLayoutHeader
            bordered
            style="padding: 12px 16px; display: flex; justify-content: space-between; align-items: center"
          >
            <div style="display: flex; align-items: center; gap: 8px">
              <strong>MPP Backoffice</strong>
              <NTag :type="auth.isAdmin() ? 'warning' : 'default'" size="small" round>
                {{ auth.isAdmin() ? ADMIN_ROLE : "read-only" }}
              </NTag>
            </div>
            <NButton size="small" @click="auth.clearToken()">Выйти</NButton>
          </NLayoutHeader>
          <NLayout has-sider style="height: calc(100vh - 49px)">
            <NLayoutSider bordered collapse-mode="width" :collapsed-width="0" :width="220">
              <NMenu :value="activeKey" :options="menuOptions" />
            </NLayoutSider>
            <NLayout content-style="padding: 24px">
              <RouterView />
            </NLayout>
          </NLayout>
        </NLayout>
        <RouterView v-else />
      </NDialogProvider>
    </NMessageProvider>
  </NConfigProvider>
</template>
