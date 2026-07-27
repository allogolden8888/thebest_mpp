<script setup lang="ts">
import { computed, h } from "vue";
import { RouterLink, RouterView, useRoute } from "vue-router";
import { NConfigProvider, NLayout, NLayoutSider, NLayoutHeader, NMenu, NButton, darkTheme } from "naive-ui";
import { useAuthStore } from "./stores/auth";

const auth = useAuthStore();
const route = useRoute();

const menuOptions = [
  { label: () => h(RouterLink, { to: "/config" }, () => "Configuration"), key: "config" },
  { label: () => h(RouterLink, { to: "/execution-control" }, () => "Execution Control"), key: "execution-control" },
  { label: () => h(RouterLink, { to: "/scheduler" }, () => "Force Scheduler Command"), key: "scheduler" },
  { label: () => h(RouterLink, { to: "/dlq" }, () => "DLQ / Replay"), key: "dlq" },
  { label: () => h(RouterLink, { to: "/reconciliation" }, () => "Reconciliation"), key: "reconciliation" },
  { label: () => h(RouterLink, { to: "/reports" }, () => "Reports"), key: "reports" },
];

const activeKey = computed(() => (route.name as string) ?? null);
</script>

<template>
  <NConfigProvider :theme="darkTheme">
    <NLayout v-if="auth.isAuthenticated" style="height: 100vh">
      <NLayoutHeader bordered style="padding: 12px 16px; display: flex; justify-content: space-between; align-items: center">
        <strong>MPP Backoffice</strong>
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
  </NConfigProvider>
</template>
