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
import { useAuthStore, PARTNER_ADMIN_ROLE } from "./stores/auth";

const auth = useAuthStore();
const route = useRoute();

// Мутирующие разделы (Applications/Senders CRUD, Credentials rotate,
// Webhook config) видны в меню только partner-admin — тот же
// defense-in-depth паттерн, что backoffice-ui: RequirePartnerAdmin.vue на
// уровне самих views перехватывает прямой переход по URL/закладке в обход
// меню. Billing/Templates — read-only, видны любой партнёрской роли.
const allMenuOptions = [
  { label: () => h(RouterLink, { to: "/applications" }, () => "Applications"), key: "applications", adminOnly: false },
  { label: () => h(RouterLink, { to: "/senders" }, () => "Senders"), key: "senders", adminOnly: false },
  { label: () => h(RouterLink, { to: "/credentials" }, () => "Credentials"), key: "credentials", adminOnly: false },
  { label: () => h(RouterLink, { to: "/webhook" }, () => "Webhook"), key: "webhook", adminOnly: false },
  { label: () => h(RouterLink, { to: "/billing" }, () => "Billing"), key: "billing", adminOnly: false },
  { label: () => h(RouterLink, { to: "/templates" }, () => "Мои шаблоны"), key: "templates", adminOnly: false },
  { label: () => h(RouterLink, { to: "/chat" }, () => "Chat"), key: "chat", adminOnly: false },
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
              <strong>MPP Partner Portal</strong>
              <NTag size="small" round>{{ auth.partnerId || "unknown partner" }}</NTag>
              <NTag :type="auth.isAdmin() ? 'warning' : 'default'" size="small" round>
                {{ auth.isAdmin() ? PARTNER_ADMIN_ROLE : "viewer" }}
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
