<script setup lang="ts">
import { computed, h, watch } from "vue";
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
import { useApi } from "./api/useApi";

const auth = useAuthStore();
const route = useRoute();
const api = useApi();

// Phase 0 (iam-service) gap — `auth.permissions` (server-side, GET /v1/me)
// isn't a JWT claim, so it can't be decoded client-side like `auth.roles`
// is. App.vue is the one component that's always mounted for the whole
// lifetime of an authenticated session (it's the root layout — see
// `<RouterView>` below), so a `watch` here on `auth.token` (immediate: true)
// covers both cases in one place: a fresh login (token goes null -> value)
// and a page reload with an already-persisted token (token is non-null on
// first run, `immediate: true` fires right away). Judgment call: menu items
// gated on `hasPermission(...)` (below) will briefly not show while this
// fetch is in flight — no loading spinner in the nav for that gap — since
// the contract here is only about correctness (fail-closed, never shows a
// permission-gated entry before we've actually confirmed it), not about
// eliminating a sub-second flash; `RequirePermission.vue` is the real
// enforcement layer for direct-URL access regardless.
watch(
  () => auth.token,
  (token) => {
    if (token && !auth.meLoaded) {
      void auth.fetchMe(api);
    }
  },
  { immediate: true },
);

// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1a) — раньше
// это меню было статическим массивом, показанным одинаково любому
// аутентифицированному пользователю: ни одна ветка не скрывала Execution
// Control/Force Scheduler Command по роли. `adminOnly` пункты теперь
// отфильтровываются для не-admin токенов (defense in depth дополняется
// RequireAdmin.vue на уровне самих views — на случай прямого перехода по
// URL/закладке, минуя меню).
// `permission` (fine-grained server-side, GET /v1/me — see stores/auth.ts)
// kept alongside the pre-existing `adminOnly` (JWT realm_access.roles) so
// every entry below has an identical shape (both fields present, undefined
// where not applicable) — this keeps TS inference for `allMenuOptions`
// structural/uniform, same as before this change, rather than introducing a
// named interface that has to be independently reconciled against
// naive-ui's own (union) `MenuOption` prop type.
const allMenuOptions = [
  { label: () => h(RouterLink, { to: "/config" }, () => "Configuration"), key: "config", adminOnly: false, permission: undefined as string | undefined },
  {
    label: () => h(RouterLink, { to: "/categories" }, () => "Categories"),
    key: "categories",
    adminOnly: false,
    // Тот же config:write gate на POST/архивации, что и ConfigView.vue —
    // не гейтится клиентски (ConfigView.vue сам этого не делает для
    // config:write, только для отдельного credentials:issue блока),
    // GET-браузинг открыт любому валидному токену.
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/ctns" }, () => "CTN"),
    key: "ctns",
    adminOnly: false,
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/guides" }, () => "Guides"),
    key: "guides",
    adminOnly: false,
    // BACKOFFICE_DESIGN_SPEC.md Экран 41. Тот же generic config-version
    // паттерн, что Categories/CTN выше — GET без gate, POST/архивация не
    // гейтятся клиентски (см. их комментарии).
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/pattern-placeholders" }, () => "Pattern Placeholders"),
    key: "pattern-placeholders",
    adminOnly: false,
    // Экран 36. Тот же generic config-version паттерн, что Categories/CTN/
    // Guides выше — GET без gate, POST/архивация не гейтятся клиентски.
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/execution-control" }, () => "Execution Control"),
    key: "execution-control",
    adminOnly: true,
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/scheduler" }, () => "Force Scheduler Command"),
    key: "scheduler",
    adminOnly: true,
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/messages" }, () => "Messages"),
    key: "messages",
    adminOnly: false,
    permission: "support:trace" as string | undefined,
  },
  { label: () => h(RouterLink, { to: "/dlq" }, () => "DLQ / Replay"), key: "dlq", adminOnly: false, permission: undefined as string | undefined },
  {
    label: () => h(RouterLink, { to: "/reconciliation" }, () => "Reconciliation"),
    key: "reconciliation",
    adminOnly: false,
    permission: undefined as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/billing" }, () => "Billing"),
    key: "billing",
    adminOnly: false,
    // backoffice-api/internal/httpapi/router.go: /v1/billing/* гейтится
    // audit:read (нет отдельного billing:read в migrations/V025__iam.sql,
    // см. billing.go — тот же класс решения, не выдумывать право в обход
    // IAM-схемы на фронте).
    permission: "audit:read" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/blacklist" }, () => "Blacklist"),
    key: "blacklist",
    adminOnly: false,
    // backoffice-api/internal/httpapi/router.go: GET /v1/compliance/consent
    // без gate (compliance-api само уже так решило для read) — сам экран
    // виден всем, ручная блокировка внутри него отдельно гейтится
    // compliance:write (RequirePermission внутри BlacklistView.vue).
    permission: undefined as string | undefined,
  },
  { label: () => h(RouterLink, { to: "/reports" }, () => "Reports"), key: "reports", adminOnly: false, permission: undefined as string | undefined },
  {
    label: () => h(RouterLink, { to: "/access-control" }, () => "Users & Roles"),
    key: "access-control",
    adminOnly: false,
    permission: "iam:manage" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/partner-users" }, () => "Partner Users"),
    key: "partner-users",
    adminOnly: false,
    permission: "iam:manage" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/roles" }, () => "Roles"),
    key: "roles",
    adminOnly: false,
    permission: "iam:manage" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/admin-users" }, () => "Admin Users"),
    key: "admin-users",
    adminOnly: false,
    permission: "iam:manage" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/audit" }, () => "Audit Log"),
    key: "audit",
    adminOnly: false,
    permission: "audit:read" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/incidents" }, () => "Incidents"),
    key: "incidents",
    adminOnly: false,
    permission: "incident:manage" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/chat" }, () => "Chat"),
    key: "chat",
    adminOnly: false,
    permission: "chat:write" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/ops-health" }, () => "Ops Health"),
    key: "ops-health",
    adminOnly: false,
    permission: "ops:read" as string | undefined,
  },
  {
    label: () => h(RouterLink, { to: "/operator-routes" }, () => "Operator Routes"),
    key: "operator-routes",
    adminOnly: false,
    // backoffice-api/internal/httpapi/router.go: GET /v1/operators/routes
    // гейтится ops:read — та же видимость живого инфраструктурного
    // состояния, что Ops Health, не отдельное operators:read право.
    permission: "ops:read" as string | undefined,
  },
];

const menuOptions = computed(() =>
  allMenuOptions.filter((o) => (!o.adminOnly || auth.isAdmin()) && (!o.permission || auth.hasPermission(o.permission))),
);

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
