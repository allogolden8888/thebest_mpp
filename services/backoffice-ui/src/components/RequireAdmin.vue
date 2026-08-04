<script setup lang="ts">
// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1b) —
// defense in depth for a non-admin who reaches a destructive view directly
// by URL (menu already hides the entry, see App.vue, but a direct link/
// bookmark/back-button bypasses the menu entirely). Renders a clear
// "insufficient permissions" state instead of showing the form and letting
// every request fail with a raw 403 from the backend's own
// auth.RequireRole(auth.AdminRole) (backoffice-api/internal/auth/jwt.go).
import { NResult, NButton } from "naive-ui";
import { useRouter } from "vue-router";
import { useAuthStore, ADMIN_ROLE } from "../stores/auth";

const auth = useAuthStore();
const router = useRouter();
</script>

<template>
  <slot v-if="auth.isAdmin()" />
  <NResult
    v-else
    status="403"
    title="Недостаточно прав"
    :description="`Для этого раздела требуется роль \`${ADMIN_ROLE}\`. Обратитесь к администратору Keycloak, если считаете, что это ошибка.`"
  >
    <template #footer>
      <NButton @click="router.push('/config')">На главную</NButton>
    </template>
  </NResult>
</template>
