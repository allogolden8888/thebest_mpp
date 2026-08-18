<script setup lang="ts">
// Defense in depth для partner-viewer, дошедшего до мутирующего раздела
// напрямую по URL (меню уже скрывает пункт, см. App.vue, но прямая
// ссылка/закладка/кнопка "назад" обходит меню) — тот же паттерн, что
// services/backoffice-ui/src/components/RequireAdmin.vue. Реальная граница
// — auth.RequireAdmin на стороне partner-self-service-api
// (internal/auth/jwt.go), это только понятный UX вместо голого 403.
import { NResult, NButton } from "naive-ui";
import { useRouter } from "vue-router";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";

const auth = useAuthStore();
const router = useRouter();
</script>

<template>
  <slot v-if="auth.isAdmin()" />
  <NResult
    v-else
    status="403"
    title="Недостаточно прав"
    :description="`Для этого раздела требуется роль \`${PARTNER_ADMIN_ROLE}\`. Обратитесь к администратору вашей компании, если считаете, что это ошибка.`"
  >
    <template #footer>
      <NButton @click="router.push('/applications')">На главную</NButton>
    </template>
  </NResult>
</template>
