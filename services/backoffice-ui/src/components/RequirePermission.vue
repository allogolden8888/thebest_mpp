<script setup lang="ts">
// Mirrors RequireAdmin.vue's structure/reasoning exactly (see that file's
// doc comment for the full rationale on defense-in-depth) but gates on a
// fine-grained server-side permission string (`auth.hasPermission`, backed
// by GET /v1/me — see stores/auth.ts) instead of the JWT-decoded
// `realm_access.roles` claim `auth.isAdmin()` uses.
//
// Same "not a security boundary on its own" caveat as RequireAdmin.vue:
// the UI never verifies the token's signature or re-derives permissions
// from anything the client can't forge — the backend's own permission
// check on every request (iam-service-backed authorization in
// backoffice-api) is the real enforcement. This exists purely so a
// legitimate user without this permission gets a clear "insufficient
// permissions" state instead of (a) seeing controls they can't use, or
// (b) a confusing raw 403 after clicking through a form. Menu entries are
// already hidden for users lacking the permission (see App.vue) — this is
// the fallback for direct URL/bookmark/back-button access that bypasses
// the menu entirely.
import { NResult, NButton } from "naive-ui";
import { useRouter } from "vue-router";
import { useAuthStore } from "../stores/auth";

const props = defineProps<{ permission: string }>();

const auth = useAuthStore();
const router = useRouter();
</script>

<template>
  <slot v-if="auth.hasPermission(props.permission)" />
  <NResult
    v-else
    status="403"
    title="Недостаточно прав"
    :description="`Для этого раздела требуется право \`${props.permission}\`. Обратитесь к администратору, если считаете, что это ошибка.`"
  >
    <template #footer>
      <NButton @click="router.push('/config')">На главную</NButton>
    </template>
  </NResult>
</template>
