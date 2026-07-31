<script setup lang="ts">
// Ручной ввод JWT — см. README "Что НЕ реализовано": полноценный Keycloak
// OIDC redirect flow не реализован в этом срезе, только хранение/использование
// уже выпущенного токена (services_specifictaion.md §8.3 "Keycloak OIDC").
import { ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { NCard, NInput, NButton, NSpace, NAlert } from "naive-ui";
import { useAuthStore } from "../stores/auth";

const auth = useAuthStore();
const router = useRouter();
const route = useRoute();
const tokenInput = ref("");
// CODE_REVIEW.md HIGH finding #4 — сигнал "сессия истекла", а не тихий
// редирект на /login без объяснения (src/api/client.ts onUnauthorized).
const sessionExpired = route.query.sessionExpired === "1";

function submit() {
  if (!tokenInput.value.trim()) return;
  auth.setToken(tokenInput.value.trim());
  const redirect = (route.query.redirect as string) ?? "/config";
  router.push(redirect);
}
</script>

<template>
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh">
    <NCard title="MPP Backoffice — вход" style="width: 480px">
      <NSpace vertical>
        <NAlert v-if="sessionExpired" type="warning" title="Сессия истекла">
          Токен недействителен или истёк — войдите заново.
        </NAlert>
        <NInput
          v-model:value="tokenInput"
          type="textarea"
          placeholder="Вставьте JWT (Keycloak access_token)"
          :autosize="{ minRows: 3, maxRows: 6 }"
        />
        <NButton type="primary" block @click="submit">Войти</NButton>
      </NSpace>
    </NCard>
  </div>
</template>