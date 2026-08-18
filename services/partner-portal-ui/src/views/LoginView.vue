<script setup lang="ts">
// Ручной ввод JWT — тот же компромисс, что services/backoffice-ui
// (полноценный Keycloak OIDC redirect flow не реализован в этом срезе, см.
// план: "новый Keycloak realm/client для партнёрских людей-пользователей —
// координация вне этого репозитория").
import { ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { NCard, NInput, NButton, NSpace, NAlert } from "naive-ui";
import { useAuthStore } from "../stores/auth";

const auth = useAuthStore();
const router = useRouter();
const route = useRoute();
const tokenInput = ref("");
const sessionExpired = route.query.sessionExpired === "1";

function submit() {
  if (!tokenInput.value.trim()) return;
  auth.setToken(tokenInput.value.trim());
  const redirect = (route.query.redirect as string) ?? "/applications";
  router.push(redirect);
}
</script>

<template>
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh">
    <NCard title="MPP Partner Portal — вход" style="width: 480px">
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
