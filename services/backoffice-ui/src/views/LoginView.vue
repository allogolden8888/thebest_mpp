<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 33 "Admin users" — реальный username/
// password вход через POST /v1/auth/login, заменяет прежний "вставь JWT
// вручную в textarea" (см. git history этого файла). Полноценный Keycloak
// OIDC redirect flow всё ещё не реализован (services_specifictaion.md
// §8.3) — это временное локальное решение до него (BACKOFFICE_ROADMAP.md
// "Admin users"), не финальная архитектура.
import { ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { NCard, NInput, NFormItem, NButton, NSpace, NAlert } from "naive-ui";
import { useAuthStore } from "../stores/auth";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";

const auth = useAuthStore();
const router = useRouter();
const route = useRoute();
const api = useApi();

const username = ref("");
const password = ref("");
const loading = ref(false);
const loginError = ref<string | null>(null);

// CODE_REVIEW.md HIGH finding #4 — сигнал "сессия истекла", а не тихий
// редирект на /login без объяснения (src/api/client.ts onUnauthorized).
const sessionExpired = route.query.sessionExpired === "1";

async function submit() {
  if (!username.value.trim() || !password.value) return;
  loading.value = true;
  loginError.value = null;
  try {
    const { data, error } = await api.POST("/auth/login", {
      body: { username: username.value.trim(), password: password.value },
    });
    if (error) throw error;
    auth.setToken(data.token);
    const redirect = (route.query.redirect as string) ?? "/config";
    router.push(redirect);
  } catch (err) {
    loginError.value = extractErrorMessage(err);
  } finally {
    loading.value = false;
  }
}
</script>

<template>
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh">
    <NCard title="MPP Backoffice — вход" style="width: 420px">
      <NSpace vertical>
        <NAlert v-if="sessionExpired" type="warning" title="Сессия истекла">
          Токен недействителен или истёк — войдите заново.
        </NAlert>
        <NAlert v-if="loginError" type="error">{{ loginError }}</NAlert>
        <NFormItem label="Логин">
          <NInput v-model:value="username" placeholder="username" @keyup.enter="submit" />
        </NFormItem>
        <NFormItem label="Пароль">
          <NInput v-model:value="password" type="password" show-password-on="click" @keyup.enter="submit" />
        </NFormItem>
        <NButton type="primary" block :loading="loading" :disabled="!username.trim() || !password" @click="submit">
          Войти
        </NButton>
      </NSpace>
    </NCard>
  </div>
</template>
