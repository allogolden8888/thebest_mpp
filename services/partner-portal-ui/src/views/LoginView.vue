<script setup lang="ts">
// BACKOFFICE_ROADMAP.md Production Readiness Review P0#5 — real username/
// password login through POST /v1/self-service/auth/login, replacing the
// previous "paste a JWT into a textarea" flow (see git history of this
// file). Mirrors backoffice-ui's LoginView.vue exactly (same form shape,
// same error handling) — partner-self-service-api's handleLogin
// (internal/httpapi/auth.go there) is a straight port of backoffice-api's
// handleLogin onto IamService.VerifyPartnerPortalCredentials instead of
// VerifyStaffCredentials. Full Keycloak OIDC redirect flow is still not
// implemented (see internal/auth/jwt.go package doc there, "Фаза 3 плана" —
// new Keycloak realm/client for partner human users, coordination outside
// this repo) — this is the same temporary local-login bridge until that
// exists, not the final architecture.
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

const sessionExpired = route.query.sessionExpired === "1";

async function submit() {
  if (!username.value.trim() || !password.value) return;
  loading.value = true;
  loginError.value = null;
  try {
    const { data, error } = await api.partner.POST("/auth/login", {
      body: { username: username.value.trim(), password: password.value },
    });
    if (error) throw error;
    auth.setToken(data.token);
    const redirect = (route.query.redirect as string) ?? "/applications";
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
    <NCard title="MPP Partner Portal — вход" style="width: 420px">
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
