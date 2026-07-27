<script setup lang="ts">
// Ручной ввод JWT — см. README "Что НЕ реализовано": полноценный Keycloak
// OIDC redirect flow не реализован в этом срезе, только хранение/использование
// уже выпущенного токена (services_specifictaion.md §8.3 "Keycloak OIDC").
import { ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { NCard, NInput, NButton, NSpace } from "naive-ui";
import { useAuthStore } from "../stores/auth";

const auth = useAuthStore();
const router = useRouter();
const route = useRoute();
const tokenInput = ref("");

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