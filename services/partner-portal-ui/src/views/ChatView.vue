<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat" — partner-portal-ui side.
// GET/POST /chat/messages (openapi-partner.yaml, partner-self-service-api's
// internal/httpapi/chat.go) — тонкий прокси в тот же chat-service, что
// backoffice-ui's эквивалентный экран (services/backoffice-ui/src/views/
// ChatView.vue) использует со стороны админа.
//
// Отличие от backoffice-ui: здесь НЕТ пикера "какой партнёр" — партнёр
// видит только свой единственный тред, partner_id всегда берётся бэкендом
// из JWT (см. chat.go doc-комментарий), а не из URL/query. Соответственно
// нет и /chat/threads-эндпоинта на этой стороне (ListThreads не вызывается
// partner-self-service-api вообще — см. platform-contracts/grpc/chat.proto
// ChatService.ListThreads docstring).
//
// Polling через TanStack Query refetchInterval — тот же механизм и тот же
// интервал, что backoffice-ui's ChatView.vue (первая polling-view в этом
// UI, выбор согласован между обеими сторонами одного экрана). Та же
// сознательная граница объёма: без read-receipts UI-полировки, без typing
// indicators, без file attachments; каждый poll перезапрашивает тред
// целиком (since не передаётся с фронтенда) — тред поддержки мал, полный
// рефетч каждые несколько секунд дешевле, чем ручной merge курсора поверх
// TanStack Query.
import { computed, nextTick, ref, watch } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NEmpty, NInput, NButton, NAlert, NSpace } from "naive-ui";
import { useMessage } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema-partner";

type ChatMessage = components["schemas"]["ChatMessage"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();
const auth = useAuthStore();

// 4с — тот же интервал, что backoffice-ui's ChatView.vue (см. doc-
// комментарий там: нет установленного прецедента в кодбейзе шире этого
// экрана, выбрано по аналогии с типичными chat-widget интервалами).
const POLL_INTERVAL_MS = 4000;

const messagesQuery = useQuery({
  queryKey: ["chat-messages"],
  refetchInterval: POLL_INTERVAL_MS,
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/chat/messages", {});
    if (error) throw error;
    return data;
  },
});

const messages = computed<ChatMessage[]>(() => messagesQuery.data.value?.messages ?? []);

const newMessageBody = ref("");
const threadContainer = ref<HTMLElement | null>(null);

function scrollToBottom() {
  nextTick(() => {
    if (threadContainer.value) {
      threadContainer.value.scrollTop = threadContainer.value.scrollHeight;
    }
  });
}

watch(messages, scrollToBottom);

const sendMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.partner.POST("/chat/messages", {
      body: { body: newMessageBody.value.trim() },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    newMessageBody.value = "";
    queryClient.invalidateQueries({ queryKey: ["chat-messages"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function sendMessage() {
  if (!newMessageBody.value.trim()) return;
  sendMutation.mutate();
}
</script>

<template>
  <NCard title="Chat с поддержкой" style="height: calc(100vh - 160px); display: flex; flex-direction: column" content-style="flex: 1; display: flex; flex-direction: column; min-height: 0">
    <NAlert v-if="messagesQuery.isError.value" type="error" style="margin-bottom: 12px">
      {{ extractErrorMessage(messagesQuery.error.value) }}
    </NAlert>
    <div ref="threadContainer" style="flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 10px; margin-bottom: 12px; min-height: 0">
      <NEmpty v-if="!messagesQuery.isLoading.value && messages.length === 0" description="Сообщений пока нет — напишите в поддержку ниже" />
      <div
        v-for="m in messages"
        :key="m.id"
        :style="{
          alignSelf: m.sender_type === 'partner' ? 'flex-end' : 'flex-start',
          background: m.sender_type === 'partner' ? 'rgba(24,160,88,0.18)' : 'var(--n-color-embedded, #2a2a2e)',
          borderRadius: '8px',
          padding: '10px 12px',
          fontSize: '13px',
          maxWidth: '70%',
        }"
      >
        <div>{{ m.body }}</div>
        <div style="font-size: 11px; opacity: 0.6; margin-top: 4px">
          {{ m.sender_type === "admin" ? "Поддержка MPP" : auth.partnerId }} · {{ m.created_at }}
        </div>
      </div>
    </div>
    <NSpace>
      <NInput
        v-model:value="newMessageBody"
        placeholder="Сообщение…"
        style="flex: 1"
        @keydown.enter.prevent="sendMessage"
      />
      <NButton type="primary" :disabled="!newMessageBody.trim()" :loading="sendMutation.isPending.value" @click="sendMessage">
        Отправить
      </NButton>
    </NSpace>
  </NCard>
</template>
