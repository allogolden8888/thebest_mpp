<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat" — polling-чат админ<->партнёр
// поверх backoffice-api's /v1/chat/* (chat.go -> chat-service, право
// chat:write, тот же класс gate, что incident:manage — см. router.go).
//
// Polling, не websocket — ни одна другая часть этого приложения (или
// платформы вообще) не поднимает websocket-инфраструктуру; TanStack
// Query's refetchInterval — первое использование в этом кодбейзе (ни одна
// существующая view не опрашивала периодически до этого экрана), выбрано
// как самый простой механизм, согласованный с остальной архитектурой
// приложения (обычный useQuery, не отдельный WebSocket-клиент).
//
// Сознательное упрощение: каждый poll запрашивает ВЕСЬ тред заново (since
// не передаётся с фронтенда), не инкрементальные "только новые сообщения"
// с локальным накоплением курсора. backoffice-api/chat-service полностью
// поддерживают since-курсор (проверено отдельно живым curl-раунд-трипом,
// см. commit message) — здесь он не используется, потому что: (1) объём
// одного треда поддержки мал (это не high-volume лента), полный рефетч
// каждые несколько секунд дешёв; (2) инкрementальное накопление на клиенте
// потребовало бы ручного merge-состояния поверх TanStack Query (не
// вписывается в его обычную модель "queryFn возвращает полный снимок для
// этого queryKey"), что усложнило бы код без реальной пользы на этом
// масштабе. since остаётся востребованной возможностью API для будущих
// потребителей/большего масштаба, не мёртвым кодом.
//
// Граница объёма (задача явно исключает): без read-receipts UI-полировки
// (двойные галочки и т.п. — read_at используется только для unread_count в
// сайдбаре), без typing indicators, без file attachments — только
// plain-text сообщения, как показывает design-референс.
import { computed, nextTick, ref, watch } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NEmpty, NInput, NButton, NAlert, NBadge, NSpace, NText, useMessage } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type ChatThread = components["schemas"]["ChatThread"];
type ChatMessage = components["schemas"]["ChatMessage"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();

// Опрос каждые 4с — часто достаточно, чтобы ощущаться "живым" в чате
// поддержки, не настолько часто, чтобы бомбить backoffice-api на каждом
// открытом табе. Нет установленного прецедента в этом кодбейзе (первая
// polling-view) — выбрано по аналогии с типичными chat-widget интервалами,
// не скопировано ниоткуда.
const POLL_INTERVAL_MS = 4000;

const threadsQuery = useQuery({
  queryKey: ["chat-threads"],
  refetchInterval: POLL_INTERVAL_MS,
  queryFn: async () => {
    const { data, error } = await api.GET("/chat/threads", {});
    if (error) throw error;
    return data;
  },
});

const threads = computed<ChatThread[]>(() => threadsQuery.data.value?.threads ?? []);

const selectedPartnerId = ref<string | null>(null);

// Автовыбор первого треда — не оставляем панель сообщений пустой, если
// есть хотя бы один партнёр с историей переписки.
watch(
  threads,
  (list) => {
    if (selectedPartnerId.value === null && list.length > 0) {
      selectedPartnerId.value = list[0].partner_id;
    }
  },
  { immediate: true },
);

function selectThread(partnerId: string) {
  selectedPartnerId.value = partnerId;
}

const messagesQuery = useQuery({
  queryKey: ["chat-messages", selectedPartnerId],
  enabled: computed(() => selectedPartnerId.value !== null),
  refetchInterval: POLL_INTERVAL_MS,
  queryFn: async () => {
    const { data, error } = await api.GET("/chat/{partner_id}/messages", {
      params: { path: { partner_id: selectedPartnerId.value as string } },
    });
    if (error) throw error;
    return data;
  },
});

const messages = computed<ChatMessage[]>(() => messagesQuery.data.value?.messages ?? []);

// Читая тред (даже просто открыв его выше), backend уже пометил партнёрские
// сообщения read_at как побочный эффект GET — invalidate сайдбара, чтобы
// счётчик "N новых" обновился без ожидания следующего poll-тика сайдбара.
watch(messages, () => {
  queryClient.invalidateQueries({ queryKey: ["chat-threads"] });
});

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
    const partnerId = selectedPartnerId.value;
    if (!partnerId) throw new Error("партнёр не выбран");
    const { data, error } = await api.POST("/chat/{partner_id}/messages", {
      params: { path: { partner_id: partnerId } },
      body: { body: newMessageBody.value.trim() },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    newMessageBody.value = "";
    queryClient.invalidateQueries({ queryKey: ["chat-messages", selectedPartnerId.value] });
    queryClient.invalidateQueries({ queryKey: ["chat-threads"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function sendMessage() {
  if (!newMessageBody.value.trim()) return;
  sendMutation.mutate();
}
</script>

<template>
  <RequirePermission permission="chat:write">
    <div style="display: grid; grid-template-columns: 280px minmax(0, 1fr); gap: 16px; height: calc(100vh - 160px)">
      <NCard title="Партнёры" content-style="padding: 0; overflow-y: auto" style="height: 100%">
        <NAlert v-if="threadsQuery.isError.value" type="error" style="margin: 12px">
          {{ extractErrorMessage(threadsQuery.error.value) }}
        </NAlert>
        <NEmpty v-else-if="!threadsQuery.isLoading.value && threads.length === 0" description="Пока нет ни одного сообщения" style="margin-top: 24px" />
        <div
          v-for="t in threads"
          :key="t.partner_id"
          :style="{
            padding: '12px 16px',
            cursor: 'pointer',
            borderBottom: '1px solid var(--n-border-color, #333)',
            background: t.partner_id === selectedPartnerId ? 'rgba(24,160,88,0.12)' : 'transparent',
          }"
          @click="selectThread(t.partner_id)"
        >
          <NSpace justify="space-between" align="center">
            <NText strong>{{ t.partner_id }}</NText>
            <NBadge v-if="t.unread_count > 0" :value="t.unread_count" type="success" />
          </NSpace>
          <NText depth="3" style="font-size: 12px; display: block; white-space: nowrap; overflow: hidden; text-overflow: ellipsis">
            {{ t.last_message_body }}
          </NText>
        </div>
      </NCard>

      <NCard :title="selectedPartnerId ?? 'Chat'" style="height: 100%; display: flex; flex-direction: column" content-style="flex: 1; display: flex; flex-direction: column; min-height: 0">
        <template v-if="selectedPartnerId">
          <NAlert v-if="messagesQuery.isError.value" type="error" style="margin-bottom: 12px">
            {{ extractErrorMessage(messagesQuery.error.value) }}
          </NAlert>
          <div ref="threadContainer" style="flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 10px; margin-bottom: 12px; min-height: 0">
            <NEmpty v-if="!messagesQuery.isLoading.value && messages.length === 0" description="Сообщений пока нет" />
            <div
              v-for="m in messages"
              :key="m.id"
              :style="{
                alignSelf: m.sender_type === 'admin' ? 'flex-end' : 'flex-start',
                background: m.sender_type === 'admin' ? 'rgba(24,160,88,0.18)' : 'var(--n-color-embedded, #2a2a2e)',
                borderRadius: '8px',
                padding: '10px 12px',
                fontSize: '13px',
                maxWidth: '70%',
              }"
            >
              <div>{{ m.body }}</div>
              <div style="font-size: 11px; opacity: 0.6; margin-top: 4px">{{ m.sender_id }} · {{ m.created_at }}</div>
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
        </template>
        <NEmpty v-else description="Выберите партнёра слева" />
      </NCard>
    </div>
  </RequirePermission>
</template>
