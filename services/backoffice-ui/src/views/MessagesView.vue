<script setup lang="ts">
// handle_message_browse + handle_message_detail (service_internal_methods.md
// §7.3) — список последних сообщений (GET /v1/messages, реальная пагинация
// через limit/offset — Пред/След, не одна статичная страница) + точечный
// поиск по id (GET /v1/support/messages/search) + деталь по клику на строку
// (GET /v1/messages/{message_id}). Деталь рендерится card'ой ниже таблицы,
// не отдельным route — тот же паттерн, что уже установлен
// IncidentsView.vue (см. doc-комментарий там за полным обоснованием).
//
// Таймлайн в деталях — реальная лента messaging.message_lifecycle_history
// (статус + occurred_at + source), НЕ пер-SMPP-PDU трасса (submit_sm_to_smsc_at
// и т.п.) — той детализации в платформе сегодня нет нигде (см. план,
// обсуждённый в чате), эта карточка не притворяется, что она есть.
import { ref, computed, h } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, NAlert, NTag, NDescriptions, NDescriptionsItem, NTimeline, NTimelineItem, NEmpty, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema";

type Message = components["schemas"]["Message"];

const api = useApi();

// --- Список / пагинация ---
const partnerId = ref("");
const currentStatus = ref("");
const page = ref(0);
const pageSize = 20;

function resetPaging() {
  page.value = 0;
}

const listQuery = useQuery({
  queryKey: ["messages-browse", partnerId, currentStatus, page],
  queryFn: async () => {
    const { data, error } = await api.GET("/messages", {
      params: {
        query: {
          partner_id: partnerId.value || undefined,
          current_status: currentStatus.value || undefined,
          limit: pageSize,
          offset: page.value * pageSize,
        },
      },
    });
    if (error) throw error;
    return data;
  },
});

// limit+1 не запрашиваем (сервер сам клампит) — "может быть следующая
// страница" определяем по тому, что текущая страница заполнена целиком;
// не точный "есть ли ещё", но без лишнего COUNT(*) на каждый список
// (messaging.message_read_model растёт неограниченно, точный total дорог
// на каждый запрос, тот же trade-off, что уже принят у DlqBrowse/
// ReconciliationBrowse — они тоже без total).
const hasNextPage = computed(() => (listQuery.data.value?.messages?.length ?? 0) === pageSize);

// --- Точечный поиск по id ---
const searchId = ref("");
const searchQuery = useQuery({
  queryKey: ["messages-search", searchId],
  enabled: () => searchId.value.length > 0,
  queryFn: async () => {
    const { data, error } = await api.GET("/support/messages/search", {
      params: { query: { message_id: searchId.value } },
    });
    if (error) throw error;
    return data;
  },
});

// --- Деталь по выбранному сообщению ---
const selectedMessageId = ref<string | null>(null);

const detailQuery = useQuery({
  queryKey: ["message-detail", selectedMessageId],
  enabled: () => selectedMessageId.value != null,
  queryFn: async () => {
    const { data, error } = await api.GET("/messages/{message_id}", {
      params: { path: { message_id: selectedMessageId.value as string } },
    });
    if (error) throw error;
    return data;
  },
});

function selectMessage(row: Message) {
  selectedMessageId.value = row.message_id;
}

const statusColor: Record<string, "default" | "info" | "success" | "warning" | "error"> = {
  RECEIVED: "default",
  SUBMITTED: "info",
  DELIVERED: "success",
  FAILED: "error",
  BLOCKED: "warning",
};

function tagType(status: string) {
  return statusColor[status] ?? "default";
}

const columns: DataTableColumns<Message> = [
  { title: "message_id", key: "message_id", ellipsis: { tooltip: true }, width: 280 },
  { title: "partner_id", key: "partner_id" },
  { title: "application_id", key: "application_id" },
  {
    title: "current_status",
    key: "current_status",
    render: (row) => h(NTag, { type: tagType(row.current_status), size: "small", round: true }, () => row.current_status),
  },
  { title: "terminal", key: "terminal", render: (row) => (row.terminal ? "да" : "нет") },
  { title: "created_at", key: "created_at" },
  { title: "updated_at", key: "updated_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) => h(NButton, { size: "small", onClick: () => selectMessage(row) }, () => "Детали"),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Поиск по message_id / trace_id">
      <NForm inline label-placement="top">
        <NFormItem label="message_id">
          <NInput v-model:value="searchId" style="width: 320px" placeholder="UUID сообщения" />
        </NFormItem>
      </NForm>
      <NAlert v-if="searchQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(searchQuery.error.value) }}
      </NAlert>
      <NDataTable
        v-if="searchId"
        :columns="columns"
        :data="searchQuery.data.value?.messages ?? []"
        :loading="searchQuery.isLoading.value"
        :row-key="(row: Message) => row.message_id"
      />
    </NCard>

    <NCard title="Последние сообщения (A2P)">
      <NForm inline label-placement="top">
        <NFormItem label="partner_id">
          <NInput v-model:value="partnerId" style="width: 220px" @update:value="resetPaging" />
        </NFormItem>
        <NFormItem label="current_status">
          <NInput v-model:value="currentStatus" placeholder="RECEIVED / SUBMITTED / DELIVERED..." style="width: 260px" @update:value="resetPaging" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton @click="listQuery.refetch()">Обновить</NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(listQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value?.messages ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: Message) => row.message_id"
        style="margin-bottom: 12px"
      />
      <NSpace justify="space-between" align="center">
        <span style="color: var(--n-text-color-3, #999)">страница {{ page + 1 }}</span>
        <NSpace>
          <NButton :disabled="page === 0" @click="page--">← Пред</NButton>
          <NButton :disabled="!hasNextPage" @click="page++">След →</NButton>
        </NSpace>
      </NSpace>
    </NCard>

    <NCard v-if="selectedMessageId != null" :title="`Сообщение ${selectedMessageId}`">
      <NAlert v-if="detailQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(detailQuery.error.value) }}
      </NAlert>
      <template v-if="detailQuery.data.value">
        <NDescriptions :column="2" bordered size="small" style="margin-bottom: 16px">
          <NDescriptionsItem label="message_id">{{ detailQuery.data.value.message.message_id }}</NDescriptionsItem>
          <NDescriptionsItem label="partner_id">{{ detailQuery.data.value.message.partner_id }}</NDescriptionsItem>
          <NDescriptionsItem label="application_id">{{ detailQuery.data.value.message.application_id }}</NDescriptionsItem>
          <NDescriptionsItem label="trace_id">{{ detailQuery.data.value.message.trace_id }}</NDescriptionsItem>
          <NDescriptionsItem label="pipeline_id">{{ detailQuery.data.value.message.pipeline_id || "—" }}</NDescriptionsItem>
          <NDescriptionsItem label="pipeline_version">{{ detailQuery.data.value.message.pipeline_version || "—" }}</NDescriptionsItem>
          <NDescriptionsItem label="current_status">
            <NTag :type="tagType(detailQuery.data.value.message.current_status)" size="small" round>
              {{ detailQuery.data.value.message.current_status }}
            </NTag>
          </NDescriptionsItem>
          <NDescriptionsItem label="terminal">{{ detailQuery.data.value.message.terminal ? "да" : "нет" }}</NDescriptionsItem>
          <NDescriptionsItem label="created_at">{{ detailQuery.data.value.message.created_at }}</NDescriptionsItem>
          <NDescriptionsItem label="updated_at">{{ detailQuery.data.value.message.updated_at }}</NDescriptionsItem>
        </NDescriptions>

        <NCard title="Таймлайн статусов (message_lifecycle_history)" size="small" embedded>
          <NEmpty v-if="!detailQuery.data.value.history?.length" description="Нет записей истории для этого сообщения" />
          <NTimeline v-else>
            <NTimelineItem
              v-for="event in detailQuery.data.value.history"
              :key="event.event_id"
              :type="tagType(event.status) === 'error' ? 'error' : tagType(event.status) === 'success' ? 'success' : 'info'"
              :title="event.status"
              :time="event.occurred_at"
              :content="`источник: ${event.source} · lifecycle_version=${event.lifecycle_version}`"
            />
          </NTimeline>
        </NCard>
      </template>
    </NCard>
  </NSpace>
</template>
