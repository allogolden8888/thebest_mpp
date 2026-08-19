<script setup lang="ts">
// handle_message_browse (service_internal_methods.md §7.3) — список
// последних сообщений (GET /v1/messages), тот же тонкий-обёрточный паттерн,
// что DlqBrowseView. Отдельная карточка ниже — точечный поиск по
// message_id/trace_id через уже существующий GET /v1/support/messages/search,
// который требует хотя бы один из двух id (см. backoffice-api's
// support.go — сознательное разграничение "browse" vs "search").
import { ref } from "vue";
import { useQuery } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, NAlert, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema";

type Message = components["schemas"]["Message"];

const api = useApi();

const partnerId = ref("");
const currentStatus = ref("");

const listQuery = useQuery({
  queryKey: ["messages-browse", partnerId, currentStatus],
  queryFn: async () => {
    const { data, error } = await api.GET("/messages", {
      params: { query: { partner_id: partnerId.value || undefined, current_status: currentStatus.value || undefined, limit: 50 } },
    });
    if (error) throw error;
    return data;
  },
});

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

const columns: DataTableColumns<Message> = [
  { title: "message_id", key: "message_id" },
  { title: "partner_id", key: "partner_id" },
  { title: "application_id", key: "application_id" },
  { title: "current_status", key: "current_status" },
  { title: "terminal", key: "terminal", render: (row) => (row.terminal ? "да" : "нет") },
  { title: "created_at", key: "created_at" },
  { title: "updated_at", key: "updated_at" },
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
          <NInput v-model:value="partnerId" style="width: 220px" />
        </NFormItem>
        <NFormItem label="current_status">
          <NInput v-model:value="currentStatus" placeholder="RECEIVED / SUBMITTED / DELIVERED..." style="width: 260px" />
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
      />
    </NCard>
  </NSpace>
</template>
