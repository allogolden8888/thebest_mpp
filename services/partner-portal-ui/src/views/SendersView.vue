<script setup lang="ts">
// GET/POST/PATCH /v1/self-service/senders — см. openapi-partner.yaml и
// services/partner-self-service-api/internal/httpapi/applications.go
// (mountSenders) за авторитетной формой.
//
// Паттерн — тот же, что ApplicationsView.vue/ConfigView.vue.
//
// У senders нет отдельного delete/update-эндпоинта — единственный способ
// поменять запись — PATCH status между active/archived (applications.go:
// handleUpdateSenderStatus), это и есть soft-delete здесь: "Архивировать"
// останавливает будущую отправку SMS от этого sender'а, поэтому он идёт
// через подтверждающий диалог, как destructive-действие. "Активировать"
// обратно — не деструктивно, без диалога (симметрично с тем, как
// ConfigView подтверждает только архивирование, не создание).
import { h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NForm,
  NFormItem,
  NInput,
  NSelect,
  NButton,
  NDataTable,
  NSpace,
  NAlert,
  useMessage,
  useDialog,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema-partner";

type Sender = components["schemas"]["Sender"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

const typeOptions = [
  { label: "ALPHANAME", value: "ALPHANAME" },
  { label: "SHORT_NUMBER", value: "SHORT_NUMBER" },
];

const listQuery = useQuery({
  queryKey: ["senders"],
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/senders", {});
    if (error) throw error;
    return data ?? [];
  },
});

// --- Создание (status всегда active на создании — форма его не задаёт) ---
const senderId = ref("");
const senderType = ref<string>("ALPHANAME");

const createMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.partner.POST("/senders", {
      body: { sender_id: senderId.value.trim(), type: senderType.value as "ALPHANAME" | "SHORT_NUMBER" },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Sender создан");
    senderId.value = "";
    senderType.value = "ALPHANAME";
    queryClient.invalidateQueries({ queryKey: ["senders"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

// --- Смена статуса (единственный способ изменить sender) ---
const statusMutation = useMutation({
  mutationFn: async (vars: { sender_id: string; status: "active" | "archived" }) => {
    const { data, error } = await api.partner.PATCH("/senders/{sender_id}", {
      params: { path: { sender_id: vars.sender_id } },
      body: { status: vars.status },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (_data, vars) => {
    message.success(vars.status === "archived" ? "Sender архивирован" : "Sender активирован");
    queryClient.invalidateQueries({ queryKey: ["senders"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(row: Sender) {
  dialog.warning({
    title: "Подтвердите архивирование",
    content: `Sender "${row.sender_id}" будет архивирован — отправка SMS от этого sender'а остановится. Продолжить?`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => statusMutation.mutate({ sender_id: row.sender_id, status: "archived" }),
  });
}

function reactivate(row: Sender) {
  statusMutation.mutate({ sender_id: row.sender_id, status: "active" });
}

const columns: DataTableColumns<Sender> = [
  { title: "sender_id", key: "sender_id" },
  { title: "type", key: "type" },
  { title: "status", key: "status" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      row.status === "active"
        ? h(
            NButton,
            { size: "small", type: "warning", disabled: !auth.isAdmin(), onClick: () => confirmArchive(row) },
            () => "Архивировать",
          )
        : h(
            NButton,
            { size: "small", type: "success", disabled: !auth.isAdmin(), onClick: () => reactivate(row) },
            () => "Активировать",
          ),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Senders — создать">
      <NForm inline label-placement="top">
        <NFormItem label="sender_id">
          <NInput v-model:value="senderId" style="width: 220px" />
        </NFormItem>
        <NFormItem label="type">
          <NSelect v-model:value="senderType" :options="typeOptions" style="width: 180px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton
            type="primary"
            :disabled="!auth.isAdmin() || !senderId.trim()"
            :loading="createMutation.isPending.value"
            @click="createMutation.mutate()"
          >
            Создать
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-top: 12px">
        Создание/архивирование senders требует роль partner-admin. Просмотр списка ниже доступен.
      </NAlert>
    </NCard>

    <NCard title="Senders">
      <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(listQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: Sender) => row.sender_id"
      />
    </NCard>
  </NSpace>
</template>
