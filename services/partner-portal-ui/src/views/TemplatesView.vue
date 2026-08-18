<script setup lang="ts">
// GET /v1/self-service/templates (read-only, "Мои шаблоны") — прокси в
// template-management-service. См. док-комментарий на listTemplates в
// services/partner-self-service-api/internal/httpapi/templates.go: partner_id
// ВСЕГДА берётся из JWT на бэкенде и любой query-параметр partner_id
// игнорируется — поэтому в этой форме сознательно НЕТ поля partner_id (оно
// было бы обманчивым: подразумевало бы возможность смотреть чужие шаблоны,
// которой не существует). sender_id/category/status — легитимные фильтры,
// они лишь сужают уже отскоуленный на partner_id набор (см. описание
// GET /templates в openapi-partner.yaml).
//
// Пагинация: TemplatesListResponse отдаёт только { templates, limit, offset },
// без total-count, поэтому itemCount для NDataTable — оценка: если пришло
// меньше limit строк, это последняя страница (точный count = offset + len);
// иначе предполагаем, что дальше есть ещё хотя бы одна страница.
import { computed, h, ref, watch } from "vue";
import { useQuery } from "@tanstack/vue-query";
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
  NTag,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import type { components } from "../api/schema-partner";

type Template = components["schemas"]["Template"];

const api = useApi();

const senderId = ref("");
const category = ref("");
const status = ref("");
const statusOptions = [
  { label: "Все", value: "" },
  { label: "active", value: "active" },
  { label: "archived", value: "archived" },
];

const page = ref(1);
const pageSize = ref(20);

// Сброс на первую страницу при изменении любого фильтра — иначе можно
// оказаться на странице 5 пустого результата после сужения фильтра.
watch([senderId, category, status], () => {
  page.value = 1;
});

const listQuery = useQuery({
  queryKey: ["templates", senderId, category, status, page, pageSize],
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/templates", {
      params: {
        query: {
          sender_id: senderId.value.trim() || undefined,
          category: category.value.trim() || undefined,
          status: status.value || undefined,
          limit: pageSize.value,
          offset: (page.value - 1) * pageSize.value,
        },
      },
    });
    if (error) throw error;
    return data;
  },
});

const estimatedItemCount = computed(() => {
  const templates = listQuery.data.value?.templates ?? [];
  const offset = listQuery.data.value?.offset ?? (page.value - 1) * pageSize.value;
  const limit = listQuery.data.value?.limit ?? pageSize.value;
  // Меньше строк, чем запрошенный limit, — значит это последняя страница.
  return templates.length < limit ? offset + templates.length : offset + templates.length + 1;
});

const pagination = computed(() => ({
  page: page.value,
  pageSize: pageSize.value,
  itemCount: estimatedItemCount.value,
  pageSizes: [10, 20, 50],
  showSizePicker: true,
  onChange: (newPage: number) => {
    page.value = newPage;
  },
  onUpdatePageSize: (newPageSize: number) => {
    pageSize.value = newPageSize;
    page.value = 1;
  },
}));

const columns: DataTableColumns<Template> = [
  {
    title: "sender_id",
    key: "sender_id",
    render: (row) => row.sender_id ?? "все отправители",
  },
  { title: "category", key: "category" },
  {
    title: "pattern",
    key: "pattern",
    minWidth: 280,
  },
  {
    title: "status",
    key: "status",
    render: (row) =>
      h(
        NTag,
        { type: row.status === "active" ? "success" : "default", size: "small" },
        () => row.status,
      ),
  },
  { title: "version", key: "version" },
  { title: "operator_id", key: "operator_id", render: (row) => row.operator_id ?? "—" },
  { title: "channel", key: "channel" },
  { title: "created_at", key: "created_at" },
  { title: "updated_at", key: "updated_at" },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Мои шаблоны">
      <NForm inline label-placement="top">
        <NFormItem label="sender_id">
          <NInput v-model:value="senderId" placeholder="ID отправителя" style="width: 220px" />
        </NFormItem>
        <NFormItem label="category">
          <NInput v-model:value="category" placeholder="SERVICE / TRANSACTION / ADVERTISING / ..." style="width: 260px" />
        </NFormItem>
        <NFormItem label="status">
          <NSelect v-model:value="status" :options="statusOptions" style="width: 160px" />
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
        :data="listQuery.data.value?.templates ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: Template) => row.template_id"
        :pagination="pagination"
        :scroll-x="1400"
      />
    </NCard>
  </NSpace>
</template>
