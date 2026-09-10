<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 41 "Guides / Guide management". Точно
// такая же тонкая обёртка вокруг generic config-version эндпоинтов, что
// CategoriesView.vue/CTNsView.vue (entity_type=CONFIG_ENTITY_TYPE_GUIDE) —
// ни одного нового REST-эндпоинта под эту сущность не заведено, схема
// payload'а уже зафиксирована в config_schemas/guide.schema.json
// (title/body_markdown/category).
//
// entity_id = slug: URL-safe идентификатор, который админ либо вводит сам,
// либо мы выводим из title (нижний регистр, пробелы/спецсимволы → "-").
// "Редактирование" — создание новой версии с тем же slug (та же неизменяемая
// версионируемая модель, что и везде в ConfigView.vue), не PATCH.
//
// Список — через ListVersions(entity_id="") browse-режим, тот же, что уже
// использует CategoriesView.vue.
//
// Явная граница объёма: этот экран — ТОЛЬКО админская сторона (создание/
// редактирование/архивация гайдов). Показ опубликованных гайдов партнёрам
// где-либо в services/partner-portal-ui — отдельный, пока НЕ решённый вопрос
// (см. BACKOFFICE_DESIGN_SPEC.md, этот же раздел) — специально не строим и
// не считаем закрытым.
//
// Живого markdown-рендерера в зависимостях backoffice-ui нет (проверено
// package.json перед реализацией) — по договорённости не тянем новую npm
// зависимость ради необязательного превью, поэтому body_markdown — обычный
// многострочный textarea без live-preview.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NForm,
  NFormItem,
  NInput,
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

const ENTITY_TYPE = "CONFIG_ENTITY_TYPE_GUIDE";

interface GuidePayload {
  title: string;
  body_markdown: string;
  category: string;
}

interface GuideRow {
  entity_id: string;
  version: number;
  payload: GuidePayload;
}

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();

const guidesQuery = useQuery({
  queryKey: ["guides"],
  queryFn: async () => {
    const { data, error } = await api.GET("/config/versions", {
      params: { query: { entity_type: ENTITY_TYPE } },
    });
    if (error) throw error;
    return data;
  },
});

const rows = computed<GuideRow[]>(() => {
  const versions = guidesQuery.data.value?.versions ?? [];
  return versions.map((v) => ({
    entity_id: v.entity_id,
    version: v.version,
    payload: (v.payload_json ?? { title: v.entity_id, body_markdown: "", category: "" }) as GuidePayload,
  }));
});

function slugify(title: string): string {
  return title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

const slug = ref("");
const slugTouched = ref(false);
const form = ref<GuidePayload>({ title: "", body_markdown: "", category: "" });

function onTitleInput(value: string) {
  form.value.title = value;
  if (!slugTouched.value) slug.value = slugify(value);
}

function onSlugInput(value: string) {
  slug.value = value;
  slugTouched.value = true;
}

const formInvalid = computed(
  () => !slug.value.trim() || !form.value.title.trim() || !form.value.category.trim(),
);

const saveMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/config/versions", {
      body: {
        entity_type: ENTITY_TYPE,
        entity_id: slug.value.trim(),
        payload_json: {
          title: form.value.title.trim(),
          body_markdown: form.value.body_markdown,
          category: form.value.category.trim(),
        },
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Гайд сохранён");
    resetForm();
    queryClient.invalidateQueries({ queryKey: ["guides"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function resetForm() {
  form.value = { title: "", body_markdown: "", category: "" };
  slug.value = "";
  slugTouched.value = false;
}

function editRow(row: GuideRow) {
  form.value = { ...row.payload };
  slug.value = row.entity_id;
  slugTouched.value = true;
}

const archiveMutation = useMutation({
  mutationFn: async (row: GuideRow) => {
    const { data, error } = await api.POST("/config/versions/archive", {
      body: { entity_type: ENTITY_TYPE, entity_id: row.entity_id, version: row.version },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Гайд снят с публикации");
    queryClient.invalidateQueries({ queryKey: ["guides"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(row: GuideRow) {
  dialog.warning({
    title: "Подтвердите архивацию",
    content: `Гайд "${row.payload.title || row.entity_id}" (версия ${row.version}) будет снят с публикации.`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => archiveMutation.mutate(row),
  });
}

const columns: DataTableColumns<GuideRow> = [
  { title: "slug", key: "entity_id" },
  { title: "title", key: "title", render: (row) => row.payload.title || "—" },
  { title: "category", key: "category", render: (row) => row.payload.category || "—" },
  { title: "версия", key: "version" },
  {
    title: "Действие",
    key: "actions",
    render: (row) => [
      h(NButton, { size: "small", style: "margin-right: 8px", onClick: () => editRow(row) }, () => "Редактировать"),
      h(NButton, { size: "small", type: "error", loading: archiveMutation.isPending.value, onClick: () => confirmArchive(row) }, () => "Архивировать"),
    ],
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Guides — создать/обновить">
      <NForm label-placement="top">
        <NSpace>
          <NFormItem label="title">
            <NInput
              :value="form.title"
              style="width: 280px"
              placeholder="Как отправить SMS через API"
              @update:value="onTitleInput"
            />
          </NFormItem>
          <NFormItem label="slug (entity_id)">
            <NInput
              :value="slug"
              style="width: 220px"
              placeholder="выводится из title"
              @update:value="onSlugInput"
            />
          </NFormItem>
          <NFormItem label="category">
            <NInput v-model:value="form.category" style="width: 200px" placeholder="getting-started" />
          </NFormItem>
        </NSpace>
        <NFormItem label="body_markdown">
          <NInput
            v-model:value="form.body_markdown"
            type="textarea"
            :autosize="{ minRows: 8, maxRows: 24 }"
            placeholder="# Заголовок&#10;&#10;Текст гайда в markdown..."
          />
        </NFormItem>
        <NFormItem label=" ">
          <NButton type="primary" :disabled="formInvalid" :loading="saveMutation.isPending.value" @click="saveMutation.mutate()">
            Сохранить
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert type="info" style="margin-top: 8px">
        "Редактировать" создаёт новую версию с тем же slug — история версий сохраняется, старая архивируется отдельно.
        Live-превью markdown не реализовано (в зависимостях проекта нет markdown-рендерера, тянуть новую библиотеку
        ради этого не стали) — редактирование в чистом тексте.
      </NAlert>
    </NCard>

    <NCard title="Guides — список">
      <NAlert v-if="guidesQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(guidesQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="rows"
        :loading="guidesQuery.isLoading.value"
        :row-key="(row: GuideRow) => row.entity_id"
      />
    </NCard>
  </NSpace>
</template>
