<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 36 "Pattern Placeholders" (бывший "Regex
// Patterns" в дизайн-референсе). Тот же тонкий generic config-version
// паттерн, что CategoriesView.vue/GuidesView.vue
// (entity_type=CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER) — ни одного нового
// REST-эндпоинта под эту сущность не заведено, схема payload'а уже
// зафиксирована в config_schemas/pattern_placeholder.schema.json
// (name/regex/description?). entity_id = name.
//
// ВАЖНО (граница объёма, зафиксирована и в BACKOFFICE_DESIGN_SPEC.md, и в
// services/policy-service/src/template_matching.rs): этот экран — ТОЛЬКО
// CRUD над реестром name->regex. Реальное использование внутри синтаксиса
// policy_template (`%{name}` в pattern, резолвинг через живой реестр,
// извлечение значения при матчинге) — отдельное изменение движка
// policy-service, сделанное этим же заходом (template_matching.rs,
// config_reload.rs), но не частью ЭТОГО файла — здесь только управление
// самими записями реестра, ничего не проверяет regex против реальных
// сообщений.
//
// Список — через ListVersions(entity_id="") browse-режим, тот же, что уже
// использует CategoriesView.vue/GuidesView.vue.
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

const ENTITY_TYPE = "CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER";

interface PlaceholderPayload {
  name: string;
  regex: string;
  description?: string;
}

interface PlaceholderRow {
  entity_id: string;
  version: number;
  payload: PlaceholderPayload;
}

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();

const placeholdersQuery = useQuery({
  queryKey: ["pattern-placeholders"],
  queryFn: async () => {
    const { data, error } = await api.GET("/config/versions", {
      params: { query: { entity_type: ENTITY_TYPE } },
    });
    if (error) throw error;
    return data;
  },
});

const rows = computed<PlaceholderRow[]>(() => {
  const versions = placeholdersQuery.data.value?.versions ?? [];
  return versions.map((v) => ({
    entity_id: v.entity_id,
    version: v.version,
    payload: (v.payload_json ?? { name: v.entity_id, regex: "" }) as PlaceholderPayload,
  }));
});

// name — сам entity_id (см. config_schemas/pattern_placeholder.schema.json:
// "^[a-z][a-z0-9_]*$", ровно тот же charset, что %{name} в template_matching.rs
// ожидает внутри pattern) — в отличие от Guides, здесь нет отдельного
// человекочитаемого title, из которого можно вывести slug: name — это и есть
// то, что admin напишет внутри %{...}, поэтому вводится напрямую, без
// автогенерации.
const form = ref<PlaceholderPayload>({ name: "", regex: "", description: "" });
const editingExistingName = ref(false);

const NAME_PATTERN = /^[a-z][a-z0-9_]*$/;
const nameInvalid = computed(() => form.value.name.length > 0 && !NAME_PATTERN.test(form.value.name));
const formInvalid = computed(
  () => !form.value.name.trim() || !form.value.regex.trim() || nameInvalid.value,
);

const saveMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/config/versions", {
      body: {
        entity_type: ENTITY_TYPE,
        entity_id: form.value.name.trim(),
        payload_json: {
          name: form.value.name.trim(),
          regex: form.value.regex.trim(),
          ...(form.value.description?.trim() ? { description: form.value.description.trim() } : {}),
        },
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Pattern placeholder сохранён");
    resetForm();
    queryClient.invalidateQueries({ queryKey: ["pattern-placeholders"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function resetForm() {
  form.value = { name: "", regex: "", description: "" };
  editingExistingName.value = false;
}

function editRow(row: PlaceholderRow) {
  form.value = { ...row.payload };
  editingExistingName.value = true;
}

const archiveMutation = useMutation({
  mutationFn: async (row: PlaceholderRow) => {
    const { data, error } = await api.POST("/config/versions/archive", {
      body: { entity_type: ENTITY_TYPE, entity_id: row.entity_id, version: row.version },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Pattern placeholder архивирован");
    queryClient.invalidateQueries({ queryKey: ["pattern-placeholders"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(row: PlaceholderRow) {
  dialog.warning({
    title: "Подтвердите архивацию",
    content: `Placeholder "%{${row.entity_id}}" (версия ${row.version}) будет архивирован — любой policy_template, ссылающийся на него, перестанет матчиться (fail closed, см. template_matching.rs).`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => archiveMutation.mutate(row),
  });
}

const columns: DataTableColumns<PlaceholderRow> = [
  { title: "name", key: "entity_id", render: (row) => `%{${row.entity_id}}` },
  { title: "regex", key: "regex", render: (row) => row.payload.regex || "—" },
  { title: "description", key: "description", render: (row) => row.payload.description || "—" },
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
    <NCard title="Pattern Placeholders — создать/обновить">
      <NForm label-placement="top">
        <NSpace>
          <NFormItem label="name (entity_id)" :feedback="nameInvalid ? 'только a-z, 0-9, _, начинается с буквы' : undefined" :validation-status="nameInvalid ? 'error' : undefined">
            <NInput
              v-model:value="form.name"
              style="width: 220px"
              placeholder="otp_code"
              :disabled="editingExistingName"
            />
          </NFormItem>
          <NFormItem label="regex">
            <NInput v-model:value="form.regex" style="width: 320px" placeholder="\d{6}" />
          </NFormItem>
          <NFormItem label="description">
            <NInput v-model:value="form.description" style="width: 320px" placeholder="одноразовый код подтверждения (6 цифр)" />
          </NFormItem>
        </NSpace>
        <NFormItem label=" ">
          <NButton type="primary" :disabled="formInvalid" :loading="saveMutation.isPending.value" @click="saveMutation.mutate()">
            Сохранить
          </NButton>
          <NButton v-if="editingExistingName" style="margin-left: 8px" @click="resetForm()">Отмена</NButton>
        </NFormItem>
      </NForm>
      <NAlert type="info" style="margin-top: 8px">
        Используйте <code>%{{ '{' }}name{{ '}' }}</code> внутри <code>policy_template.pattern</code>, чтобы сослаться
        на именованный плейсхолдер — движок матчинга (policy-service) резолвит имя через этот реестр и извлекает
        подошедшее значение. Неизвестное или архивированное имя приводит к тому, что шаблон никогда не матчится
        (fail closed), а не к ошибке. "Редактировать" создаёт новую версию с тем же name — история версий сохраняется.
      </NAlert>
    </NCard>

    <NCard title="Pattern Placeholders — список">
      <NAlert v-if="placeholdersQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(placeholdersQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="rows"
        :loading="placeholdersQuery.isLoading.value"
        :row-key="(row: PlaceholderRow) => row.entity_id"
      />
    </NCard>
  </NSpace>
</template>
