<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 32 "Categories". Тонкая структурированная
// форма поверх уже существующих generic config-version эндпоинтов
// (entity_type=CONFIG_ENTITY_TYPE_CATEGORY), тот же путь, что уже работает
// для policy_ruleset/route_table/billing_tariff/subscriber_consent — ни
// одного нового REST-эндпоинта под эту сущность не заведено.
//
// entity_id = name (человекочитаемый, тот же паттерн, что entity_id=
// partner_id у billing_tariff/route_table). "Редактирование" — создание
// новой версии с тем же entity_id (та же неизменяемая версионируемая
// модель, что ConfigView.vue уже использует для всего остального), не
// PATCH на существующей строке.
//
// Список — через ListVersions(entity_id="") "browse-режим", добавленный
// вместе с этим экраном (configuration-service/internal/grpcserver/
// server.go): раньше ListVersions требовал уже известный entity_id
// (история версий ОДНОЙ сущности), для списка "все категории" нужен был
// противоположный срез.
//
// Явная граница объёма (см. luminous-hugging-charm.md): policy-service/
// billing-service продолжают принимать любую строку как category без
// сверки с этим реестром — это слой управления/метаданных, не новая
// валидация в live-пайплайне.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NForm,
  NFormItem,
  NInput,
  NSwitch,
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

const ENTITY_TYPE = "CONFIG_ENTITY_TYPE_CATEGORY";

interface CategoryPayload {
  name: string;
  regex: string;
  count_in_cdr: boolean;
}

interface CategoryRow {
  entity_id: string;
  version: number;
  payload: CategoryPayload;
}

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();

const categoriesQuery = useQuery({
  queryKey: ["categories"],
  queryFn: async () => {
    const { data, error } = await api.GET("/config/versions", {
      params: { query: { entity_type: ENTITY_TYPE } },
    });
    if (error) throw error;
    return data;
  },
});

const rows = computed<CategoryRow[]>(() => {
  const versions = categoriesQuery.data.value?.versions ?? [];
  return versions.map((v) => ({
    entity_id: v.entity_id,
    version: v.version,
    payload: (v.payload_json ?? { name: v.entity_id, regex: "", count_in_cdr: false }) as CategoryPayload,
  }));
});

const form = ref<CategoryPayload>({ name: "", regex: "", count_in_cdr: false });
const formInvalid = computed(() => !form.value.name.trim());

const saveMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/config/versions", {
      body: {
        entity_type: ENTITY_TYPE,
        entity_id: form.value.name.trim(),
        payload_json: { name: form.value.name.trim(), regex: form.value.regex, count_in_cdr: form.value.count_in_cdr },
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Категория сохранена");
    form.value = { name: "", regex: "", count_in_cdr: false };
    queryClient.invalidateQueries({ queryKey: ["categories"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function editRow(row: CategoryRow) {
  form.value = { ...row.payload };
}

const archiveMutation = useMutation({
  mutationFn: async (row: CategoryRow) => {
    const { data, error } = await api.POST("/config/versions/archive", {
      body: { entity_type: ENTITY_TYPE, entity_id: row.entity_id, version: row.version },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Категория архивирована");
    queryClient.invalidateQueries({ queryKey: ["categories"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(row: CategoryRow) {
  dialog.warning({
    title: "Подтвердите архивацию",
    content: `Категория "${row.entity_id}" (версия ${row.version}) будет архивирована.`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => archiveMutation.mutate(row),
  });
}

const columns: DataTableColumns<CategoryRow> = [
  { title: "name", key: "entity_id" },
  { title: "regex", key: "regex", render: (row) => row.payload.regex || "—" },
  { title: "учитывать в CDR", key: "count_in_cdr", render: (row) => (row.payload.count_in_cdr ? "да" : "нет") },
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
    <NCard title="Categories — создать/обновить">
      <NForm inline label-placement="top">
        <NFormItem label="name">
          <NInput v-model:value="form.name" style="width: 200px" placeholder="SERVICE" />
        </NFormItem>
        <NFormItem label="regex">
          <NInput v-model:value="form.regex" style="width: 280px" placeholder="опционально" />
        </NFormItem>
        <NFormItem label="учитывать в CDR">
          <NSwitch v-model:value="form.count_in_cdr" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton type="primary" :disabled="formInvalid" :loading="saveMutation.isPending.value" @click="saveMutation.mutate()">
            Сохранить
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert type="info" style="margin-top: 8px">
        "Редактировать" создаёт новую версию с тем же именем — история версий сохраняется, старая архивируется отдельно.
      </NAlert>
    </NCard>

    <NCard title="Categories — список">
      <NAlert v-if="categoriesQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(categoriesQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="rows"
        :loading="categoriesQuery.isLoading.value"
        :row-key="(row: CategoryRow) => row.entity_id"
      />
    </NCard>
  </NSpace>
</template>
