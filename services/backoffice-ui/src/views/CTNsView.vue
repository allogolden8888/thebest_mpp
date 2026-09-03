<script setup lang="ts">
// BACKOFFICE_DESIGN_SPEC.md Экран 23 "CTNs". Тот же generic config-version
// путь, что CategoriesView.vue (entity_type=CONFIG_ENTITY_TYPE_CTN) — CTN
// (termination number) привязывает (partner_id, category) -> ctn/
// service_name для офлайн телеком-биллинга. entity_id — составной ключ
// "{partner_id}:{category}" (тот же композитный-entity_id паттерн, что уже
// принят для subscriber_consent), вычисляется автоматически из формы, не
// вводится вручную.
//
// Сам CDR-экспорт (генерация файла для сверки с оператором) спланирован в
// BACKOFFICE_ROADMAP.md/BACKOFFICE_DESIGN_SPEC.md, не реализован здесь —
// этот экран только управляет справочником привязок.
import { computed, h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, NAlert, useMessage, useDialog, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";

const ENTITY_TYPE = "CONFIG_ENTITY_TYPE_CTN";

interface CTNPayload {
  ctn: string;
  partner_id: string;
  category: string;
  service_name: string;
}

interface CTNRow {
  entity_id: string;
  version: number;
  payload: CTNPayload;
}

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();

const ctnsQuery = useQuery({
  queryKey: ["ctns"],
  queryFn: async () => {
    const { data, error } = await api.GET("/config/versions", {
      params: { query: { entity_type: ENTITY_TYPE } },
    });
    if (error) throw error;
    return data;
  },
});

const rows = computed<CTNRow[]>(() => {
  const versions = ctnsQuery.data.value?.versions ?? [];
  return versions.map((v) => ({
    entity_id: v.entity_id,
    version: v.version,
    payload: v.payload_json as CTNPayload,
  }));
});

const form = ref<CTNPayload>({ ctn: "", partner_id: "", category: "", service_name: "" });
const formInvalid = computed(
  () => !form.value.ctn.trim() || !form.value.partner_id.trim() || !form.value.category.trim() || !form.value.service_name.trim(),
);

const saveMutation = useMutation({
  mutationFn: async () => {
    const entityId = `${form.value.partner_id.trim()}:${form.value.category.trim()}`;
    const { data, error } = await api.POST("/config/versions", {
      body: {
        entity_type: ENTITY_TYPE,
        entity_id: entityId,
        payload_json: {
          ctn: form.value.ctn.trim(),
          partner_id: form.value.partner_id.trim(),
          category: form.value.category.trim(),
          service_name: form.value.service_name.trim(),
        },
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("CTN сохранён");
    form.value = { ctn: "", partner_id: "", category: "", service_name: "" };
    queryClient.invalidateQueries({ queryKey: ["ctns"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function editRow(row: CTNRow) {
  form.value = { ...row.payload };
}

const archiveMutation = useMutation({
  mutationFn: async (row: CTNRow) => {
    const { data, error } = await api.POST("/config/versions/archive", {
      body: { entity_type: ENTITY_TYPE, entity_id: row.entity_id, version: row.version },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("CTN архивирован");
    queryClient.invalidateQueries({ queryKey: ["ctns"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(row: CTNRow) {
  dialog.warning({
    title: "Подтвердите архивацию",
    content: `CTN "${row.payload.ctn}" (${row.entity_id}, версия ${row.version}) будет архивирован.`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => archiveMutation.mutate(row),
  });
}

const columns: DataTableColumns<CTNRow> = [
  { title: "partner_id", key: "partner_id", render: (row) => row.payload.partner_id },
  { title: "category", key: "category", render: (row) => row.payload.category },
  { title: "ctn", key: "ctn", render: (row) => row.payload.ctn },
  { title: "service_name", key: "service_name", render: (row) => row.payload.service_name },
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
    <NCard title="CTN — создать/обновить">
      <NForm inline label-placement="top">
        <NFormItem label="partner_id">
          <NInput v-model:value="form.partner_id" style="width: 180px" />
        </NFormItem>
        <NFormItem label="category">
          <NInput v-model:value="form.category" style="width: 160px" placeholder="SERVICE" />
        </NFormItem>
        <NFormItem label="ctn">
          <NInput v-model:value="form.ctn" style="width: 180px" placeholder="998881112233" />
        </NFormItem>
        <NFormItem label="service_name">
          <NInput v-model:value="form.service_name" style="width: 240px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton type="primary" :disabled="formInvalid" :loading="saveMutation.isPending.value" @click="saveMutation.mutate()">
            Сохранить
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert type="info" style="margin-top: 8px">
        Привязка (partner_id, category) -&gt; ctn/service_name для офлайн-биллинга. Сам CDR-экспорт спланирован, не реализован.
      </NAlert>
    </NCard>

    <NCard title="CTN — список">
      <NAlert v-if="ctnsQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(ctnsQuery.error.value) }}
      </NAlert>
      <NDataTable :columns="columns" :data="rows" :loading="ctnsQuery.isLoading.value" :row-key="(row: CTNRow) => row.entity_id" />
    </NCard>
  </NSpace>
</template>
