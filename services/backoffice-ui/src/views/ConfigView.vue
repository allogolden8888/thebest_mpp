<script setup lang="ts">
// handle_config_crud (service_internal_methods.md §7.3) — CreateVersion/
// GetActiveVersion/ListVersions/ArchiveVersion, gRPC-проксирование в
// Configuration Service на стороне Backoffice API.
//
// CODE_REVIEW.md findings fixed in this view (subagent-1 / backoffice-ui):
// #1 — Create/Archive actions disabled for non-admins (RequireAdmin gates
//      the mutating form; listing versions stays read-only-visible to any
//      authenticated user, matching backend router.go — GET routes have no
//      role requirement).
// #2 — confirmation dialog before "Архивировать" fires.
// #5 — errors rendered via extractErrorMessage(), not `String(errObject)`.
import { h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, NAlert, useMessage, useDialog, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema";

type ConfigVersion = components["schemas"]["ConfigVersion"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

const entityType = ref("CONFIG_ENTITY_TYPE_PIPELINE");
const entityId = ref("");
const payloadJson = ref("{}");

const listQuery = useQuery({
  queryKey: ["config-versions", entityType, entityId],
  queryFn: async () => {
    if (!entityId.value) return { versions: [], next_page_token: "" };
    const { data, error } = await api.GET("/config/versions", {
      params: { query: { entity_type: entityType.value, entity_id: entityId.value } },
    });
    if (error) throw error;
    return data;
  },
});

const createMutation = useMutation({
  mutationFn: async () => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payloadJson.value);
    } catch {
      throw new Error("payload_json должен быть валидным JSON");
    }
    const { data, error } = await api.POST("/config/versions", {
      body: { entity_type: entityType.value, entity_id: entityId.value, payload_json: parsed },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Версия создана");
    queryClient.invalidateQueries({ queryKey: ["config-versions"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const archiveMutation = useMutation({
  mutationFn: async (version: number) => {
    const { data, error } = await api.POST("/config/versions/archive", {
      body: { entity_type: entityType.value, entity_id: entityId.value, version },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Версия архивирована");
    queryClient.invalidateQueries({ queryKey: ["config-versions"] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmArchive(version: number) {
  dialog.warning({
    title: "Подтвердите архивирование",
    content: `Версия ${version} (${entityType.value}/${entityId.value}) будет архивирована.`,
    positiveText: "Архивировать",
    negativeText: "Отмена",
    onPositiveClick: () => archiveMutation.mutate(version),
  });
}

const columns: DataTableColumns<ConfigVersion> = [
  { title: "Version", key: "version" },
  { title: "Status", key: "status" },
  { title: "Created", key: "created_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(
        NButton,
        { size: "small", disabled: !auth.isAdmin(), onClick: () => confirmArchive(row.version) },
        () => "Архивировать",
      ),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Configuration — создать версию">
      <NForm inline label-placement="top">
        <NFormItem label="entity_type">
          <NInput v-model:value="entityType" style="width: 280px" />
        </NFormItem>
        <NFormItem label="entity_id">
          <NInput v-model:value="entityId" style="width: 200px" />
        </NFormItem>
        <NFormItem label="payload_json">
          <NInput v-model:value="payloadJson" type="textarea" style="width: 320px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton
            type="primary"
            :disabled="!auth.isAdmin()"
            :loading="createMutation.isPending.value"
            @click="createMutation.mutate()"
          >
            Создать версию
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-top: 12px">
        Создание/архивирование версий конфигурации требует роль backoffice-admin. Просмотр версий ниже доступен.
      </NAlert>
    </NCard>

    <NCard title="Версии">
      <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(listQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value?.versions ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: ConfigVersion) => row.version"
      />
    </NCard>
  </NSpace>
</template>