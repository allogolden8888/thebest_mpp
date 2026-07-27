<script setup lang="ts">
// handle_config_crud (service_internal_methods.md §7.3) — CreateVersion/
// GetActiveVersion/ListVersions/ArchiveVersion, gRPC-проксирование в
// Configuration Service на стороне Backoffice API.
import { h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, useMessage, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import type { components } from "../api/schema";

type ConfigVersion = components["schemas"]["ConfigVersion"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();

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
  onError: (err: unknown) => message.error(String(err)),
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
  onError: (err: unknown) => message.error(String(err)),
});

const columns: DataTableColumns<ConfigVersion> = [
  { title: "Version", key: "version" },
  { title: "Status", key: "status" },
  { title: "Created", key: "created_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(NButton, { size: "small", onClick: () => archiveMutation.mutate(row.version) }, () => "Архивировать"),
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
          <NButton type="primary" :loading="createMutation.isPending.value" @click="createMutation.mutate()">
            Создать версию
          </NButton>
        </NFormItem>
      </NForm>
    </NCard>

    <NCard title="Версии">
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value?.versions ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: ConfigVersion) => row.version"
      />
    </NCard>
  </NSpace>
</template>