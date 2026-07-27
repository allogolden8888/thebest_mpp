<script setup lang="ts">
// handle_dlq_browse + handle_replay_request (service_internal_methods.md
// §7.3) — одна страница: браузер DLQ с кнопкой Replay на каждую строку
// (естественный UX-поток HLD §20 — сначала посмотреть, что в очереди, потом
// решить, что реплеить).
import { h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NDataTable, NSpace, useMessage, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import type { components } from "../api/schema";

type DlqRecord = components["schemas"]["DlqRecord"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();

const stageName = ref("");
const replayStatus = ref("");

const listQuery = useQuery({
  queryKey: ["dlq-records", stageName, replayStatus],
  queryFn: async () => {
    const { data, error } = await api.GET("/dlq", {
      params: { query: { stage_name: stageName.value || undefined, replay_status: replayStatus.value || undefined } },
    });
    if (error) throw error;
    return data;
  },
});

const replayMutation = useMutation({
  mutationFn: async (stageExecutionId: string) => {
    const { data, error } = await api.POST("/replay", { body: { stage_execution_id: stageExecutionId } });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    if (data?.accepted) {
      message.success("Replay принят");
    } else {
      message.warning(`Replay отклонён: ${data?.rejection_reason}`);
    }
    queryClient.invalidateQueries({ queryKey: ["dlq-records"] });
  },
  onError: (err: unknown) => message.error(String(err)),
});

const columns: DataTableColumns<DlqRecord> = [
  { title: "stage_execution_id", key: "stage_execution_id" },
  { title: "stage_name", key: "stage_name" },
  { title: "attempt", key: "attempt" },
  { title: "reason_code", key: "reason_code" },
  { title: "replay_status", key: "replay_status" },
  { title: "created_at", key: "created_at" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(
        NButton,
        {
          size: "small",
          disabled: row.replay_status !== "pending",
          onClick: () => replayMutation.mutate(row.stage_execution_id),
        },
        () => "Replay",
      ),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="DLQ Browse">
      <NForm inline label-placement="top">
        <NFormItem label="stage_name">
          <NInput v-model:value="stageName" style="width: 220px" />
        </NFormItem>
        <NFormItem label="replay_status">
          <NInput v-model:value="replayStatus" placeholder="pending / replayed / expired" style="width: 220px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton @click="listQuery.refetch()">Обновить</NButton>
        </NFormItem>
      </NForm>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value?.records ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: DlqRecord) => row.stage_execution_id"
      />
    </NCard>
  </NSpace>
</template>