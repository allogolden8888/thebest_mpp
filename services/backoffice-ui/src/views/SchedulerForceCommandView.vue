<script setup lang="ts">
// handle_force_scheduler_command (service_internal_methods.md §7.3) — Kafka
// publish на scheduler.critical.commands. task_type ограничен
// FORCE_TIMEOUT/FORCE_RETRY (та же защита от открытого редиректа, что на
// стороне backoffice-api) — выпадающий список, не свободный ввод.
import { ref } from "vue";
import { useMutation } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NSelect, NButton, useMessage } from "naive-ui";
import { useApi } from "../api/useApi";

const api = useApi();
const message = useMessage();

const stageExecutionId = ref("");
const taskType = ref<"CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT" | "CRITICAL_COMMAND_TYPE_FORCE_RETRY">(
  "CRITICAL_COMMAND_TYPE_FORCE_RETRY",
);
const reason = ref("");

const taskTypeOptions = [
  { label: "FORCE_RETRY", value: "CRITICAL_COMMAND_TYPE_FORCE_RETRY" },
  { label: "FORCE_TIMEOUT", value: "CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT" },
];

const mutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/scheduler/force-command", {
      body: { stage_execution_id: stageExecutionId.value, task_type: taskType.value, reason: reason.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => message.success("Команда отправлена в scheduler.critical.commands"),
  onError: (err: unknown) => message.error(String(err)),
});
</script>

<template>
  <NCard title="Force Scheduler Command">
    <NForm label-placement="top" style="max-width: 480px">
      <NFormItem label="stage_execution_id">
        <NInput v-model:value="stageExecutionId" />
      </NFormItem>
      <NFormItem label="task_type">
        <NSelect v-model:value="taskType" :options="taskTypeOptions" />
      </NFormItem>
      <NFormItem label="reason">
        <NInput v-model:value="reason" type="textarea" />
      </NFormItem>
      <NButton type="primary" :loading="mutation.isPending.value" @click="mutation.mutate()">
        Отправить
      </NButton>
    </NForm>
  </NCard>
</template>