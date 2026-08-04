<script setup lang="ts">
// handle_force_scheduler_command (service_internal_methods.md §7.3) — Kafka
// publish на scheduler.critical.commands. task_type ограничен
// FORCE_TIMEOUT/FORCE_RETRY (та же защита от открытого редиректа, что на
// стороне backoffice-api) — выпадающий список, не свободный ввод.
//
// CODE_REVIEW.md findings fixed in this view (subagent-1 / backoffice-ui):
// #1 — RequireAdmin.vue gate (defense in depth, menu already hides this
//      entry for non-admins — see App.vue).
// #2 — confirmation dialog before the command fires, plus client-side
//      non-empty `reason` check matching backend validation
//      (backoffice-api/internal/httpapi/scheduler.go).
// #5 — errors rendered via extractErrorMessage(), not `String(errObject)`.
import { computed, ref } from "vue";
import { useMutation } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NSelect, NButton, useMessage, useDialog } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import RequireAdmin from "../components/RequireAdmin.vue";

const api = useApi();
const message = useMessage();
const dialog = useDialog();

const stageExecutionId = ref("");
const taskType = ref<"CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT" | "CRITICAL_COMMAND_TYPE_FORCE_RETRY">(
  "CRITICAL_COMMAND_TYPE_FORCE_RETRY",
);
const reason = ref("");

const taskTypeOptions = [
  { label: "FORCE_RETRY", value: "CRITICAL_COMMAND_TYPE_FORCE_RETRY" },
  { label: "FORCE_TIMEOUT", value: "CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT" },
];

const reasonMissing = computed(() => reason.value.trim() === "");

const mutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/scheduler/force-command", {
      body: { stage_execution_id: stageExecutionId.value, task_type: taskType.value, reason: reason.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: () => message.success("Команда отправлена в scheduler.critical.commands"),
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmSend() {
  if (reasonMissing.value) {
    message.warning("reason обязателен для force scheduler command");
    return;
  }
  if (!stageExecutionId.value.trim()) {
    message.warning("stage_execution_id обязателен");
    return;
  }
  dialog.warning({
    title: "Подтвердите Force Scheduler Command",
    content: `${taskType.value} для stage_execution_id=${stageExecutionId.value}`,
    positiveText: "Отправить",
    negativeText: "Отмена",
    onPositiveClick: () => mutation.mutate(),
  });
}
</script>

<template>
  <RequireAdmin>
    <NCard title="Force Scheduler Command">
      <NForm label-placement="top" style="max-width: 480px">
        <NFormItem label="stage_execution_id">
          <NInput v-model:value="stageExecutionId" />
        </NFormItem>
        <NFormItem label="task_type">
          <NSelect v-model:value="taskType" :options="taskTypeOptions" />
        </NFormItem>
        <NFormItem label="reason" :feedback="reasonMissing ? 'обязательное поле' : undefined" :validation-status="reasonMissing ? 'error' : undefined">
          <NInput v-model:value="reason" type="textarea" placeholder="обязательно — попадает в audit log" />
        </NFormItem>
        <NButton type="primary" :disabled="reasonMissing" :loading="mutation.isPending.value" @click="confirmSend">
          Отправить
        </NButton>
      </NForm>
    </NCard>
  </RequireAdmin>
</template>
