<script setup lang="ts">
// handle_execution_control_override (service_internal_methods.md §7.3) —
// ApplyOverride/ClearOverride, gRPC-проксирование в Execution Control Service.
//
// CODE_REVIEW.md findings fixed in this view (subagent-1 / backoffice-ui):
// #1 — RequireAdmin.vue gate (defense in depth if reached directly by URL,
//      menu already hides this entry for non-admins — see App.vue).
// #2 — confirmation dialog (NDialogProvider `useDialog().warning`) before
//      Apply Override / Clear Override fire, plus client-side non-empty
//      `reason` check matching the backend's validation
//      (backoffice-api/internal/httpapi/executioncontrol.go) before the
//      request is even sent.
// #5 — errors rendered via extractErrorMessage(), not `String(errObject)`.
// #7 — this is the one form that can set PAUSED at GLOBAL scope; `scope`/
//      `state` are now NSelect enum dropdowns (values from
//      platform-contracts/common/enums.proto ExecutionControlScope/State),
//      matching the pattern SchedulerForceCommandView.vue already used, not
//      free-text inputs a typo could silently no-op or misroute.
import { computed, ref } from "vue";
import { useMutation } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NInputNumber, NSelect, NButton, NSpace, NAlert, useMessage, useDialog } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import RequireAdmin from "../components/RequireAdmin.vue";

const api = useApi();
const message = useMessage();
const dialog = useDialog();

const scopeOptions = [
  { label: "GLOBAL", value: "EXECUTION_CONTROL_SCOPE_GLOBAL" },
  { label: "STAGE", value: "EXECUTION_CONTROL_SCOPE_STAGE" },
  { label: "PARTNER", value: "EXECUTION_CONTROL_SCOPE_PARTNER" },
  { label: "PARTNER_STAGE", value: "EXECUTION_CONTROL_SCOPE_PARTNER_STAGE" },
  { label: "OPERATOR_ROUTE", value: "EXECUTION_CONTROL_SCOPE_OPERATOR_ROUTE" },
];

const stateOptions = [
  { label: "ACTIVE", value: "EXECUTION_CONTROL_STATE_ACTIVE" },
  { label: "DEGRADED", value: "EXECUTION_CONTROL_STATE_DEGRADED" },
  { label: "PAUSED", value: "EXECUTION_CONTROL_STATE_PAUSED" },
];

const scope = ref<string>("EXECUTION_CONTROL_SCOPE_GLOBAL");
const scopeId = ref("");
const state = ref<string>("EXECUTION_CONTROL_STATE_PAUSED");
const admissionRate = ref(0);
const reason = ref("");

const reasonMissing = computed(() => reason.value.trim() === "");
const isGlobalPause = computed(
  () => scope.value === "EXECUTION_CONTROL_SCOPE_GLOBAL" && state.value === "EXECUTION_CONTROL_STATE_PAUSED",
);

const applyMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/execution-control/override", {
      body: { scope: scope.value, scope_id: scopeId.value, state: state.value, admission_rate: admissionRate.value, reason: reason.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => message.success(`Override применён, version=${data?.version}`),
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const clearMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/execution-control/override/clear", {
      body: { scope: scope.value, scope_id: scopeId.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => message.success(`Override снят, version=${data?.version}`),
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmApply() {
  if (reasonMissing.value) {
    message.warning("reason обязателен для execution-control override");
    return;
  }
  dialog.warning({
    title: "Подтвердите Apply Override",
    content: () =>
      `scope=${scope.value}${scopeId.value ? `, scope_id=${scopeId.value}` : ""}, state=${state.value}` +
      (isGlobalPause.value ? " — это остановит обработку ВСЕГО трафика платформы (GLOBAL/PAUSED)." : "."),
    positiveText: "Apply Override",
    negativeText: "Отмена",
    onPositiveClick: () => applyMutation.mutate(),
  });
}

function confirmClear() {
  dialog.warning({
    title: "Подтвердите Clear Override",
    content: `scope=${scope.value}${scopeId.value ? `, scope_id=${scopeId.value}` : ""} — override будет снят.`,
    positiveText: "Clear Override",
    negativeText: "Отмена",
    onPositiveClick: () => clearMutation.mutate(),
  });
}
</script>

<template>
  <RequireAdmin>
    <NCard title="Execution Control — ручное вмешательство">
      <NAlert v-if="isGlobalPause" type="error" style="margin-bottom: 16px" title="Опасно">
        GLOBAL + PAUSED останавливает обработку всего трафика платформы.
      </NAlert>
      <NForm label-placement="top" style="max-width: 480px">
        <NFormItem label="scope">
          <NSelect v-model:value="scope" :options="scopeOptions" />
        </NFormItem>
        <NFormItem label="scope_id">
          <NInput v-model:value="scopeId" placeholder="напр. acme:billing, пусто для GLOBAL" />
        </NFormItem>
        <NFormItem label="state">
          <NSelect v-model:value="state" :options="stateOptions" />
        </NFormItem>
        <NFormItem label="admission_rate">
          <NInputNumber v-model:value="admissionRate" :min="0" :max="1" :step="0.05" style="width: 100%" />
        </NFormItem>
        <NFormItem label="reason" :feedback="reasonMissing ? 'обязательное поле' : undefined" :validation-status="reasonMissing ? 'error' : undefined">
          <NInput v-model:value="reason" type="textarea" placeholder="обязательно — попадает в audit log" />
        </NFormItem>
        <NSpace>
          <NButton
            type="primary"
            :disabled="reasonMissing"
            :loading="applyMutation.isPending.value"
            @click="confirmApply"
          >
            Apply Override
          </NButton>
          <NButton type="warning" :loading="clearMutation.isPending.value" @click="confirmClear">
            Clear Override
          </NButton>
        </NSpace>
      </NForm>
    </NCard>
  </RequireAdmin>
</template>
