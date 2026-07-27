<script setup lang="ts">
// handle_execution_control_override (service_internal_methods.md §7.3) —
// ApplyOverride/ClearOverride, gRPC-проксирование в Execution Control Service.
import { ref } from "vue";
import { useMutation } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NInputNumber, NButton, NSpace, useMessage } from "naive-ui";
import { useApi } from "../api/useApi";

const api = useApi();
const message = useMessage();

const scope = ref("EXECUTION_CONTROL_SCOPE_GLOBAL");
const scopeId = ref("");
const state = ref("EXECUTION_CONTROL_STATE_PAUSED");
const admissionRate = ref(0);
const reason = ref("");

const applyMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/execution-control/override", {
      body: { scope: scope.value, scope_id: scopeId.value, state: state.value, admission_rate: admissionRate.value, reason: reason.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => message.success(`Override применён, version=${data?.version}`),
  onError: (err: unknown) => message.error(String(err)),
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
  onError: (err: unknown) => message.error(String(err)),
});
</script>

<template>
  <NCard title="Execution Control — ручное вмешательство">
    <NForm label-placement="top" style="max-width: 480px">
      <NFormItem label="scope">
        <NInput v-model:value="scope" />
      </NFormItem>
      <NFormItem label="scope_id">
        <NInput v-model:value="scopeId" placeholder="напр. acme:billing, пусто для GLOBAL" />
      </NFormItem>
      <NFormItem label="state">
        <NInput v-model:value="state" />
      </NFormItem>
      <NFormItem label="admission_rate">
        <NInputNumber v-model:value="admissionRate" :min="0" :max="1" :step="0.05" style="width: 100%" />
      </NFormItem>
      <NFormItem label="reason">
        <NInput v-model:value="reason" type="textarea" />
      </NFormItem>
      <NSpace>
        <NButton type="primary" :loading="applyMutation.isPending.value" @click="applyMutation.mutate()">
          Apply Override
        </NButton>
        <NButton type="warning" :loading="clearMutation.isPending.value" @click="clearMutation.mutate()">
          Clear Override
        </NButton>
      </NSpace>
    </NForm>
  </NCard>
</template>