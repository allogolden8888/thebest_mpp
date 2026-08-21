<script setup lang="ts">
// BACKOFFICE_ROADMAP.md §4 (Blacklist) — proxy на compliance-api через
// backoffice-api (GET/POST /v1/compliance/consent). Поиск ТОЛЬКО по msisdn —
// msisdn не индексируется реверсивно в Runtime Redis (см. §4 и
// compliance-api/README.md), "показать все заблокированные" структурно
// недостижимо, честное ограничение, не баг этого экрана.
//
// Ручная блокировка/разблокировка — за RequirePermission("compliance:write"),
// тот же permission, что и compliance-api сам гейтит POST (роль
// compliance-officer). Просмотр — без гейта, тот же выбор, что уже принят
// самим compliance-api (GET открыт любому валидному токену realm'а).
import { ref } from "vue";
import { useMutation, useQuery, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NSelect, NButton, NAlert, NSpace, NTag, NText, useMessage } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import RequirePermission from "../components/RequirePermission.vue";

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();

// --- Поиск по msisdn ---
const searchMsisdn = ref("");
const lookedUpMsisdn = ref<string | null>(null);

const consentQuery = useQuery({
  queryKey: ["compliance-consent", lookedUpMsisdn],
  queryFn: async () => {
    const { data, error } = await api.GET("/compliance/consent", {
      params: { query: { msisdn: lookedUpMsisdn.value! } },
    });
    if (error) throw error;
    return data;
  },
  enabled: false,
});

function runSearch() {
  if (!searchMsisdn.value) return;
  lookedUpMsisdn.value = searchMsisdn.value;
  void consentQuery.refetch();
}

// --- Ручная блокировка/разблокировка ---
const form = ref({
  msisdn: "",
  scope_type: "CATEGORY" as "CATEGORY" | "SENDER",
  scope_value: "",
  channel: "SMS",
  action: "block" as "block" | "unblock",
  reason: "",
});

const submitMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/compliance/consent", { body: form.value });
    if (error) throw error;
    return data;
  },
  onSuccess: () => {
    message.success("Запись сохранена");
    void queryClient.invalidateQueries({ queryKey: ["compliance-consent"] });
  },
});
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Blacklist — проверка по msisdn">
      <NForm inline label-placement="top">
        <NFormItem label="msisdn">
          <NInput v-model:value="searchMsisdn" style="width: 220px" placeholder="998901234567" @keyup.enter="runSearch" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton type="primary" :disabled="!searchMsisdn" @click="runSearch">Проверить</NButton>
        </NFormItem>
      </NForm>
      <NAlert v-if="consentQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(consentQuery.error.value) }}
      </NAlert>
      <template v-if="consentQuery.data.value">
        <NText depth="3" style="display: block; margin-bottom: 8px">msisdn: {{ consentQuery.data.value.msisdn }}</NText>
        <NSpace vertical size="small">
          <div>
            <NText depth="3">Заблокированные категории: </NText>
            <template v-if="consentQuery.data.value.blocked_categories.length">
              <NTag v-for="c in consentQuery.data.value.blocked_categories" :key="c" type="error" size="small" style="margin-right: 4px">{{ c }}</NTag>
            </template>
            <NText v-else depth="3">—</NText>
          </div>
          <div>
            <NText depth="3">Заблокированные отправители: </NText>
            <template v-if="consentQuery.data.value.blocked_senders.length">
              <NTag v-for="s in consentQuery.data.value.blocked_senders" :key="s" type="error" size="small" style="margin-right: 4px">{{ s }}</NTag>
            </template>
            <NText v-else depth="3">—</NText>
          </div>
        </NSpace>
      </template>
    </NCard>

    <RequirePermission permission="compliance:write">
      <NCard title="Blacklist — ручная блокировка/разблокировка">
        <NForm label-placement="top" style="max-width: 480px">
          <NFormItem label="msisdn">
            <NInput v-model:value="form.msisdn" placeholder="998901234567" />
          </NFormItem>
          <NFormItem label="scope_type">
            <NSelect
              v-model:value="form.scope_type"
              :options="[
                { label: 'CATEGORY', value: 'CATEGORY' },
                { label: 'SENDER', value: 'SENDER' },
              ]"
            />
          </NFormItem>
          <NFormItem label="scope_value">
            <NInput
              v-model:value="form.scope_value"
              :placeholder="form.scope_type === 'CATEGORY' ? 'SERVICE / TRANSACTION / ADVERTISING / UNTEMPLATED / BLOCKED' : 'sender id'"
            />
          </NFormItem>
          <NFormItem label="channel">
            <NInput v-model:value="form.channel" />
          </NFormItem>
          <NFormItem label="action">
            <NSelect
              v-model:value="form.action"
              :options="[
                { label: 'block', value: 'block' },
                { label: 'unblock', value: 'unblock' },
              ]"
            />
          </NFormItem>
          <NFormItem label="reason">
            <NInput v-model:value="form.reason" type="textarea" placeholder="жалоба абонента / требование регулятора / ..." />
          </NFormItem>
          <NAlert v-if="submitMutation.isError.value" type="error" style="margin-bottom: 12px">
            {{ extractErrorMessage(submitMutation.error.value) }}
          </NAlert>
          <NButton
            type="primary"
            :loading="submitMutation.isPending.value"
            :disabled="!form.msisdn || !form.scope_value || !form.channel || !form.reason"
            @click="submitMutation.mutate()"
          >
            Сохранить
          </NButton>
        </NForm>
      </NCard>
    </RequirePermission>
  </NSpace>
</template>
