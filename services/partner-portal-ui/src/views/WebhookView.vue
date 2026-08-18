<script setup lang="ts">
// GET/PUT /applications/{application_id}/webhook + POST .../webhook/test —
// см. openapi-partner.yaml и services/partner-self-service-api/internal/
// httpapi/webhook.go за авторитетной формой.
//
// notification_callback_url живёт ВНУТРИ конкретного application (не
// глобально на партнёра) — отсюда пикер application_id ниже, читающий
// GET /applications, перед тем как показать/редактировать вебхук именно
// этого application.
//
// Разграничение прав в точности как в backend router.go/webhook.go:
//  - GET .../webhook — открыт любому аутентифицированному partner-токену.
//  - PUT .../webhook (меняет URL) — требует partner-admin, форма ниже
//    гейтится через auth.isAdmin() (тот же паттерн, что ConfigView.vue в
//    backoffice-ui: не полный экран-гейт, только мутирующая форма — чтение
//    и test-send доступны partner-viewer тоже).
//  - POST .../webhook/test — тоже открыт любому аутентифицированному
//    partner-токену (не только admin), см. doc-комментарий mountWebhook в
//    webhook.go.
//
// Важное свойство безопасности, которое этот UI обязан не нарушать:
// test-send ВСЕГДА бьёт по URL, УЖЕ СОХРАНЁННОМУ в конфиге партнёра (тело
// POST .../webhook/test пустое — backend не принимает целевой URL от
// вызывающего, иначе это был бы open SSRF-прокси). Поэтому здесь НЕТ
// кнопки "проверить вот этот ещё не сохранённый URL" рядом с полем ввода —
// только отдельная кнопка "отправить тестовое событие" для уже
// сохранённого вебхука выбранного application, независимая от формы
// редактирования.
//
// Любой http_status (включая 4xx/5xx от эндпоинта самого партнёра) —
// штатный, информативный РЕЗУЛЬТАТ теста, не ошибка UI: сам handler в
// webhook.go отвечает 200 с телом WebhookTestResult в этом случае. Ошибкой
// (message.error/toast) здесь считается только отказ самого запроса —
// network error, throw из мутации (например 502 "тестовый запрос не
// выполнен" или 400 "webhook not configured", когда URL ещё не сохранён).
import { computed, ref, watch } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NButton, NSelect, NSpace, NAlert, NText, useMessage, type SelectOption } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema-partner";

type Application = components["schemas"]["Application"];
type WebhookTestResult = components["schemas"]["WebhookTestResult"];

const api = useApi();
const message = useMessage();
const queryClient = useQueryClient();
const auth = useAuthStore();

const selectedApplicationId = ref<string | null>(null);
const editUrl = ref("");
const testResult = ref<WebhookTestResult | null>(null);

const applicationsQuery = useQuery({
  queryKey: ["applications-for-webhook-picker"],
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/applications", {});
    if (error) throw error;
    return data ?? [];
  },
});

const applicationOptions = computed<SelectOption[]>(() =>
  (applicationsQuery.data.value ?? []).map((app: Application) => ({
    label: `${app.display_name} (${app.application_id})`,
    value: app.application_id,
  })),
);

// Автовыбор первого application, как только список пришёл — не оставляем
// пикер пустым, если у партнёра есть хотя бы один application.
watch(
  () => applicationsQuery.data.value,
  (apps) => {
    if (selectedApplicationId.value === null && apps && apps.length > 0) {
      selectedApplicationId.value = apps[0].application_id;
    }
  },
  { immediate: true },
);

function onSelectApplication(id: string) {
  selectedApplicationId.value = id;
  testResult.value = null;
}

const webhookQuery = useQuery({
  queryKey: ["webhook-config", selectedApplicationId],
  enabled: computed(() => selectedApplicationId.value !== null),
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/applications/{application_id}/webhook", {
      params: { path: { application_id: selectedApplicationId.value as string } },
    });
    if (error) throw error;
    return data;
  },
});

// Поле редактирования всегда синхронизировано с тем, что реально сохранено
// на бэкенде (при смене application и после успешного PUT ниже) — не
// оставляем в input значение предыдущего application.
watch(
  () => webhookQuery.data.value,
  (data) => {
    editUrl.value = data?.notification_callback_url ?? "";
  },
);

// Клиентская санити-проверка — зеркалит ожидание backend-валидации
// (net/url.ParseRequestURI + проверка схемы в webhook.go: только http/https,
// абсолютный URL с хостом). Backend остаётся единственной настоящей точкой
// принуждения — 400 оттуда всё равно обрабатывается через
// extractErrorMessage ниже, это только чтобы не пускать явный мусор.
function isPlausibleHttpUrl(value: string): boolean {
  if (!value.trim()) return false;
  try {
    const u = new URL(value);
    return (u.protocol === "http:" || u.protocol === "https:") && u.host !== "";
  } catch {
    return false;
  }
}

const editUrlInvalid = computed(() => editUrl.value.trim() !== "" && !isPlausibleHttpUrl(editUrl.value));
const canSave = computed(() => auth.isAdmin() && isPlausibleHttpUrl(editUrl.value));

const updateMutation = useMutation({
  mutationFn: async () => {
    const appId = selectedApplicationId.value;
    if (!appId) throw new Error("application не выбран");
    const { data, error } = await api.partner.PUT("/applications/{application_id}/webhook", {
      params: { path: { application_id: appId } },
      body: { notification_callback_url: editUrl.value },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    message.success("Webhook URL сохранён");
    queryClient.setQueryData(["webhook-config", selectedApplicationId.value], data);
    queryClient.invalidateQueries({ queryKey: ["webhook-config", selectedApplicationId.value] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

const testMutation = useMutation({
  mutationFn: async () => {
    const appId = selectedApplicationId.value;
    if (!appId) throw new Error("application не выбран");
    const { data, error } = await api.partner.POST("/applications/{application_id}/webhook/test", {
      params: { path: { application_id: appId } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    testResult.value = data ?? null;
  },
  onError: (err: unknown) => {
    testResult.value = null;
    message.error(extractErrorMessage(err));
  },
});

const currentSavedUrl = computed(() => webhookQuery.data.value?.notification_callback_url ?? "");
const testResultIsSuccessStatus = computed(
  () => !!testResult.value && testResult.value.http_status >= 200 && testResult.value.http_status < 300,
);
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Webhook — выбор application">
      <NAlert v-if="applicationsQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(applicationsQuery.error.value) }}
      </NAlert>
      <NAlert
        v-else-if="!applicationsQuery.isLoading.value && applicationOptions.length === 0"
        type="info"
      >
        У вашей компании пока нет ни одного application — создайте его на странице Applications, прежде чем настраивать webhook.
      </NAlert>
      <NForm v-else inline label-placement="top">
        <NFormItem label="application_id">
          <NSelect
            :value="selectedApplicationId"
            :options="applicationOptions"
            :loading="applicationsQuery.isLoading.value"
            style="width: 360px"
            placeholder="Выберите application"
            @update:value="onSelectApplication"
          />
        </NFormItem>
      </NForm>
    </NCard>

    <NCard v-if="selectedApplicationId" title="Текущий webhook">
      <NAlert v-if="webhookQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(webhookQuery.error.value) }}
      </NAlert>
      <template v-else>
        <NAlert v-if="webhookQuery.isLoading.value" type="default">Загрузка…</NAlert>
        <NAlert v-else-if="!currentSavedUrl" type="warning">
          Webhook не настроен для этого application.
        </NAlert>
        <NAlert v-else type="success">
          Текущий URL: <NText code>{{ currentSavedUrl }}</NText>
        </NAlert>
      </template>
    </NCard>

    <NCard v-if="selectedApplicationId" title="Изменить URL">
      <NForm label-placement="top" style="max-width: 480px">
        <NFormItem
          label="notification_callback_url"
          :feedback="editUrlInvalid ? 'должен быть абсолютным URL со схемой http:// или https://' : undefined"
          :validation-status="editUrlInvalid ? 'error' : undefined"
        >
          <NInput
            v-model:value="editUrl"
            :disabled="!auth.isAdmin()"
            placeholder="https://partner.example.com/webhooks/mpp"
          />
        </NFormItem>
        <NButton
          type="primary"
          :disabled="!canSave"
          :loading="updateMutation.isPending.value"
          @click="updateMutation.mutate()"
        >
          Сохранить
        </NButton>
      </NForm>
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-top: 12px">
        Изменение webhook URL требует роль partner-admin. Просмотр текущего URL и отправка тестового события выше/ниже доступны без этой роли.
      </NAlert>
    </NCard>

    <NCard v-if="selectedApplicationId" title="Тестовое событие">
      <NSpace vertical>
        <NText depth="3">
          Тест всегда отправляется на URL, уже сохранённый для этого application (не то, что сейчас введено в поле выше, если оно ещё не сохранено).
          Любой полученный HTTP-код — валидный результат теста, а не ошибка.
        </NText>
        <NButton
          :disabled="!currentSavedUrl"
          :loading="testMutation.isPending.value"
          @click="testMutation.mutate()"
        >
          Отправить тестовое событие
        </NButton>
        <NAlert v-if="!currentSavedUrl" type="default">
          Сначала сохраните webhook URL — тест шлётся только на уже сохранённый адрес.
        </NAlert>
        <NAlert v-if="testResult" :type="testResultIsSuccessStatus ? 'success' : 'warning'">
          Тест отправлен: HTTP {{ testResult.http_status }}, {{ testResult.latency_ms }} мс.
        </NAlert>
      </NSpace>
    </NCard>
  </NSpace>
</template>
