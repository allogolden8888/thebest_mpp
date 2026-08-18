<script setup lang="ts">
// GET /credentials + POST /applications/{application_id}/credentials/rotate
// — см. openapi-partner.yaml и
// services/partner-self-service-api/internal/httpapi/credentials.go за
// авторитетной формой.
//
// Роли (см. credentials.go doc-комментарии): listCredentials — открыт
// любому аутентифицированному партнёрскому токену (без гейта). rotate —
// `auth.RequireAdmin` на сервере (роль partner-admin), т.к. это
// деструктивная операция: старый credential_ref теряет силу МГНОВЕННО,
// как только rotate успешно отработал. Тот же паттерн gate'а кнопки, что
// backoffice-ui/src/views/ConfigView.vue (:disabled="!auth.isAdmin()") +
// confirm-диалог перед отправкой.
//
// Show-once reveal: RotateCredentialResponse.plaintext_secret приходит
// РОВНО один раз, в этом самом ответе — IssuedSecretSummary (то, что
// возвращает список) структурно не несёт значения секрета, так что после
// закрытия модалки это значение нигде и никогда больше не достать. Поэтому
// показываем его в персистентной NModal, которую пользователь обязан
// осознанно закрыть сам (никаких auto-close/toast — useMessage() здесь
// категорически не подходит, он тает через пару секунд и значение будет
// потеряно, если партнёр не успел скопировать).
import { h, ref } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import {
  NCard,
  NButton,
  NDataTable,
  NSpace,
  NAlert,
  NModal,
  NInput,
  NInputGroup,
  NText,
  useMessage,
  useDialog,
  type DataTableColumns,
} from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import type { components } from "../api/schema-partner";

type IssuedSecretSummary = components["schemas"]["IssuedSecretSummary"];
type RotateCredentialResponse = components["schemas"]["RotateCredentialResponse"];

const api = useApi();
const message = useMessage();
const dialog = useDialog();
const queryClient = useQueryClient();
const auth = useAuthStore();

const listQuery = useQuery({
  queryKey: ["credentials"],
  queryFn: async () => {
    const { data, error } = await api.partner.GET("/credentials", {});
    if (error) throw error;
    return data;
  },
});

// Show-once modal state — held only in-memory, never persisted, cleared on close.
const revealOpen = ref(false);
const revealed = ref<RotateCredentialResponse | null>(null);

const rotateMutation = useMutation({
  mutationFn: async (applicationId: string) => {
    const { data, error } = await api.partner.POST("/applications/{application_id}/credentials/rotate", {
      params: { path: { application_id: applicationId } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    revealed.value = data ?? null;
    revealOpen.value = true;
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmRotate(applicationId: string) {
  dialog.warning({
    title: "Подтвердите ротацию credential",
    content:
      `Ротация немедленно инвалидирует текущий секрет для application_id "${applicationId}". ` +
      "Старый credential перестанет работать СРАЗУ после успешной ротации. " +
      "Убедитесь, что вы готовы обновить его в интеграции партнёра, прежде чем продолжить.",
    positiveText: "Ротировать",
    negativeText: "Отмена",
    onPositiveClick: () => rotateMutation.mutate(applicationId),
  });
}

function closeReveal() {
  revealOpen.value = false;
  revealed.value = null;
  queryClient.invalidateQueries({ queryKey: ["credentials"] });
}

async function copySecret() {
  if (!revealed.value) return;
  try {
    await navigator.clipboard.writeText(revealed.value.plaintext_secret);
    message.success("Скопировано в буфер обмена");
  } catch (err) {
    message.error(extractErrorMessage(err));
  }
}

const columns: DataTableColumns<IssuedSecretSummary> = [
  { title: "application_id", key: "application_id" },
  { title: "credential_ref", key: "credential_ref" },
  { title: "secret_version", key: "secret_version" },
  { title: "status", key: "status" },
  { title: "issued_at", key: "issued_at" },
  { title: "issued_by", key: "issued_by" },
  {
    title: "Действие",
    key: "actions",
    render: (row) =>
      h(
        NButton,
        {
          size: "small",
          type: "warning",
          disabled: !auth.isAdmin(),
          loading: rotateMutation.isPending.value && rotateMutation.variables.value === row.application_id,
          onClick: () => confirmRotate(row.application_id),
        },
        () => "Ротировать",
      ),
  },
];
</script>

<template>
  <NSpace vertical size="large">
    <NCard title="Credentials">
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-bottom: 12px">
        Ротация credential требует роль partner-admin. Просмотр списка ниже доступен любому аутентифицированному пользователю партнёра.
      </NAlert>
      <NAlert v-if="listQuery.isError.value" type="error" style="margin-bottom: 12px">
        {{ extractErrorMessage(listQuery.error.value) }}
      </NAlert>
      <NDataTable
        :columns="columns"
        :data="listQuery.data.value ?? []"
        :loading="listQuery.isLoading.value"
        :row-key="(row: IssuedSecretSummary) => row.id"
      />
    </NCard>

    <!--
      Show-once reveal — persistent, no auto-close, no mask-click dismiss
      (closable/maskClosable false): the user must consciously click
      "Я сохранил секрет — закрыть" to acknowledge the value is gone for
      good. A transient useMessage() toast would risk losing the secret
      before it's copied.
    -->
    <NModal
      :show="revealOpen"
      :mask-closable="false"
      :closable="false"
      preset="card"
      title="Новый секрет выпущен"
      style="width: 560px"
    >
      <NAlert type="warning" style="margin-bottom: 16px">
        Это значение показывается ОДИН РАЗ и никогда не будет доступно повторно — ни на этом экране, ни где-либо ещё.
        Скопируйте его сейчас и обновите интеграцию партнёра. Старый credential уже не работает.
      </NAlert>
      <template v-if="revealed">
        <NSpace vertical>
          <div>
            <NText depth="3">credential_ref: {{ revealed.credential_ref }} (version {{ revealed.secret_version }})</NText>
          </div>
          <NInputGroup>
            <NInput :value="revealed.plaintext_secret" readonly />
            <NButton @click="copySecret">Копировать</NButton>
          </NInputGroup>
        </NSpace>
      </template>
      <template #footer>
        <NButton type="primary" @click="closeReveal">Я сохранил секрет — закрыть</NButton>
      </template>
    </NModal>
  </NSpace>
</template>
