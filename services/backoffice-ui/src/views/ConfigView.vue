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
import { computed, h, ref, watch } from "vue";
import { useQuery, useMutation, useQueryClient } from "@tanstack/vue-query";
import { NCard, NForm, NFormItem, NInput, NInputNumber, NButton, NDataTable, NSpace, NAlert, NModal, NText, useMessage, useDialog, type DataTableColumns } from "naive-ui";
import { useApi } from "../api/useApi";
import { extractErrorMessage } from "../api/errorMessage";
import { useAuthStore } from "../stores/auth";
import RequirePermission from "../components/RequirePermission.vue";
import type { components } from "../api/schema";

type ConfigVersion = components["schemas"]["ConfigVersion"];
type IssuedSecret = components["schemas"]["IssuedSecret"];
type RotateCredentialResponse = components["schemas"]["RotateCredentialResponse"];

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

// luminous-hugging-charm.md Ф10 — "Preview" half: validate the draft
// payload_json against its JSON Schema BEFORE publishing (POST
// /config/versions/validate writes nothing — see backoffice-api's
// package doc on that handler). Separate from createMutation: a user can
// check validity repeatedly while editing without ever calling
// createMutation, and a failed validation is a normal 200 response
// (valid: false), not an error — validateResult is plain reactive state,
// not useMutation's error branch.
const validateResult = ref<{ valid: boolean; errors: string[] } | null>(null);
// Stale validation result from a previous draft would be misleading once
// the user keeps typing — clear it, don't let an old "valid" carry over
// to text it never actually checked.
watch([entityType, payloadJson], () => {
  validateResult.value = null;
});
const validateMutation = useMutation({
  mutationFn: async () => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payloadJson.value);
    } catch {
      throw new Error("payload_json должен быть валидным JSON");
    }
    const { data, error } = await api.POST("/config/versions/validate", {
      body: { entity_type: entityType.value, payload_json: parsed },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    validateResult.value = data ?? null;
  },
  onError: (err: unknown) => {
    validateResult.value = null;
    message.error(extractErrorMessage(err));
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

// luminous-hugging-charm.md Ф10 — "Diff" half: compare two ALREADY
// PERSISTED versions of the currently-viewed entity (not the unsaved
// draft above — DiffVersions only knows about real config_versions rows,
// see backoffice-api's package doc on GET /config/versions/diff). Manual
// trigger (button), not a reactive useQuery keyed on the two version
// numbers — a half-typed version number mid-edit shouldn't fire a request
// on every keystroke.
const diffFromVersion = ref<number | null>(null);
const diffToVersion = ref<number | null>(null);
const diffResult = ref<{ fromVersion: number; fromPayload: string; toVersion: number; toPayload: string } | null>(null);
const diffFormInvalid = computed(
  () => !entityId.value.trim() || diffFromVersion.value === null || diffToVersion.value === null,
);

const diffMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.GET("/config/versions/diff", {
      params: {
        query: {
          entity_type: entityType.value,
          entity_id: entityId.value,
          from: diffFromVersion.value as number,
          to: diffToVersion.value as number,
        },
      },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    if (!data) return;
    diffResult.value = {
      fromVersion: data.from_version,
      fromPayload: JSON.stringify(data.from_payload_json, null, 2),
      toVersion: data.to_version,
      toPayload: JSON.stringify(data.to_payload_json, null, 2),
    };
  },
  onError: (err: unknown) => {
    diffResult.value = null;
    message.error(extractErrorMessage(err));
  },
});

// Ф1 (luminous-hugging-charm.md, credential-issuer-service) — "Rotate
// credential". ConfigView.vue's payload_json editor is a raw textarea, not
// a structured per-application view (this codebase has no parsed rendering
// of PARTNER config's applications[] anywhere yet), so this is a
// self-contained card taking partner_id/application_id directly — same
// REST contract backoffice-api exposes, no dependency on the raw-JSON form
// above. Gated by `credentials:issue` (RequirePermission, same dual-layer
// pattern as AccessControlView.vue — hidden entirely for users without the
// permission, not just disabled).
const hasCredentialsIssue = computed(() => auth.hasPermission("credentials:issue"));

const rotatePartnerId = ref("");
const rotateApplicationId = ref("");
const rotateFormInvalid = computed(() => !rotatePartnerId.value.trim() || !rotateApplicationId.value.trim());

const issuedSecretModal = ref<RotateCredentialResponse | null>(null);

const issuedSecretsQuery = useQuery({
  queryKey: ["issued-secrets", rotatePartnerId],
  enabled: computed(() => hasCredentialsIssue.value && !!rotatePartnerId.value.trim()),
  queryFn: async () => {
    const { data, error } = await api.GET("/partners/{partner_id}/credentials", {
      params: { path: { partner_id: rotatePartnerId.value.trim() } },
    });
    if (error) throw error;
    return data;
  },
});

const rotateMutation = useMutation({
  mutationFn: async () => {
    const { data, error } = await api.POST("/partners/{partner_id}/applications/{application_id}/credentials/rotate", {
      params: { path: { partner_id: rotatePartnerId.value.trim(), application_id: rotateApplicationId.value.trim() } },
    });
    if (error) throw error;
    return data;
  },
  onSuccess: (data) => {
    // Show-once: этот ответ — единственное место, где plaintext_secret
    // вообще появляется. Модалка не перечитывает его ниоткуда после
    // закрытия — закрыл значит потерял, партнёру нужно запросить новую
    // ротацию, если не скопировал вовремя.
    issuedSecretModal.value = data ?? null;
    queryClient.invalidateQueries({ queryKey: ["issued-secrets", rotatePartnerId.value] });
  },
  onError: (err: unknown) => message.error(extractErrorMessage(err)),
});

function confirmRotate() {
  dialog.warning({
    title: "Подтвердите ротацию credential",
    content: `Новый секрет будет сгенерирован для ${rotatePartnerId.value.trim()}/${rotateApplicationId.value.trim()}. `
      + `Старый секрет немедленно перестанет действовать. Действие необратимо.`,
    positiveText: "Ротировать",
    negativeText: "Отмена",
    onPositiveClick: () => rotateMutation.mutate(),
  });
}

async function copyPlaintextToClipboard() {
  if (!issuedSecretModal.value) return;
  try {
    await navigator.clipboard.writeText(issuedSecretModal.value.plaintext_secret);
    message.success("Скопировано в буфер обмена");
  } catch {
    message.error("Не удалось скопировать автоматически — выделите и скопируйте вручную");
  }
}

const issuedSecretsColumns: DataTableColumns<IssuedSecret> = [
  { title: "application_id", key: "application_id" },
  { title: "version", key: "secret_version" },
  { title: "status", key: "status" },
  { title: "issued_by", key: "issued_by" },
  { title: "issued_at", key: "issued_at" },
];

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
          <NSpace>
            <!-- luminous-hugging-charm.md Ф10 — "Preview": проверить payload_json
                 против JSON Schema ДО публикации, без gate прав (ничего не
                 пишет) — доступно даже без backoffice-admin, в отличие от
                 самого "Создать версию" ниже. -->
            <NButton :loading="validateMutation.isPending.value" @click="validateMutation.mutate()">
              Проверить
            </NButton>
            <NButton
              type="primary"
              :disabled="!auth.isAdmin()"
              :loading="createMutation.isPending.value"
              @click="createMutation.mutate()"
            >
              Создать версию
            </NButton>
          </NSpace>
        </NFormItem>
      </NForm>
      <NAlert v-if="validateResult?.valid" type="success" style="margin-top: 12px">
        Валиден — ошибок схемы/семантики не найдено.
      </NAlert>
      <NAlert v-else-if="validateResult && !validateResult.valid" type="error" style="margin-top: 12px">
        <ul style="margin: 0; padding-left: 20px">
          <li v-for="(e, i) in validateResult.errors" :key="i">{{ e }}</li>
        </ul>
      </NAlert>
      <NAlert v-if="!auth.isAdmin()" type="info" style="margin-top: 12px">
        Создание/архивирование версий конфигурации требует роль backoffice-admin. Просмотр версий и проверка — доступны без неё.
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

    <NCard title="Diff — сравнить две версии">
      <NForm inline label-placement="top">
        <NFormItem label="from">
          <NInputNumber v-model:value="diffFromVersion" :min="1" style="width: 120px" />
        </NFormItem>
        <NFormItem label="to">
          <NInputNumber v-model:value="diffToVersion" :min="1" style="width: 120px" />
        </NFormItem>
        <NFormItem label=" ">
          <NButton :disabled="diffFormInvalid" :loading="diffMutation.isPending.value" @click="diffMutation.mutate()">
            Показать diff
          </NButton>
        </NFormItem>
      </NForm>
      <NAlert type="info" style="margin-top: 8px">
        Сравнивает две уже опубликованные версии текущей сущности (entity_type/entity_id из формы выше) — не
        черновик payload_json, тот проверяется кнопкой "Проверить".
      </NAlert>
      <NSpace v-if="diffResult" :size="16" style="margin-top: 12px" align="start">
        <div style="flex: 1; min-width: 0">
          <NText strong>Версия {{ diffResult.fromVersion }}</NText>
          <pre style="overflow-x: auto; background: rgba(128,128,128,0.08); padding: 8px; border-radius: 4px">{{ diffResult.fromPayload }}</pre>
        </div>
        <div style="flex: 1; min-width: 0">
          <NText strong>Версия {{ diffResult.toVersion }}</NText>
          <pre style="overflow-x: auto; background: rgba(128,128,128,0.08); padding: 8px; border-radius: 4px">{{ diffResult.toPayload }}</pre>
        </div>
      </NSpace>
    </NCard>

    <RequirePermission permission="credentials:issue">
      <NCard title="Rotate partner credential">
        <NForm inline label-placement="top">
          <NFormItem label="partner_id">
            <NInput v-model:value="rotatePartnerId" style="width: 200px" />
          </NFormItem>
          <NFormItem label="application_id">
            <NInput v-model:value="rotateApplicationId" style="width: 200px" />
          </NFormItem>
          <NFormItem label=" ">
            <NButton
              type="warning"
              :disabled="rotateFormInvalid"
              :loading="rotateMutation.isPending.value"
              @click="confirmRotate"
            >
              Rotate credential
            </NButton>
          </NFormItem>
        </NForm>
        <NAlert type="info" style="margin-top: 8px">
          Генерирует новый секрет и немедленно инвалидирует предыдущий — не требует передеплоя. Секрет будет показан
          РОВНО ОДИН РАЗ сразу после ротации.
        </NAlert>

        <template v-if="rotatePartnerId.trim()">
          <NAlert v-if="issuedSecretsQuery.isError.value" type="error" style="margin-top: 12px">
            {{ extractErrorMessage(issuedSecretsQuery.error.value) }}
          </NAlert>
          <NDataTable
            style="margin-top: 12px"
            :columns="issuedSecretsColumns"
            :data="issuedSecretsQuery.data.value?.secrets ?? []"
            :loading="issuedSecretsQuery.isLoading.value"
            :row-key="(row: IssuedSecret) => row.id"
          />
        </template>
      </NCard>
    </RequirePermission>

    <NModal
      :show="issuedSecretModal !== null"
      preset="dialog"
      title="Новый credential — сохраните сейчас"
      :closable="false"
      :mask-closable="false"
      @update:show="(v: boolean) => { if (!v) issuedSecretModal = null; }"
    >
      <NAlert type="warning" style="margin-bottom: 12px">
        Этот секрет показывается только один раз и нигде не сохраняется в открытом виде. Скопируйте его сейчас — после
        закрытия окна получить его снова будет невозможно (только новая ротация).
      </NAlert>
      <NText code style="word-break: break-all">{{ issuedSecretModal?.plaintext_secret }}</NText>
      <template #action>
        <NSpace>
          <NButton @click="copyPlaintextToClipboard">Скопировать</NButton>
          <NButton type="primary" @click="issuedSecretModal = null">Готово, закрыть</NButton>
        </NSpace>
      </template>
    </NModal>
  </NSpace>
</template>