// Covers the "Rotate partner credential" card added for luminous-hugging-charm.md
// Ф1 (credential-issuer-service): permission-gated visibility (`credentials:issue`,
// nested RequirePermission — the rest of ConfigView stays visible/functional
// for users without it, only this one card is gated), the rotate flow
// (confirm dialog before POST fires, form disabled until both fields filled),
// and the show-once modal (renders the returned plaintext, does not refetch
// or persist it after closing). Mirrors the mount/mock conventions used by
// AccessControlView.test.ts.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import ConfigView from "./ConfigView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientKey } from "../api/useApi";

function bodyButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll("button")).filter((b) => b.textContent?.includes(text)) as HTMLButtonElement[];
}

// ConfigView already has entity_type/entity_id inputs above the rotate
// card in the DOM — locate the "Rotate partner credential" NCard by its
// title text and scope the input query to that subtree, so this doesn't
// silently start reading the wrong form's fields if either form's layout
// changes.
function rotateCardInputs(_wrapper: unknown) {
  // mount() uses `attachTo: document.body`, same as bodyButtons() above —
  // query the real DOM directly rather than through the wrapper's (loosely
  // typed in this generic-Host-component context) `.element`.
  const cards = Array.from(document.body.querySelectorAll(".n-card"));
  const rotateCard = cards.find((c) => c.textContent?.includes("Rotate partner credential"));
  if (!rotateCard) throw new Error("карта 'Rotate partner credential' не найдена в DOM");
  return Array.from(rotateCard.querySelectorAll('input[type="text"], input:not([type])')).map((el) => ({
    setValue: async (v: string) => {
      (el as HTMLInputElement).value = v;
      el.dispatchEvent(new Event("input"));
      await flushPromises();
    },
  }));
}

interface FakeApi {
  GET: ReturnType<typeof vi.fn>;
  POST: ReturnType<typeof vi.fn>;
}

async function mountView(fakeApi: FakeApi) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(ConfigView) }),
      }),
  });

  return mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientKey as symbol]: fakeApi },
    },
  });
}

function defaultGet() {
  return vi.fn(async () => ({ data: { versions: [], next_page_token: "" }, error: undefined }));
}

describe("ConfigView — Rotate partner credential", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("без credentials:issue карта ротации скрыта, остальной экран виден", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeApi: FakeApi = { GET: defaultGet(), POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(wrapper.text()).not.toContain("Rotate credential");
    // Остальной экран (создание версии конфига) остаётся функционален.
    expect(wrapper.text()).toContain("Configuration");
  });

  it("с credentials:issue показывает карту ротации, кнопка отключена без обоих полей", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["credentials:issue"];
    auth.meLoaded = true;
    const fakeApi: FakeApi = { GET: defaultGet(), POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(wrapper.text()).toContain("Rotate credential");
    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Rotate credential"))!;
    expect(rotateButton.attributes("disabled")).toBeDefined();
  });

  it("ротация требует подтверждения в диалоге перед вызовом POST, затем показывает show-once модалку", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["credentials:issue"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/config/versions"
        ? { data: { versions: [], next_page_token: "" }, error: undefined }
        : { data: { secrets: [] }, error: undefined },
    );
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { credential_ref: "vault://partners/click_uz/main/api_key", secret_version: 2, plaintext_secret: "brand-new-secret-value", issued_at: "2026-01-01T00:00:00Z" },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    // ConfigView уже несёт entity_type/entity_id inputs выше в DOM — скоупим
    // выборку к самой карте ротации (её DOM-предок), а не к первым двум
    // input'ам на всей странице.
    const [partnerIdInput, applicationIdInput] = rotateCardInputs(wrapper);
    await partnerIdInput.setValue("click_uz");
    await applicationIdInput.setValue("click_uz_main");
    await flushPromises();

    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Rotate credential"))!;
    expect(rotateButton.attributes("disabled")).toBeUndefined();
    await rotateButton.trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Ротировать").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/partners/{partner_id}/applications/{application_id}/credentials/rotate");
    expect(
      (fakePost.mock.calls[0][1] as { params: { path: { partner_id: string; application_id: string } } }).params.path,
    ).toEqual({ partner_id: "click_uz", application_id: "click_uz_main" });

    // Show-once модалка реально отображает возвращённый plaintext.
    expect(document.body.textContent).toContain("brand-new-secret-value");
  });

  it("закрытие show-once модалки не оставляет plaintext доступным где-либо ещё", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["credentials:issue"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/config/versions"
        ? { data: { versions: [], next_page_token: "" }, error: undefined }
        : { data: { secrets: [] }, error: undefined },
    );
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { credential_ref: "vault://partners/click_uz/main/api_key", secret_version: 3, plaintext_secret: "one-time-secret-xyz", issued_at: "2026-01-01T00:00:00Z" },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const [partnerIdInput, applicationIdInput] = rotateCardInputs(wrapper);
    await partnerIdInput.setValue("click_uz");
    await applicationIdInput.setValue("click_uz_main");
    await flushPromises();

    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Rotate credential"))!;
    await rotateButton.trigger("click");
    await flushPromises();
    const confirmButtons = bodyButtons("Ротировать").filter((b) => !wrapper.element.contains(b));
    confirmButtons[0].click();
    await flushPromises();

    expect(document.body.textContent).toContain("one-time-secret-xyz");

    const closeButton = bodyButtons("Готово, закрыть")[0];
    closeButton.click();
    await flushPromises();

    expect(document.body.textContent).not.toContain("one-time-secret-xyz");
    // Единственный вызов POST — повторное открытие/закрытие не перечитывает секрет откуда-либо.
    expect(fakePost).toHaveBeenCalledTimes(1);
  });
});

// luminous-hugging-charm.md Ф10 — "Проверить" (validate) and "Diff" cards.
describe("ConfigView — Preview/Diff (Ф10)", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("'Проверить' отправляет POST /config/versions/validate и показывает ошибки, не бросает исключение", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakePost = vi.fn(async (path: string) =>
      path === "/config/versions/validate"
        ? { data: { valid: false, errors: ["entity_id: is required"] }, error: undefined }
        : { data: undefined, error: undefined },
    );
    const fakeApi: FakeApi = { GET: defaultGet(), POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const checkButton = wrapper.findAll("button").find((b) => b.text().includes("Проверить"))!;
    await checkButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledWith(
      "/config/versions/validate",
      expect.objectContaining({ body: expect.objectContaining({ entity_type: "CONFIG_ENTITY_TYPE_PIPELINE" }) }),
    );
    expect(wrapper.text()).toContain("entity_id: is required");
  });

  it("'Проверить' с valid:true показывает сообщение об успехе, не ошибку", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakePost = vi.fn(async () => ({ data: { valid: true, errors: [] }, error: undefined }));
    const fakeApi: FakeApi = { GET: defaultGet(), POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const checkButton = wrapper.findAll("button").find((b) => b.text().includes("Проверить"))!;
    await checkButton.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("Валиден");
  });

  it("Diff-кнопка отключена, пока entity_id и обе версии не заполнены", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeApi: FakeApi = { GET: defaultGet(), POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const diffButton = wrapper.findAll("button").find((b) => b.text().includes("Показать diff"))!;
    expect(diffButton.attributes("disabled")).toBeDefined();
  });

  it("Diff запрашивает GET /config/versions/diff с from/to и показывает оба payload", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeGet = vi.fn(async (path: string) =>
      path === "/config/versions/diff"
        ? {
            data: { from_version: 1, from_payload_json: { n: 1 }, to_version: 2, to_payload_json: { n: 2 } },
            error: undefined,
          }
        : { data: { versions: [], next_page_token: "" }, error: undefined },
    );
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    // entity_id — единственный "голый" text input в верхней форме создания
    // версии (entity_type уже предзаполнен дефолтным значением).
    const entityIdInputs = wrapper.findAll("input").filter((i) => (i.element as HTMLInputElement).value === "");
    await entityIdInputs[0].setValue("acme");

    const numberInputs = wrapper.findAll('input[type="text"]').filter((i) => {
      const card = (i.element as HTMLElement).closest(".n-card");
      return card?.textContent?.includes("Diff — сравнить две версии");
    });
    // naive-ui NInputNumber рендерит текстовый input внутри — from/to, в порядке появления.
    await numberInputs[0].setValue("1");
    await numberInputs[1].setValue("2");
    await flushPromises();

    const diffButton = wrapper.findAll("button").find((b) => b.text().includes("Показать diff"))!;
    expect(diffButton.attributes("disabled")).toBeUndefined();
    await diffButton.trigger("click");
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledWith(
      "/config/versions/diff",
      expect.objectContaining({ params: expect.objectContaining({ query: expect.objectContaining({ from: 1, to: 2 }) }) }),
    );
    expect(wrapper.text()).toContain("Версия 1");
    expect(wrapper.text()).toContain("Версия 2");
  });
});
