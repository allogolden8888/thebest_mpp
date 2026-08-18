// Покрывает: (1) пикер application_id — автовыбор первого application из
// GET /applications; (2) "Webhook не настроен" для пустого
// notification_callback_url вместо пустого на вид поля; (3) форма
// редактирования гейтится auth.isAdmin() — не-admin не может отправить PUT
// даже кликом (input задизейблен, кнопка задизейблена); (4) admin с
// невалидным URL не может отправить PUT (кнопка задизейблена клиентской
// санити-проверкой); (5) test-send отделён от формы редактирования и
// показывает non-2xx http_status как штатный результат (не как ошибку) —
// см. doc-комментарий в WebhookView.vue и services/partner-self-service-api/
// internal/httpapi/webhook.go.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider } from "naive-ui";
import WebhookView from "./WebhookView.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";
import { apiClientsKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

const APPLICATIONS = [
  {
    application_id: "app-1",
    display_name: "App One",
    auth: { type: "API_KEY", credential_ref: "cred-1" },
    rate_limit_tps: 10,
    allowed_channels: ["SMS"],
  },
];

function fakePartnerClient(opts: {
  webhookUrl: string;
  putImpl?: ReturnType<typeof vi.fn>;
  postImpl?: ReturnType<typeof vi.fn>;
}) {
  const fakeGet = vi.fn(async (path: string, req?: { params?: { path?: { application_id?: string } } }) => {
    if (path === "/applications") {
      return { data: APPLICATIONS, error: undefined };
    }
    if (path === "/applications/{application_id}/webhook") {
      return {
        data: { application_id: req?.params?.path?.application_id, notification_callback_url: opts.webhookUrl },
        error: undefined,
      };
    }
    throw new Error(`unexpected GET ${path}`);
  });

  return {
    GET: fakeGet,
    PUT: opts.putImpl ?? vi.fn(),
    POST: opts.postImpl ?? vi.fn(),
  };
}

async function mountView(partnerClient: ReturnType<typeof fakePartnerClient>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () => h(NMessageProvider, null, { default: () => h(WebhookView) }),
  });

  const wrapper = mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: {
        [apiClientsKey as symbol]: { partner: partnerClient, billing: { GET: vi.fn(), PUT: vi.fn(), POST: vi.fn() } },
      },
    },
  });
  return wrapper;
}

describe("WebhookView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("автовыбирает первый application и показывает 'не настроен' для пустого URL", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const partnerClient = fakePartnerClient({ webhookUrl: "" });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("App One");
    expect(wrapper.text()).toContain("не настроен");
    expect(partnerClient.GET).toHaveBeenCalledWith(
      "/applications/{application_id}/webhook",
      expect.objectContaining({ params: { path: { application_id: "app-1" } } }),
    );
  });

  it("показывает сохранённый URL, когда он задан", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const partnerClient = fakePartnerClient({ webhookUrl: "https://partner.example.com/hooks" });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("https://partner.example.com/hooks");
  });

  it("не-admin: поле редактирования задизейблено, PUT не отправляется", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const putImpl = vi.fn(async () => ({ data: {}, error: undefined }));
    const partnerClient = fakePartnerClient({ webhookUrl: "https://old.example.com", putImpl });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    const input = wrapper.find("input[type='text'], input:not([type])");
    expect(input.attributes("disabled")).toBeDefined();

    const saveButton = wrapper.findAll("button").find((b) => b.text().includes("Сохранить"))!;
    await saveButton.trigger("click");
    await flushPromises();

    expect(putImpl).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain(PARTNER_ADMIN_ROLE);
  });

  it("admin с невалидным URL: кнопка 'Сохранить' задизейблена, PUT не отправляется", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const putImpl = vi.fn(async () => ({ data: {}, error: undefined }));
    const partnerClient = fakePartnerClient({ webhookUrl: "", putImpl });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    const inputs = wrapper.findAll("input").filter((i) => i.attributes("disabled") === undefined);
    const urlInput = inputs[0];
    await urlInput.setValue("javascript:alert(1)");
    await flushPromises();

    const saveButton = wrapper.findAll("button").find((b) => b.text().includes("Сохранить"))!;
    await saveButton.trigger("click");
    await flushPromises();

    expect(putImpl).not.toHaveBeenCalled();
  });

  it("admin с валидным URL: клик 'Сохранить' отправляет PUT с введённым URL", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const putImpl = vi.fn(async (..._args: unknown[]) => ({
      data: { application_id: "app-1", notification_callback_url: "https://new.example.com/hooks" },
      error: undefined,
    }));
    const partnerClient = fakePartnerClient({ webhookUrl: "", putImpl });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    const inputs = wrapper.findAll("input").filter((i) => i.attributes("disabled") === undefined);
    const urlInput = inputs[0];
    await urlInput.setValue("https://new.example.com/hooks");
    await flushPromises();

    const saveButton = wrapper.findAll("button").find((b) => b.text().includes("Сохранить"))!;
    await saveButton.trigger("click");
    await flushPromises();

    expect(putImpl).toHaveBeenCalledTimes(1);
    const [path, req] = putImpl.mock.calls[0] as unknown as [string, { params: { path: { application_id: string } }; body: { notification_callback_url: string } }];
    expect(path).toBe("/applications/{application_id}/webhook");
    expect(req.params.path.application_id).toBe("app-1");
    expect(req.body.notification_callback_url).toBe("https://new.example.com/hooks");
  });

  it("test-send: non-2xx http_status отображается как обычный результат, не как ошибка", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const postImpl = vi.fn(async (..._args: unknown[]) => ({ data: { http_status: 500, latency_ms: 340 }, error: undefined }));
    const partnerClient = fakePartnerClient({ webhookUrl: "https://partner.example.com/hooks", postImpl });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    const testButton = wrapper.findAll("button").find((b) => b.text().includes("Отправить тестовое событие"))!;
    await testButton.trigger("click");
    await flushPromises();

    expect(postImpl).toHaveBeenCalledTimes(1);
    const [path, req] = postImpl.mock.calls[0] as unknown as [string, { params: { path: { application_id: string } } }];
    expect(path).toBe("/applications/{application_id}/webhook/test");
    expect(req.params.path.application_id).toBe("app-1");
    expect(wrapper.text()).toContain("HTTP 500");
    expect(wrapper.text()).toContain("340");
  });

  it("test-send: кнопка задизейблена, если webhook ещё не сохранён", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const postImpl = vi.fn();
    const partnerClient = fakePartnerClient({ webhookUrl: "", postImpl });

    const wrapper = await mountView(partnerClient);
    await flushPromises();
    await flushPromises();

    const testButton = wrapper.findAll("button").find((b) => b.text().includes("Отправить тестовое событие"))!;
    expect(testButton.attributes("disabled")).toBeDefined();

    await testButton.trigger("click");
    await flushPromises();
    expect(postImpl).not.toHaveBeenCalled();
  });
});
