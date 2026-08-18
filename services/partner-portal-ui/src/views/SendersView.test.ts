// Covers: non-admin sees list read-only (GET /senders has no role gate,
// only POST/PATCH do — mountSenders in applications.go); archiving an
// active sender requires confirming a dialog first (it stops future SMS
// from that sender — the only soft-delete senders has); reactivating an
// archived sender does not require confirmation.
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { mount, flushPromises, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import SendersView from "./SendersView.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";
import { apiClientsKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

const activeSender = { sender_id: "ACME", type: "ALPHANAME", status: "active" };
const archivedSender = { sender_id: "12345", type: "SHORT_NUMBER", status: "archived" };

function bodyButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll("button")).filter((b) => b.textContent?.includes(text)) as HTMLButtonElement[];
}

// Найдено при интеграции: без явного unmount() предыдущего теста naive-ui
// диалог-провайдер от него остаётся технически "живым" (Vue-инстанс не
// уничтожен, только его DOM стёрт через innerHTML="") — bodyButtons() в
// следующем тесте иногда матчит кнопку СТАРОГО диалога с уже
// осиротевшим/устаревшим onPositiveClick, что рушится как
// "onPositiveClick is not a function" асинхронно после клика. afterEach
// ниже явно размонтирует каждый wrapper.
let mountedWrapper: VueWrapper | null = null;

async function mountView(fakeGet: ReturnType<typeof vi.fn>, fakePost: ReturnType<typeof vi.fn>, fakePatch: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(SendersView) }),
      }),
  });

  const wrapper = mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: {
        [apiClientsKey as symbol]: {
          partner: { GET: fakeGet, POST: fakePost, PUT: vi.fn(), PATCH: fakePatch },
          billing: { GET: vi.fn() },
        },
      },
    },
  });
  mountedWrapper = wrapper;
  return wrapper;
}

describe("SendersView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  afterEach(() => {
    mountedWrapper?.unmount();
    mountedWrapper = null;
  });

  it("рендерит список senders из GET /senders", async () => {
    const fakeGet = vi.fn(async () => ({ data: [activeSender, archivedSender], error: undefined }));
    const wrapper = await mountView(fakeGet, vi.fn(), vi.fn());
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledWith("/senders", {});
    expect(wrapper.text()).toContain("ACME");
    expect(wrapper.text()).toContain("12345");
  });

  it("не-admin: действия задизейблены, создание недоступно", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["partner-viewer"] } }));
    const fakeGet = vi.fn(async () => ({ data: [activeSender], error: undefined }));

    const wrapper = await mountView(fakeGet, vi.fn(), vi.fn());
    await flushPromises();

    const createButton = wrapper.findAll("button").find((b) => b.text() === "Создать")!;
    const archiveButton = wrapper.findAll("button").find((b) => b.text() === "Архивировать")!;
    expect(createButton.attributes("disabled")).toBeDefined();
    expect(archiveButton.attributes("disabled")).toBeDefined();
  });

  it("admin: 'Архивировать' активного sender'а требует подтверждения в диалоге", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [activeSender], error: undefined }));
    const fakePatch = vi.fn(async () => ({ data: { ...activeSender, status: "archived" }, error: undefined }));

    const wrapper = await mountView(fakeGet, vi.fn(), fakePatch);
    await flushPromises();

    const archiveButton = wrapper.findAll("button").find((b) => b.text() === "Архивировать")!;
    await archiveButton.trigger("click");
    await flushPromises();

    expect(fakePatch).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Архивировать").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePatch).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePatch.mock.calls[0] as unknown as [string, { params: { path: { sender_id: string } }; body: { status: string } }];
    expect(path).toBe("/senders/{sender_id}");
    expect(opts.params.path.sender_id).toBe("ACME");
    expect(opts.body.status).toBe("archived");
  });

  it("admin: 'Активировать' archived sender'а отправляет PATCH сразу, без диалога", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [archivedSender], error: undefined }));
    const fakePatch = vi.fn(async () => ({ data: { ...archivedSender, status: "active" }, error: undefined }));

    const wrapper = await mountView(fakeGet, vi.fn(), fakePatch);
    await flushPromises();

    const reactivateButton = wrapper.findAll("button").find((b) => b.text() === "Активировать")!;
    await reactivateButton.trigger("click");
    await flushPromises();

    expect(fakePatch).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePatch.mock.calls[0] as unknown as [string, { params: { path: { sender_id: string } }; body: { status: string } }];
    expect(path).toBe("/senders/{sender_id}");
    expect(opts.params.path.sender_id).toBe("12345");
    expect(opts.body.status).toBe("active");
  });

  it("admin: 'Создать' отправляет CreateSenderRequest", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [], error: undefined }));
    const fakePost = vi.fn(async () => ({ data: activeSender, error: undefined }));

    const wrapper = await mountView(fakeGet, fakePost, vi.fn());
    await flushPromises();

    await wrapper.find("input").setValue("NEWSENDER");
    const createButton = wrapper.findAll("button").find((b) => b.text() === "Создать")!;
    await createButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePost.mock.calls[0] as unknown as [string, { body: { sender_id: string; type: string } }];
    expect(path).toBe("/senders");
    expect(opts.body.sender_id).toBe("NEWSENDER");
    expect(opts.body.type).toBe("ALPHANAME");
  });
});
