// Covers: non-admin sees list but mutating buttons disabled (defense in
// depth — GET /applications has no role gate server-side, only POST/PUT
// do, see applications.go mountApplications); create sends the expected
// CreateApplicationRequest shape; edit modal populates from the row and
// PUT never includes auth.credential_ref (that's Credentials screen's job,
// see applications.go:127-130 doc-comment).
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { mount, flushPromises, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider, NSelect } from "naive-ui";
import ApplicationsView from "./ApplicationsView.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";
import { apiClientsKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

const sampleApplication = {
  application_id: "app-1",
  display_name: "App One",
  auth: { type: "API_KEY", credential_ref: "vault://app-1/key" },
  ip_allowlist: ["10.0.0.1"],
  rate_limit_tps: 5,
  allowed_channels: ["SMS"],
  notification_callback_url: "https://example.com/hook",
};

// Найдено при интеграции: без явного unmount() предыдущего теста
// Vue-инстанс от него остаётся технически "живым" (только DOM стёрт через
// innerHTML=""), что может путать document.body-селекторы в следующем
// тесте. afterEach ниже явно размонтирует каждый wrapper.
let mountedWrapper: VueWrapper | null = null;

async function mountView(fakeGet: ReturnType<typeof vi.fn>, fakePost: ReturnType<typeof vi.fn>, fakePut: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(ApplicationsView) }),
      }),
  });

  const wrapper = mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: {
        [apiClientsKey as symbol]: {
          partner: { GET: fakeGet, POST: fakePost, PUT: fakePut, PATCH: vi.fn() },
          billing: { GET: vi.fn() },
        },
      },
    },
  });
  mountedWrapper = wrapper;
  return wrapper;
}

describe("ApplicationsView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  afterEach(() => {
    mountedWrapper?.unmount();
    mountedWrapper = null;
  });

  it("рендерит список applications из GET /applications", async () => {
    const fakeGet = vi.fn(async () => ({ data: [sampleApplication], error: undefined }));
    const wrapper = await mountView(fakeGet, vi.fn(), vi.fn());
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledWith("/applications", {});
    expect(wrapper.text()).toContain("app-1");
    expect(wrapper.text()).toContain("App One");
    expect(wrapper.text()).toContain("vault://app-1/key");
  });

  it("не-admin: кнопки 'Создать' и 'Изменить' задизейблены", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["partner-viewer"] } }));
    const fakeGet = vi.fn(async () => ({ data: [sampleApplication], error: undefined }));

    const wrapper = await mountView(fakeGet, vi.fn(), vi.fn());
    await flushPromises();

    const createButton = wrapper.findAll("button").find((b) => b.text() === "Создать")!;
    const editButton = wrapper.findAll("button").find((b) => b.text() === "Изменить")!;
    expect(createButton.attributes("disabled")).toBeDefined();
    expect(editButton.attributes("disabled")).toBeDefined();
    expect(wrapper.text()).toContain("требует роль partner-admin");
  });

  it("admin: 'Создать' отправляет CreateApplicationRequest без лишних полей", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [], error: undefined }));
    const fakePost = vi.fn(async () => ({ data: sampleApplication, error: undefined }));

    const wrapper = await mountView(fakeGet, fakePost, vi.fn());
    await flushPromises();

    await wrapper.find("input").setValue("app-2");
    const inputs = wrapper.findAll("input");
    // application_id, display_name, auth.credential_ref, ip_allowlist (text inputs, in DOM order)
    await inputs[1].setValue("App Two");
    await inputs[2].setValue("vault://app-2/key");

    // Select at least one allowed_channels option so the client-side gate opens.
    // findAllComponents by string name ("NSelect") returned 0 matches — this
    // naive-ui version doesn't expose that as the component's `name` option.
    // Matching by the actual imported component definition is reliable
    // regardless of naming internals.
    const selects = wrapper.findAllComponents(NSelect);
    await selects[1].vm.$emit("update:value", ["SMS"]);
    await flushPromises();

    const createButton = wrapper.findAll("button").find((b) => b.text() === "Создать")!;
    expect(createButton.attributes("disabled")).toBeUndefined();
    await createButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePost.mock.calls[0] as unknown as [string, { body: Record<string, unknown> }];
    expect(path).toBe("/applications");
    expect(opts.body.application_id).toBe("app-2");
    expect(opts.body.display_name).toBe("App Two");
    expect((opts.body.auth as { credential_ref: string }).credential_ref).toBe("vault://app-2/key");
    expect(opts.body.allowed_channels).toEqual(["SMS"]);
  });

  it("admin: 'Изменить' открывает модалку с данными строки и PUT не содержит auth", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [sampleApplication], error: undefined }));
    const fakePut = vi.fn(async () => ({ data: sampleApplication, error: undefined }));

    const wrapper = await mountView(fakeGet, vi.fn(), fakePut);
    await flushPromises();

    const editButton = wrapper.findAll("button").find((b) => b.text() === "Изменить")!;
    await editButton.trigger("click");
    await flushPromises();

    // Modal renders via Teleport into document.body. "App One" lives in an
    // <input>'s value property, not in any element's textContent — checking
    // textContent for it always fails regardless of whether the modal
    // actually pre-filled correctly. Check the real input value instead.
    const modalInputValues = Array.from(document.body.querySelectorAll(".n-modal input")).map(
      (el) => (el as HTMLInputElement).value,
    );
    expect(modalInputValues).toContain("App One");

    // Точное сравнение textContent ненадёжно — naive-ui/Vue template
    // рендерит текст кнопки с окружающим пробельным форматированием.
    const saveButton = Array.from(document.body.querySelectorAll("button")).find((b) => b.textContent?.trim() === "Сохранить")!;
    expect(saveButton).toBeTruthy();
    saveButton.click();
    await flushPromises();

    expect(fakePut).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePut.mock.calls[0] as unknown as [string, { params: { path: { application_id: string } }; body: Record<string, unknown> }];
    expect(path).toBe("/applications/{application_id}");
    expect(opts.params.path.application_id).toBe("app-1");
    expect(opts.body).not.toHaveProperty("auth");
    expect(opts.body).not.toHaveProperty("application_id");
    expect(opts.body.display_name).toBe("App One");
  });
});
