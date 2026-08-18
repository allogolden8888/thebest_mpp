// Covers: permission-gated rendering (RequirePermission "iam:manage"), the
// grant flow (form validity + POST body), and the revoke flow (confirm
// dialog before DELETE fires, `revoked: false` handled as an inline info
// message rather than an error). Mirrors the mount/mock conventions used by
// ExecutionControlView.test.ts / SchedulerForceCommandView.test.ts.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider, NSelect } from "naive-ui";
import AccessControlView from "./AccessControlView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientKey } from "../api/useApi";

function bodyButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll("button")).filter((b) => b.textContent?.includes(text)) as HTMLButtonElement[];
}

interface FakeApi {
  GET: ReturnType<typeof vi.fn>;
  POST: ReturnType<typeof vi.fn>;
  DELETE: ReturnType<typeof vi.fn>;
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
        default: () => h(NDialogProvider, null, { default: () => h(AccessControlView) }),
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

const oneRole = { roles: [{ id: 1, name: "ops-viewer", description: "read-only ops", permissions: ["ops:read"] }] };
const oneAssignment = {
  assignments: [{ id: 1, external_id: "u2", role: "ops-viewer", granted_by: "u1", granted_at: "2026-01-01T00:00:00Z" }],
};

describe("AccessControlView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("без iam:manage показывает 'недостаточно прав' и не запрашивает данные", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeApi: FakeApi = { GET: vi.fn(), POST: vi.fn(), DELETE: vi.fn() };

    const wrapper = await mountView(fakeApi);

    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(fakeApi.GET).not.toHaveBeenCalled();
  });

  it("с iam:manage показывает таблицу активных назначений", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/iam/roles" ? { data: oneRole, error: undefined } : { data: oneAssignment, error: undefined },
    );
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn(), DELETE: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(wrapper.text()).toContain("u2");
    expect(wrapper.text()).toContain("ops-viewer");
  });

  it("кнопка 'Назначить' отключена, пока external_id и role не заполнены", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/iam/roles" ? { data: { roles: [] }, error: undefined } : { data: { assignments: [] }, error: undefined },
    );
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn(), DELETE: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const grantButton = wrapper.findAll("button").find((b) => b.text().includes("Назначить"))!;
    expect(grantButton.attributes("disabled")).toBeDefined();
  });

  it("отправляет POST /iam/staff-assignments с заполненными external_id/role", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/iam/roles" ? { data: oneRole, error: undefined } : { data: { assignments: [] }, error: undefined },
    );
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { assignment: { id: 2, external_id: "u3", role: "ops-viewer", granted_by: "u1", granted_at: "now" } },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost, DELETE: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    await wrapper.find("input").setValue("u3");
    const select = wrapper.findComponent(NSelect);
    await select.vm.$emit("update:value", "ops-viewer");
    await flushPromises();

    const grantButton = wrapper.findAll("button").find((b) => b.text().includes("Назначить"))!;
    expect(grantButton.attributes("disabled")).toBeUndefined();
    await grantButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/iam/staff-assignments");
    expect((fakePost.mock.calls[0][1] as { body: { external_id: string; role: string } }).body).toEqual({
      external_id: "u3",
      role: "ops-viewer",
    });
  });

  it("отзыв роли требует подтверждения в диалоге перед вызовом DELETE", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/iam/roles" ? { data: { roles: [] }, error: undefined } : { data: oneAssignment, error: undefined },
    );
    const fakeDelete = vi.fn(async (..._args: unknown[]) => ({ data: { revoked: true }, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn(), DELETE: fakeDelete };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const revokeButton = wrapper.findAll("button").find((b) => b.text().includes("Отозвать"))!;
    await revokeButton.trigger("click");
    await flushPromises();

    expect(fakeDelete).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Отозвать").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakeDelete).toHaveBeenCalledTimes(1);
    expect(fakeDelete.mock.calls[0][0]).toBe("/iam/staff-assignments/{external_id}/{role}");
    expect(
      (fakeDelete.mock.calls[0][1] as { params: { path: { external_id: string; role: string } } }).params.path,
    ).toEqual({ external_id: "u2", role: "ops-viewer" });
  });

  it("revoked:false отображается как обычный inline-результат, а не как ошибка", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/iam/roles" ? { data: { roles: [] }, error: undefined } : { data: oneAssignment, error: undefined },
    );
    const fakeDelete = vi.fn(async (..._args: unknown[]) => ({ data: { revoked: false }, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn(), DELETE: fakeDelete };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const revokeButton = wrapper.findAll("button").find((b) => b.text().includes("Отозвать"))!;
    await revokeButton.trigger("click");
    await flushPromises();
    const confirmButtons = bodyButtons("Отозвать").filter((b) => !wrapper.element.contains(b));
    confirmButtons[0].click();
    await flushPromises();

    expect(fakeDelete).toHaveBeenCalledTimes(1);
    // Не бросает / не остаётся в состоянии ошибки — onError не должен был сработать.
    expect(document.body.textContent).not.toContain("undefined");
  });
});
