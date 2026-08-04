// CODE_REVIEW.md findings covered by this test (subagent-1 / backoffice-ui):
// #1 — role-gating. #2 — confirmation dialog + non-empty reason required
// before the force-command request fires.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import SchedulerForceCommandView from "./SchedulerForceCommandView.vue";
import { useAuthStore, ADMIN_ROLE } from "../stores/auth";
import { apiClientKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

function bodyButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll("button")).filter((b) => b.textContent?.includes(text)) as HTMLButtonElement[];
}

async function mountView(fakePost: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(SchedulerForceCommandView) }),
      }),
  });

  return mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientKey as symbol]: { POST: fakePost, GET: vi.fn() } },
    },
  });
}

describe("SchedulerForceCommandView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("не-admin видит 'недостаточно прав' вместо формы", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["support"] } }));
    const wrapper = await mountView(vi.fn());

    expect(wrapper.text()).toContain("Недостаточно прав");
  });

  it("admin: пустой reason или stage_execution_id блокирует отправку без запроса", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));
    const fakePost = vi.fn();
    const wrapper = await mountView(fakePost);

    await wrapper.find("button").trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
  });

  it("admin: требует подтверждения в диалоге перед отправкой команды", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: {}, error: undefined }));
    const wrapper = await mountView(fakePost);

    const inputs = wrapper.findAll("input");
    await inputs[0].setValue("stage-exec-123");
    await wrapper.find("textarea").setValue("зависший stage, инцидент INC-9");

    await wrapper.find("button").trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Отправить").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/scheduler/force-command");
    expect((fakePost.mock.calls[0][1] as { body: { stage_execution_id: string } }).body.stage_execution_id).toBe(
      "stage-exec-123",
    );
  });
});
