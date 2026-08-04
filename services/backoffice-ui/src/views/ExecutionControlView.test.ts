// CODE_REVIEW.md findings covered by this test (subagent-1 / backoffice-ui):
// #1 — role-gating (non-admin sees "insufficient permissions", not the form
//      that can pause the whole platform).
// #2 — confirmation dialog required before Apply/Clear Override fire, and
//      empty `reason` is rejected client-side before any request is sent.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import ExecutionControlView from "./ExecutionControlView.vue";
import { useAuthStore, ADMIN_ROLE } from "../stores/auth";
import { apiClientKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

// naive-ui's dialog/message overlays render via <Teleport> (default target:
// document.body) -- they exist in the real DOM but outside `wrapper`'s own
// element subtree, so assertions below query `document.body` directly.
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
        default: () => h(NDialogProvider, null, { default: () => h(ExecutionControlView) }),
      }),
  });

  const wrapper = mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: {
        [apiClientKey as symbol]: { POST: fakePost, GET: vi.fn() },
      },
    },
  });
  return wrapper;
}

describe("ExecutionControlView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("не-admin видит 'недостаточно прав' вместо формы override", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["support"] } }));
    const fakePost = vi.fn();

    const wrapper = await mountView(fakePost);

    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(wrapper.find("button").exists() ? wrapper.text().includes("Apply Override") : true).toBe(false);
  });

  it("admin: клик 'Apply Override' с пустым reason не отправляет запрос", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: { version: 1 }, error: undefined }));

    const wrapper = await mountView(fakePost);
    const applyButton = wrapper.findAll("button").find((b) => b.text().includes("Apply Override"))!;
    await applyButton.trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
  });

  it("admin: Apply Override требует подтверждения в диалоге перед отправкой запроса", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: { version: 7 }, error: undefined }));

    const wrapper = await mountView(fakePost);

    const reasonTextarea = wrapper.find("textarea");
    await reasonTextarea.setValue("плановое обслуживание — инцидент INC-123");

    const applyButton = wrapper.findAll("button").find((b) => b.text().includes("Apply Override"))!;
    await applyButton.trigger("click");
    await flushPromises();

    // Диалог показан, запрос ещё не ушёл.
    expect(fakePost).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Apply Override").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/execution-control/override");
    expect((fakePost.mock.calls[0][1] as { body: { reason: string } }).body.reason).toContain("INC-123");
  });

  it("admin: Clear Override требует подтверждения в диалоге перед отправкой запроса", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: { version: 8 }, error: undefined }));

    const wrapper = await mountView(fakePost);

    const clearButton = wrapper.findAll("button").find((b) => b.text().includes("Clear Override"))!;
    await clearButton.trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Clear Override").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/execution-control/override/clear");
  });
});
