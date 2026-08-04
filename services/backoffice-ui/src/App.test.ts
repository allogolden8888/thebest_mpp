// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1a) — the
// menu used to be a static array shown identically to every authenticated
// user, with no branch hiding Execution Control/Force Scheduler Command by
// role. This test covers the fix directly.
import { describe, expect, it, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import naive from "naive-ui";
import App from "./App.vue";
import { useAuthStore, ADMIN_ROLE } from "./stores/auth";
import { makeTestJwt } from "./test-utils/jwt";
import { apiClientKey } from "./api/useApi";

async function mountApp() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", redirect: "/config" },
      { path: "/config", name: "config", component: { template: "<div/>" } },
      { path: "/execution-control", name: "execution-control", component: { template: "<div/>" } },
      { path: "/scheduler", name: "scheduler", component: { template: "<div/>" } },
      { path: "/dlq", name: "dlq", component: { template: "<div/>" } },
    ],
  });
  await router.push("/config");
  await router.isReady();

  return mount(App, {
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientKey as symbol]: { GET: () => Promise.resolve({ data: undefined, error: undefined }) } },
    },
  });
}

describe("App.vue — role-gated menu", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("не-admin не видит пункты Execution Control / Force Scheduler Command", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["support"] } }));

    const wrapper = await mountApp();
    await flushPromises();

    expect(wrapper.text()).toContain("Configuration");
    expect(wrapper.text()).not.toContain("Execution Control");
    expect(wrapper.text()).not.toContain("Force Scheduler Command");
    expect(wrapper.text()).toContain("read-only");
  });

  it("admin видит все пункты меню, включая деструктивные", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));

    const wrapper = await mountApp();
    await flushPromises();

    expect(wrapper.text()).toContain("Execution Control");
    expect(wrapper.text()).toContain("Force Scheduler Command");
    expect(wrapper.text()).toContain(ADMIN_ROLE);
  });
});
