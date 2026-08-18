import { describe, expect, it, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import naive from "naive-ui";
import App from "./App.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "./stores/auth";
import { makeTestJwt } from "./test-utils/jwt";
import { apiClientsKey } from "./api/useApi";

const stubApiClient = { GET: () => Promise.resolve({ data: undefined, error: undefined }) };

async function mountApp() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", redirect: "/applications" },
      { path: "/applications", name: "applications", component: { template: "<div/>" } },
      { path: "/billing", name: "billing", component: { template: "<div/>" } },
    ],
  });
  await router.push("/applications");
  await router.isReady();

  return mount(App, {
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientsKey as symbol]: { partner: stubApiClient, billing: stubApiClient } },
    },
  });
}

describe("App.vue", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("показывает форму входа без токена", async () => {
    const wrapper = await mountApp();
    await flushPromises();

    expect(wrapper.findComponent({ name: "NLayoutSider" }).exists()).toBe(false);
  });

  it("показывает partner_id и роль viewer для partner-viewer токена", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "click_uz", realm_access: { roles: ["partner-viewer"] } }));

    const wrapper = await mountApp();
    await flushPromises();

    expect(wrapper.text()).toContain("click_uz");
    expect(wrapper.text()).toContain("viewer");
    expect(wrapper.text()).not.toContain(PARTNER_ADMIN_ROLE);
  });

  it("показывает роль partner-admin для admin-токена", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "click_uz", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));

    const wrapper = await mountApp();
    await flushPromises();

    expect(wrapper.text()).toContain(PARTNER_ADMIN_ROLE);
  });
});
