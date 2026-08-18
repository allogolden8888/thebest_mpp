import { describe, expect, it, beforeEach } from "vitest";
import { mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import naive from "naive-ui";
import RequirePartnerAdmin from "./RequirePartnerAdmin.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";
import { makeTestJwt } from "../test-utils/jwt";

async function mountWithRouter() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", name: "root", component: { template: "<div/>" } },
      { path: "/applications", name: "applications", component: { template: "<div/>" } },
    ],
  });
  await router.push("/");
  await router.isReady();

  return mount(RequirePartnerAdmin, {
    slots: { default: "<div data-testid='protected-content'>secret controls</div>" },
    global: { plugins: [router, naive] },
  });
}

describe("RequirePartnerAdmin", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("показывает 'недостаточно прав' для аутентифицированного, но не-admin пользователя", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(false);
    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(wrapper.text()).toContain(PARTNER_ADMIN_ROLE);
  });

  it("показывает защищённый контент для пользователя с ролью partner-admin", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(true);
    expect(wrapper.text()).not.toContain("Недостаточно прав");
  });
});
