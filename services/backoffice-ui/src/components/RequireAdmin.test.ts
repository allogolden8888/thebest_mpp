// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1b) —
// regression coverage for the defense-in-depth gate on direct-URL access to
// a destructive view by a non-admin.
import { describe, expect, it, beforeEach } from "vitest";
import { mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import naive from "naive-ui";
import RequireAdmin from "./RequireAdmin.vue";
import { useAuthStore, ADMIN_ROLE } from "../stores/auth";
import { makeTestJwt } from "../test-utils/jwt";

async function mountWithRouter() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/", name: "root", component: { template: "<div/>" } },
      { path: "/config", name: "config", component: { template: "<div/>" } },
    ],
  });
  await router.push("/");
  await router.isReady();

  return mount(RequireAdmin, {
    slots: { default: "<div data-testid='protected-content'>secret controls</div>" },
    global: { plugins: [router, naive] },
  });
}

describe("RequireAdmin", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("показывает 'недостаточно прав' для аутентифицированного, но не-admin пользователя", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["support"] } }));

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(false);
    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(wrapper.text()).toContain(ADMIN_ROLE);
  });

  it("показывает защищённый контент для пользователя с ролью backoffice-admin", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(true);
    expect(wrapper.text()).not.toContain("Недостаточно прав");
  });
});
