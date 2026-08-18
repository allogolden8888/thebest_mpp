// Mirrors RequireAdmin.test.ts — regression coverage for the defense-in-depth
// gate on direct-URL access to a permission-gated view, but for the
// server-side (GET /v1/me) permission set instead of the JWT-decoded role.
import { describe, expect, it, beforeEach } from "vitest";
import { mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import naive from "naive-ui";
import RequirePermission from "./RequirePermission.vue";
import { useAuthStore } from "../stores/auth";

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

  return mount(RequirePermission, {
    props: { permission: "iam:manage" },
    slots: { default: "<div data-testid='protected-content'>secret controls</div>" },
    global: { plugins: [router, naive] },
  });
}

describe("RequirePermission", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("показывает 'недостаточно прав' пользователю без нужного permission", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    // meLoaded stays false / permissions stays [] — same as "fetch hasn't
    // resolved yet" and "user genuinely lacks it": both fail-closed.

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(false);
    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(wrapper.text()).toContain("iam:manage");
  });

  it("показывает защищённый контент пользователю с нужным permission", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["iam:manage"];
    auth.meLoaded = true;

    const wrapper = await mountWithRouter();

    expect(wrapper.find("[data-testid='protected-content']").exists()).toBe(true);
    expect(wrapper.text()).not.toContain("Недостаточно прав");
  });
});
