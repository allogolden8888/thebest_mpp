import { describe, expect, it, beforeEach } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import naive from "naive-ui";
import LoginView from "./LoginView.vue";
import { useAuthStore } from "../stores/auth";

async function mountWithRouter() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/login", name: "login", component: LoginView },
      { path: "/config", name: "config", component: { template: "<div>config</div>" } },
    ],
  });
  await router.push("/login");
  await router.isReady();

  const wrapper = mount(LoginView, {
    global: { plugins: [router, naive] },
  });
  return { wrapper, router };
}

describe("LoginView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("сохраняет введённый токен и переходит на /config при отправке", async () => {
    const { wrapper, router } = await mountWithRouter();

    const textarea = wrapper.find("textarea");
    await textarea.setValue("my-jwt-token");
    await wrapper.find("button").trigger("click");
    await flushPromises();

    const auth = useAuthStore();
    expect(auth.token).toBe("my-jwt-token");
    expect(router.currentRoute.value.name).toBe("config");
  });

  it("не отправляет форму с пустым токеном", async () => {
    const { wrapper } = await mountWithRouter();

    await wrapper.find("button").trigger("click");

    const auth = useAuthStore();
    expect(auth.token).toBeNull();
  });
});