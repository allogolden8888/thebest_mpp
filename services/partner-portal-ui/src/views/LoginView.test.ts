// Same structure as services/backoffice-ui/src/views/LoginView.test.ts —
// real username/password form posting to POST /auth/login (via the
// `partner` client, see api/clients.ts) instead of the old paste-a-JWT
// textarea flow.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import naive from "naive-ui";
import LoginView from "./LoginView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientsKey } from "../api/useApi";

async function mountWithRouter(fakePost: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/login", name: "login", component: LoginView },
      { path: "/applications", name: "applications", component: { template: "<div>applications</div>" } },
    ],
  });
  await router.push("/login");
  await router.isReady();

  const wrapper = mount(LoginView, {
    global: {
      plugins: [router, naive],
      provide: {
        [apiClientsKey as symbol]: {
          partner: { POST: fakePost, GET: vi.fn() },
          billing: { GET: vi.fn() },
        },
      },
    },
  });
  return { wrapper, router };
}

describe("LoginView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("логинится через POST /auth/login и переходит на /applications", async () => {
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: { token: "issued-jwt", expires_at: "2026-01-01T00:00:00Z" }, error: undefined }));
    const { wrapper, router } = await mountWithRouter(fakePost);

    const inputs = wrapper.findAll("input");
    await inputs[0].setValue("acme-bob");
    await inputs[1].setValue("hunter2");
    await wrapper.find("button").trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/auth/login");
    expect((fakePost.mock.calls[0][1] as { body: { username: string; password: string } }).body).toEqual({
      username: "acme-bob",
      password: "hunter2",
    });

    const auth = useAuthStore();
    expect(auth.token).toBe("issued-jwt");
    expect(router.currentRoute.value.name).toBe("applications");
  });

  it("не отправляет форму без логина или пароля", async () => {
    const fakePost = vi.fn();
    const { wrapper } = await mountWithRouter(fakePost);

    await wrapper.find("button").trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
    const auth = useAuthStore();
    expect(auth.token).toBeNull();
  });

  it("показывает ошибку и не сохраняет токен при неверных кредах", async () => {
    const fakePost = vi.fn(async (..._args: unknown[]) => ({ data: undefined, error: "неверный логин или пароль" }));
    const { wrapper } = await mountWithRouter(fakePost);

    const inputs = wrapper.findAll("input");
    await inputs[0].setValue("acme-bob");
    await inputs[1].setValue("wrong");
    await wrapper.find("button").trigger("click");
    await flushPromises();

    const auth = useAuthStore();
    expect(auth.token).toBeNull();
    expect(wrapper.text()).toContain("неверный логин или пароль");
  });
});
