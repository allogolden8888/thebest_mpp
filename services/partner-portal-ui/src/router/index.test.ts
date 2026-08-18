import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useAuthStore } from "../stores/auth";
import router from "./index";

describe("router guard", () => {
  beforeEach(async () => {
    localStorage.clear();
    setActivePinia(createPinia());
    await router.push("/");
    await router.isReady();
  });

  it("перенаправляет на /login при попытке зайти на защищённый маршрут без токена", async () => {
    await router.push("/billing");
    expect(router.currentRoute.value.name).toBe("login");
    expect(router.currentRoute.value.query.redirect).toBe("/billing");
  });

  it("пропускает на /login без редиректа (публичный маршрут)", async () => {
    await router.push("/login");
    expect(router.currentRoute.value.name).toBe("login");
  });

  it("пропускает на защищённый маршрут при наличии токена", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");

    await router.push("/billing");
    expect(router.currentRoute.value.name).toBe("billing");
  });
});
