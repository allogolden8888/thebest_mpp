import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useAuthStore } from "./auth";

describe("useAuthStore", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("считает пользователя неавторизованным без токена", () => {
    const auth = useAuthStore();
    expect(auth.isAuthenticated).toBe(false);
  });

  it("сохраняет токен в localStorage и помечает как авторизован", () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token-1");

    expect(auth.isAuthenticated).toBe(true);
    expect(localStorage.getItem("backoffice-ui.jwt")).toBe("jwt-token-1");
  });

  it("восстанавливает токен из localStorage при создании нового store-инстанса", () => {
    localStorage.setItem("backoffice-ui.jwt", "persisted-token");
    setActivePinia(createPinia());

    const auth = useAuthStore();
    expect(auth.token).toBe("persisted-token");
    expect(auth.isAuthenticated).toBe(true);
  });

  it("очищает токен из state и localStorage", () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token-2");
    auth.clearToken();

    expect(auth.isAuthenticated).toBe(false);
    expect(localStorage.getItem("backoffice-ui.jwt")).toBeNull();
  });
});