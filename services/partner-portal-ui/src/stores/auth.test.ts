import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "./auth";
import { makeTestJwt } from "../test-utils/jwt";

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
    expect(localStorage.getItem("partner-portal-ui.jwt")).toBe("jwt-token-1");
  });

  it("восстанавливает токен из localStorage при создании нового store-инстанса", () => {
    localStorage.setItem("partner-portal-ui.jwt", "persisted-token");
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
    expect(localStorage.getItem("partner-portal-ui.jwt")).toBeNull();
  });

  describe("hasRole / isAdmin", () => {
    it("не считает пользователя admin'ом без токена", () => {
      const auth = useAuthStore();
      expect(auth.isAdmin()).toBe(false);
      expect(auth.hasRole(PARTNER_ADMIN_ROLE)).toBe(false);
    });

    it("не считает пользователя admin'ом с токеном без нужной роли", () => {
      const auth = useAuthStore();
      auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));

      expect(auth.isAdmin()).toBe(false);
      expect(auth.roles).toEqual(["partner-viewer"]);
    });

    it("считает пользователя admin'ом при наличии роли partner-admin", () => {
      const auth = useAuthStore();
      auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));

      expect(auth.isAdmin()).toBe(true);
      expect(auth.hasRole(PARTNER_ADMIN_ROLE)).toBe(true);
    });

    it("не роняет store на токене без realm_access (не JWT / старый формат)", () => {
      const auth = useAuthStore();
      auth.setToken("not-a-real-jwt");

      expect(auth.isAuthenticated).toBe(true);
      expect(auth.isAdmin()).toBe(false);
      expect(auth.roles).toEqual([]);
    });
  });

  describe("partnerId", () => {
    it("извлекает partner_id из claim'а токена", () => {
      const auth = useAuthStore();
      auth.setToken(makeTestJwt({ sub: "u1", partner_id: "click_uz" }));
      expect(auth.partnerId).toBe("click_uz");
    });

    it("возвращает '' без токена", () => {
      const auth = useAuthStore();
      expect(auth.partnerId).toBe("");
    });
  });
});
