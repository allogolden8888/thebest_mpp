import { beforeEach, describe, expect, it } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useAuthStore, ADMIN_ROLE } from "./auth";
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

  // CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1) —
  // client-side role-gating: этот блок покрывает регрессией именно то, что
  // раньше не проверялось нигде ("role" не встречался в src/ вообще).
  describe("hasRole / isAdmin — CODE_REVIEW.md #1 client-side RBAC mirror", () => {
    it("не считает пользователя admin'ом без токена", () => {
      const auth = useAuthStore();
      expect(auth.isAdmin()).toBe(false);
      expect(auth.hasRole(ADMIN_ROLE)).toBe(false);
    });

    it("не считает пользователя admin'ом с токеном без нужной роли", () => {
      const auth = useAuthStore();
      auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: ["support"] } }));

      expect(auth.isAdmin()).toBe(false);
      expect(auth.roles).toEqual(["support"]);
    });

    it("считает пользователя admin'ом при наличии роли backoffice-admin", () => {
      const auth = useAuthStore();
      auth.setToken(makeTestJwt({ sub: "u1", realm_access: { roles: [ADMIN_ROLE] } }));

      expect(auth.isAdmin()).toBe(true);
      expect(auth.hasRole(ADMIN_ROLE)).toBe(true);
    });

    it("не роняет store на токене без realm_access (не JWT / старый формат)", () => {
      const auth = useAuthStore();
      auth.setToken("not-a-real-jwt");

      expect(auth.isAuthenticated).toBe(true);
      expect(auth.isAdmin()).toBe(false);
      expect(auth.roles).toEqual([]);
    });
  });
});