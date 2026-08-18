import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { useAuthStore, ADMIN_ROLE } from "./auth";
import { makeTestJwt } from "../test-utils/jwt";
import type { ApiClient } from "../api/client";

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

  // Phase 0 (iam-service) gap — `permissions`/`hasPermission`/`fetchMe` are
  // the server-side (GET /v1/me) counterpart to the JWT-decoded `roles`/
  // `hasRole` above: fine-grained permission strings (e.g. `iam:manage`)
  // live in Postgres (iam.role_permissions), not in any JWT claim.
  describe("hasPermission / fetchMe — GET /v1/me permission fetch (Phase 0 iam-service)", () => {
    it("hasPermission возвращает false без предварительного fetchMe", () => {
      const auth = useAuthStore();
      auth.setToken("jwt-token");

      expect(auth.hasPermission("iam:manage")).toBe(false);
      expect(auth.meLoaded).toBe(false);
    });

    it("fetchMe заполняет permissions и meLoaded из ответа GET /v1/me", async () => {
      const auth = useAuthStore();
      auth.setToken("jwt-token");
      const fakeGet = vi.fn(async () => ({
        data: { external_id: "u1", roles: ["ops-viewer"], permissions: ["ops:read", "audit:read"] },
        error: undefined,
      }));
      const fakeApi = { GET: fakeGet } as unknown as ApiClient;

      await auth.fetchMe(fakeApi);

      expect(fakeGet).toHaveBeenCalledWith("/me");
      expect(auth.meLoaded).toBe(true);
      expect(auth.hasPermission("audit:read")).toBe(true);
      expect(auth.hasPermission("iam:manage")).toBe(false);
    });

    it("fetchMe не вызывает GET, если токена нет", async () => {
      const auth = useAuthStore();
      const fakeGet = vi.fn();
      const fakeApi = { GET: fakeGet } as unknown as ApiClient;

      await auth.fetchMe(fakeApi);

      expect(fakeGet).not.toHaveBeenCalled();
      expect(auth.meLoaded).toBe(false);
    });

    it("fetchMe оставляет permissions пустым и meLoaded=false при ошибке ответа (fail-closed, можно повторить)", async () => {
      const auth = useAuthStore();
      auth.setToken("jwt-token");
      const fakeGet = vi.fn(async () => ({ data: undefined, error: "недоступно" }));
      const fakeApi = { GET: fakeGet } as unknown as ApiClient;

      await auth.fetchMe(fakeApi);

      expect(auth.meLoaded).toBe(false);
      expect(auth.permissions).toEqual([]);
      expect(auth.hasPermission("audit:read")).toBe(false);
    });

    it("clearToken сбрасывает permissions и meLoaded", async () => {
      const auth = useAuthStore();
      auth.setToken("jwt-token");
      const fakeApi = {
        GET: vi.fn(async () => ({ data: { external_id: "u1", roles: [], permissions: ["iam:manage"] }, error: undefined })),
      } as unknown as ApiClient;
      await auth.fetchMe(fakeApi);
      expect(auth.hasPermission("iam:manage")).toBe(true);

      auth.clearToken();

      expect(auth.meLoaded).toBe(false);
      expect(auth.permissions).toEqual([]);
      expect(auth.hasPermission("iam:manage")).toBe(false);
    });
  });
});