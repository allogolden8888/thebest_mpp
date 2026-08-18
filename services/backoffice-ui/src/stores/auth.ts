// Pinia store — Keycloak OIDC bearer-токен (services_specifictaion.md §8.5,
// §8.3 "Backoffice API ... Keycloak OIDC"). Backoffice UI сам не общается с
// Keycloak напрямую в этом срезе — токен вводится вручную (см. LoginView) до
// появления полноценного OIDC redirect flow (см. README "Что НЕ реализовано").
//
// CODE_REVIEW.md HIGH finding (#3) — хранение в `localStorage` (не
// HttpOnly-cookie, не in-memory-only) остаётся как есть: это уже честно
// задокументированный компромисс (см. README "Аутентификация" и комментарий
// выше), а не скрытый риск. In-memory-only вариант обсуждался и отклонён —
// он ломает F5/обновление вкладки при отсутствии полноценного OIDC redirect
// flow (пришлось бы заново вставлять JWT вручную при каждой перезагрузке
// страницы), что для инструмента, которым пользуются во время инцидента,
// хуже, чем задокументированный риск XSS-эксфильтрации токена — тем более
// что в кодовой базе по-прежнему нет ни одного XSS sink (см. README/
// CODE_REVIEW.md "Positive findings" — не используем `v-html`/`innerHTML`
// нигде, и это не должно измениться).
import { defineStore } from "pinia";
import { ADMIN_ROLE, decodeRealmRoles } from "./jwtRoles";
import type { ApiClient } from "../api/client";

const STORAGE_KEY = "backoffice-ui.jwt";

export { ADMIN_ROLE };

export const useAuthStore = defineStore("auth", {
  state: () => ({
    token: localStorage.getItem(STORAGE_KEY) as string | null,
    // Phase 0 (iam-service) gap — fine-grained permission strings
    // (`audit:read`, `iam:manage`, ...) live server-side in Postgres
    // (iam.role_permissions) and are NOT a JWT claim, unlike `roles` below
    // (which stays a pure client-side decode of `realm_access.roles`). This
    // is populated by `fetchMe()` from `GET /v1/me` — until that resolves
    // (or if it fails), `permissions` stays `[]` and `hasPermission()`
    // correctly denies everything (fail-closed), same as `meLoaded: false`
    // signals "not fetched yet" to callers that care (e.g. to distinguish
    // "definitely no" from "don't know yet").
    permissions: [] as string[],
    meLoaded: false,
  }),
  getters: {
    isAuthenticated: (state) => state.token !== null && state.token !== "",
    // CODE_REVIEW.md CRITICAL finding (#1) — realm-роли из JWT, для
    // client-side role-gating меню/форм. Не проверяет подпись токена (см.
    // jwtRoles.ts doc) — сервер (backoffice-api) остаётся единственной
    // реальной линией защиты, это только UX-слой.
    roles: (state) => decodeRealmRoles(state.token),
  },
  actions: {
    setToken(token: string) {
      this.token = token;
      localStorage.setItem(STORAGE_KEY, token);
    },
    clearToken() {
      this.token = null;
      localStorage.removeItem(STORAGE_KEY);
      // Reset the fetched-permission state too — otherwise a stale
      // permission set from the previous session would survive a
      // logout/login-as-someone-else cycle within the same page load.
      this.permissions = [];
      this.meLoaded = false;
    },
    hasRole(role: string): boolean {
      return this.roles.includes(role);
    },
    isAdmin(): boolean {
      return this.hasRole(ADMIN_ROLE);
    },
    // Fetches GET /v1/me and populates `permissions`. Takes the ApiClient as
    // a parameter rather than calling `useApi()` internally: this is a
    // Pinia options-store (state/getters/actions object, not a setup
    // store), so its actions run outside any Vue component's active
    // injection context — `inject()` (which `useApi()` relies on) would
    // throw if called from in here. Callers (App.vue) already have the
    // client via `useApi()` in their own setup scope and pass it in — same
    // dependency-injection shape as everywhere else in this codebase, just
    // pushed one level up because a store can't `inject()` for itself.
    async fetchMe(api: ApiClient): Promise<void> {
      if (!this.token) return;
      const { data, error } = await api.GET("/me");
      if (error) {
        // Leave permissions as-is (fail-closed) and meLoaded false so a
        // transient failure (network blip, backend briefly down) can be
        // retried by the caller rather than being permanently treated as
        // "user has zero permissions".
        return;
      }
      this.permissions = data?.permissions ?? [];
      this.meLoaded = true;
    },
    hasPermission(permission: string): boolean {
      return this.permissions.includes(permission);
    },
  },
});
