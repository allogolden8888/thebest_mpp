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

const STORAGE_KEY = "backoffice-ui.jwt";

export { ADMIN_ROLE };

export const useAuthStore = defineStore("auth", {
  state: () => ({
    token: localStorage.getItem(STORAGE_KEY) as string | null,
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
    },
    hasRole(role: string): boolean {
      return this.roles.includes(role);
    },
    isAdmin(): boolean {
      return this.hasRole(ADMIN_ROLE);
    },
  },
});
