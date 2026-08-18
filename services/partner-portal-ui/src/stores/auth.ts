// Pinia store — Keycloak OIDC bearer-токен, тот же паттерн, что
// services/backoffice-ui/src/stores/auth.ts (см. doc-комментарий там за
// полным обоснованием ручного ввода токена вместо OIDC redirect flow и
// localStorage-компромисса).
import { defineStore } from "pinia";
import { PARTNER_ADMIN_ROLE, decodeRealmRoles, decodePartnerID } from "./jwtRoles";

const STORAGE_KEY = "partner-portal-ui.jwt";

export { PARTNER_ADMIN_ROLE };

export const useAuthStore = defineStore("auth", {
  state: () => ({
    token: localStorage.getItem(STORAGE_KEY) as string | null,
  }),
  getters: {
    isAuthenticated: (state) => state.token !== null && state.token !== "",
    roles: (state) => decodeRealmRoles(state.token),
    partnerId: (state) => decodePartnerID(state.token),
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
      return this.hasRole(PARTNER_ADMIN_ROLE);
    },
  },
});
