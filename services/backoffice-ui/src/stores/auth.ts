// Pinia store — Keycloak OIDC bearer-токен (services_specifictaion.md §8.5,
// §8.3 "Backoffice API ... Keycloak OIDC"). Backoffice UI сам не общается с
// Keycloak напрямую в этом срезе — токен вводится вручную (см. LoginView) до
// появления полноценного OIDC redirect flow (см. README "Что НЕ реализовано").
import { defineStore } from "pinia";

const STORAGE_KEY = "backoffice-ui.jwt";

export const useAuthStore = defineStore("auth", {
  state: () => ({
    token: localStorage.getItem(STORAGE_KEY) as string | null,
  }),
  getters: {
    isAuthenticated: (state) => state.token !== null && state.token !== "",
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
  },
});
