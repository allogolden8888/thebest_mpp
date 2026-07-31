import { createApp } from "vue";
import { createPinia } from "pinia";
import { VueQueryPlugin } from "@tanstack/vue-query";
import naive from "naive-ui";

import "./style.css";
import App from "./App.vue";
import router from "./router";
import { createApiClient } from "./api/client";
import { apiClientKey } from "./api/useApi";

const app = createApp(App);

app.use(createPinia());
app.use(router);
app.use(VueQueryPlugin);
app.use(naive);

app.provide(
  apiClientKey,
  createApiClient(import.meta.env.VITE_BACKOFFICE_API_URL ?? "/v1", {
    // CODE_REVIEW.md HIGH finding #4 — 401 (истёкший/невалидный токен) ->
    // редирект на /login с понятным сигналом, вместо тихого вечного отказа.
    onUnauthorized: () => {
      if (router.currentRoute.value.name !== "login") {
        router.push({ name: "login", query: { redirect: router.currentRoute.value.fullPath, sessionExpired: "1" } });
      }
    },
  }),
);

app.mount("#app");