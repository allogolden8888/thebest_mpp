import { createApp } from "vue";
import { createPinia } from "pinia";
import { VueQueryPlugin } from "@tanstack/vue-query";
import naive from "naive-ui";

import "./style.css";
import App from "./App.vue";
import router from "./router";
import { createApiClients } from "./api/clients";
import { apiClientsKey } from "./api/useApi";

const app = createApp(App);

app.use(createPinia());
app.use(router);
app.use(VueQueryPlugin);
app.use(naive);

app.provide(
  apiClientsKey,
  createApiClients(
    import.meta.env.VITE_PARTNER_SELF_SERVICE_API_URL ?? "/partner-api/v1/self-service",
    import.meta.env.VITE_BILLING_SELF_SERVICE_API_URL ?? "/billing-api/v1/self-service",
    {
      onUnauthorized: () => {
        if (router.currentRoute.value.name !== "login") {
          router.push({ name: "login", query: { redirect: router.currentRoute.value.fullPath, sessionExpired: "1" } });
        }
      },
    },
  ),
);

app.mount("#app");
