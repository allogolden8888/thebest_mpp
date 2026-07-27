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

app.provide(apiClientKey, createApiClient(import.meta.env.VITE_BACKOFFICE_API_URL ?? "/v1"));

app.mount("#app");