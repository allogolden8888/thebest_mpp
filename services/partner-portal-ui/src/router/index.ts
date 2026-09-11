// Один маршрут на экран — тот же паттерн, что
// services/backoffice-ui/src/router/index.ts.
import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "../stores/auth";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/applications" },
    { path: "/login", name: "login", component: () => import("../views/LoginView.vue"), meta: { public: true } },
    { path: "/applications", name: "applications", component: () => import("../views/ApplicationsView.vue") },
    { path: "/senders", name: "senders", component: () => import("../views/SendersView.vue") },
    { path: "/credentials", name: "credentials", component: () => import("../views/CredentialsView.vue") },
    { path: "/webhook", name: "webhook", component: () => import("../views/WebhookView.vue") },
    { path: "/billing", name: "billing", component: () => import("../views/BillingView.vue") },
    { path: "/templates", name: "templates", component: () => import("../views/TemplatesView.vue") },
    { path: "/chat", name: "chat", component: () => import("../views/ChatView.vue") },
  ],
});

router.beforeEach((to) => {
  if (to.meta.public) return true;
  const auth = useAuthStore();
  if (!auth.isAuthenticated) {
    return { name: "login", query: { redirect: to.fullPath } };
  }
  return true;
});

export default router;
