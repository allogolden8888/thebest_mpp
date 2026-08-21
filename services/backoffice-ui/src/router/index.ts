// Один маршрут на метод service_internal_methods.md §7.3 — UI сам не имеет
// собственных методов обработки данных (§7.5), каждая страница — тонкая
// обёртка вокруг одного HTTP-вызова Backoffice API.
import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "../stores/auth";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/config" },
    { path: "/login", name: "login", component: () => import("../views/LoginView.vue"), meta: { public: true } },
    { path: "/config", name: "config", component: () => import("../views/ConfigView.vue") },
    { path: "/execution-control", name: "execution-control", component: () => import("../views/ExecutionControlView.vue") },
    { path: "/scheduler", name: "scheduler", component: () => import("../views/SchedulerForceCommandView.vue") },
    { path: "/messages", name: "messages", component: () => import("../views/MessagesView.vue") },
    { path: "/dlq", name: "dlq", component: () => import("../views/DlqBrowseView.vue") },
    { path: "/reconciliation", name: "reconciliation", component: () => import("../views/ReconciliationView.vue") },
    { path: "/billing", name: "billing", component: () => import("../views/BillingView.vue") },
    { path: "/reports", name: "reports", component: () => import("../views/ReportsView.vue") },
    { path: "/access-control", name: "access-control", component: () => import("../views/AccessControlView.vue") },
    { path: "/audit", name: "audit", component: () => import("../views/AuditLogView.vue") },
    { path: "/incidents", name: "incidents", component: () => import("../views/IncidentsView.vue") },
    { path: "/ops-health", name: "ops-health", component: () => import("../views/OpsHealthView.vue") },
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
