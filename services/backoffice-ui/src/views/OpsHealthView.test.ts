// Covers: permission-gated rendering (RequirePermission "ops:read"),
// rendering of the Kafka lag group summary + flattened partition table,
// the readyz grid, and the `kafka_lag_available`/`readyz_available` false
// case showing a warning instead of an empty table. Mirrors the mount/mock
// conventions used by AccessControlView.test.ts/AuditLogView.test.ts.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import OpsHealthView from "./OpsHealthView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientKey } from "../api/useApi";

interface FakeApi {
  GET: ReturnType<typeof vi.fn>;
}

async function mountView(fakeApi: FakeApi) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(OpsHealthView) }),
      }),
  });

  return mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientKey as symbol]: fakeApi },
    },
  });
}

const fullSnapshot = {
  kafka_lag: {
    generated_at: "2026-01-01T00:00:00Z",
    bootstrap_servers: ["kafka:9092"],
    groups: [
      {
        group: "billing-service",
        state: "Stable",
        total_lag: 42,
        partitions: [{ topic: "message.events", partition: 0, commit_offset: 100, end_offset: 142, lag: 42 }],
      },
    ],
  },
  kafka_lag_available: true,
  readyz: {
    generated_at: "2026-01-01T00:00:00Z",
    services: [{ service: "iam-service", ready: true, http_status: 200, latency_ms: 12 }],
  },
  readyz_available: true,
};

describe("OpsHealthView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("без ops:read показывает 'недостаточно прав' и не запрашивает данные", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeApi: FakeApi = { GET: vi.fn() };

    const wrapper = await mountView(fakeApi);

    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(fakeApi.GET).not.toHaveBeenCalled();
  });

  it("с ops:read показывает таблицы Kafka lag и readyz", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["ops:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({ data: fullSnapshot, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledWith("/ops/snapshot");
    expect(wrapper.text()).toContain("billing-service");
    expect(wrapper.text()).toContain("message.events");
    expect(wrapper.text()).toContain("iam-service");
    expect(wrapper.text()).toContain("ready");
  });

  it("kafka_lag_available:false показывает предупреждение вместо пустой таблицы", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["ops:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({
      data: { kafka_lag: null, kafka_lag_available: false, readyz: fullSnapshot.readyz, readyz_available: true },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(wrapper.text()).toContain("Снапшот lag недоступен");
  });

  it("кнопка 'Обновить' вызывает повторный запрос", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["ops:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({ data: fullSnapshot, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet };

    const wrapper = await mountView(fakeApi);
    await flushPromises();
    expect(fakeGet).toHaveBeenCalledTimes(1);

    const refreshButton = wrapper.findAll("button").find((b) => b.text().includes("Обновить"))!;
    await refreshButton.trigger("click");
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledTimes(2);
  });
});
