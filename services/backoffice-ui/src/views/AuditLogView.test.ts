// Covers: permission-gated rendering (RequirePermission "audit:read"), the
// source filter (resets accumulated pages), and "load more" pagination
// (accumulates entries across pages, stops once next_offset is null).
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider, NSelect } from "naive-ui";
import AuditLogView from "./AuditLogView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientKey } from "../api/useApi";

async function mountView(fakeGet: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(AuditLogView) }),
      }),
  });

  return mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientKey as symbol]: { GET: fakeGet, POST: vi.fn(), DELETE: vi.fn() } },
    },
  });
}

describe("AuditLogView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("без audit:read показывает 'недостаточно прав' и не запрашивает данные", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeGet = vi.fn();

    const wrapper = await mountView(fakeGet);

    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(fakeGet).not.toHaveBeenCalled();
  });

  it("с audit:read загружает и показывает первую страницу записей", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["audit:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (..._args: unknown[]) => ({
      data: {
        entries: [{ source: "replay", actor: "u1", action: "requested", target: "stage-exec-1", created_at: "2026-01-01T00:00:00Z" }],
        next_offset: 50,
      },
      error: undefined,
    }));

    const wrapper = await mountView(fakeGet);
    await flushPromises();

    expect(wrapper.text()).toContain("stage-exec-1");
    expect(wrapper.text()).toContain("replay");
    expect(fakeGet.mock.calls[0][0]).toBe("/audit");
    expect((fakeGet.mock.calls[0][1] as { params: { query: { offset: number } } }).params.query.offset).toBe(0);
  });

  it("'Загрузить ещё' запрашивает следующую страницу и накапливает записи", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["audit:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (_path: string, opts: { params: { query: { offset: number } } }) => {
      const offset = opts.params.query.offset;
      if (offset === 0) {
        return {
          data: {
            entries: [{ source: "replay", actor: "u1", action: "requested", target: "page-1", created_at: "2026-01-01T00:00:00Z" }],
            next_offset: 50,
          },
          error: undefined,
        };
      }
      return {
        data: {
          entries: [{ source: "identity", actor: "u2", action: "granted", target: "page-2", created_at: "2026-01-02T00:00:00Z" }],
          next_offset: null,
        },
        error: undefined,
      };
    });

    const wrapper = await mountView(fakeGet);
    await flushPromises();
    expect(wrapper.text()).toContain("page-1");

    const loadMoreButton = wrapper.findAll("button").find((b) => b.text().includes("Загрузить ещё"))!;
    expect(loadMoreButton.attributes("disabled")).toBeUndefined();
    await loadMoreButton.trigger("click");
    await flushPromises();

    expect(fakeGet).toHaveBeenCalledTimes(2);
    expect(fakeGet.mock.calls[1][1].params.query.offset).toBe(50);
    expect(wrapper.text()).toContain("page-1");
    expect(wrapper.text()).toContain("page-2");
    expect(wrapper.text()).toContain("Больше нет записей");
  });

  it("смена фильтра source сбрасывает накопленные записи и перезапрашивает с offset=0", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["audit:read"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (_path: string, opts: { params: { query: { source?: string; offset: number } } }) => ({
      data: {
        entries: [
          {
            source: opts.params.query.source ?? "replay",
            actor: "u1",
            action: "x",
            target: opts.params.query.source ?? "all",
            created_at: "2026-01-01T00:00:00Z",
          },
        ],
        next_offset: null,
      },
      error: undefined,
    }));

    const wrapper = await mountView(fakeGet);
    await flushPromises();
    expect(wrapper.text()).toContain("all");

    const select = wrapper.findComponent(NSelect);
    await select.vm.$emit("update:value", "identity");
    await flushPromises();

    const lastCall = fakeGet.mock.calls[fakeGet.mock.calls.length - 1];
    expect(lastCall[1].params.query.source).toBe("identity");
    expect(lastCall[1].params.query.offset).toBe(0);
    expect(wrapper.text()).toContain("identity");
    expect(wrapper.text()).not.toContain("all");
  });
});
