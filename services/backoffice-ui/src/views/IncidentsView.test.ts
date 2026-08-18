// Covers: permission-gated rendering (RequirePermission "incident:manage"),
// the open-incident flow (form validity + POST body), selecting a row to
// load detail (timeline + notes composed in one GET), adding a note, and
// the resolve flow (confirm dialog before POST fires, postmortem_notes
// required). Mirrors the mount/mock conventions used by
// AccessControlView.test.ts.
import { describe, expect, it, beforeEach, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider, NSelect } from "naive-ui";
import IncidentsView from "./IncidentsView.vue";
import { useAuthStore } from "../stores/auth";
import { apiClientKey } from "../api/useApi";

function bodyButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll("button")).filter((b) => b.textContent?.includes(text)) as HTMLButtonElement[];
}

interface FakeApi {
  GET: ReturnType<typeof vi.fn>;
  POST: ReturnType<typeof vi.fn>;
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
        default: () => h(NDialogProvider, null, { default: () => h(IncidentsView) }),
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

const oneIncident = {
  incidents: [
    { id: 1, title: "Кассовый разрыв billing-service", severity: "HIGH", status: "OPEN", opened_by: "u1", opened_at: "2026-01-01T00:00:00Z" },
  ],
};

const detailForIncident1 = {
  incident: oneIncident.incidents[0],
  timeline: [
    { id: 10, scope: "partner", scope_id: "p1", state: "THROTTLED", admission_rate: 0.5, reason: "overload", requested_by: "u1", created_at: "2026-01-01T00:00:00Z" },
  ],
  notes: [{ id: 20, incident_id: 1, author: "u1", note: "investigating", created_at: "2026-01-01T00:05:00Z" }],
};

describe("IncidentsView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  it("без incident:manage показывает 'недостаточно прав' и не запрашивает данные", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    const fakeApi: FakeApi = { GET: vi.fn(), POST: vi.fn() };

    const wrapper = await mountView(fakeApi);

    expect(wrapper.text()).toContain("Недостаточно прав");
    expect(fakeApi.GET).not.toHaveBeenCalled();
  });

  it("с incident:manage показывает список инцидентов", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({ data: oneIncident, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    expect(wrapper.text()).toContain("Кассовый разрыв billing-service");
    expect(wrapper.text()).toContain("HIGH");
  });

  it("кнопка 'Открыть' отключена, пока title и severity не заполнены", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({ data: { incidents: [] }, error: undefined }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const openButton = wrapper.findAll("button").find((b) => b.text().includes("Открыть"))!;
    expect(openButton.attributes("disabled")).toBeDefined();
  });

  it("отправляет POST /incidents с заполненными title/severity", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async () => ({ data: { incidents: [] }, error: undefined }));
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { id: 5, title: "SMPP down", severity: "CRITICAL", status: "OPEN", opened_by: "u1", opened_at: "now" },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    await wrapper.find("input").setValue("SMPP down");
    const select = wrapper.findComponent(NSelect);
    await select.vm.$emit("update:value", "CRITICAL");
    await flushPromises();

    const openButton = wrapper.findAll("button").find((b) => b.text().includes("Открыть"))!;
    expect(openButton.attributes("disabled")).toBeUndefined();
    await openButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/incidents");
    expect((fakePost.mock.calls[0][1] as { body: { title: string; severity: string } }).body).toEqual({
      title: "SMPP down",
      severity: "CRITICAL",
    });
  });

  it("клик 'Просмотр' загружает детали инцидента (таймлайн + заметки)", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/incidents/{incident_id}" ? { data: detailForIncident1, error: undefined } : { data: oneIncident, error: undefined },
    );
    const fakeApi: FakeApi = { GET: fakeGet, POST: vi.fn() };

    const wrapper = await mountView(fakeApi);
    await flushPromises();

    const viewButton = wrapper.findAll("button").find((b) => b.text().includes("Просмотр"))!;
    await viewButton.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("Инцидент #1");
    expect(wrapper.text()).toContain("investigating");
    expect(wrapper.text()).toContain("THROTTLED");
  });

  it("добавление заметки отправляет POST /incidents/{incident_id}/notes", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/incidents/{incident_id}" ? { data: detailForIncident1, error: undefined } : { data: oneIncident, error: undefined },
    );
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { id: 21, incident_id: 1, author: "u1", note: "escalated to on-call", created_at: "now" },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();
    const viewButton = wrapper.findAll("button").find((b) => b.text().includes("Просмотр"))!;
    await viewButton.trigger("click");
    await flushPromises();

    const noteInputs = wrapper.findAll("input").filter((i) => i.element.type !== "checkbox");
    await noteInputs[noteInputs.length - 1].setValue("escalated to on-call");
    const addButton = wrapper.findAll("button").find((b) => b.text().includes("Добавить"))!;
    await addButton.trigger("click");
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/incidents/{incident_id}/notes");
    expect((fakePost.mock.calls[0][1] as { params: { path: { incident_id: number } } }).params.path).toEqual({ incident_id: 1 });
    expect((fakePost.mock.calls[0][1] as { body: { note: string } }).body).toEqual({ note: "escalated to on-call" });
  });

  it("закрытие инцидента требует подтверждения в диалоге перед вызовом POST /resolve", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-token");
    auth.permissions = ["incident:manage"];
    auth.meLoaded = true;
    const fakeGet = vi.fn(async (path: string) =>
      path === "/incidents/{incident_id}" ? { data: detailForIncident1, error: undefined } : { data: oneIncident, error: undefined },
    );
    const fakePost = vi.fn(async (..._args: unknown[]) => ({
      data: { ...oneIncident.incidents[0], status: "RESOLVED", resolved_by: "u1", resolved_at: "now", postmortem_notes: "root cause: X" },
      error: undefined,
    }));
    const fakeApi: FakeApi = { GET: fakeGet, POST: fakePost };

    const wrapper = await mountView(fakeApi);
    await flushPromises();
    const viewButton = wrapper.findAll("button").find((b) => b.text().includes("Просмотр"))!;
    await viewButton.trigger("click");
    await flushPromises();

    const textareas = wrapper.findAll("textarea");
    await textareas[0].setValue("root cause: X");
    const closeButton = wrapper.findAll("button").find((b) => b.text().includes("Закрыть") && b.text() !== "Закрыть инцидент")!;
    await closeButton.trigger("click");
    await flushPromises();

    expect(fakePost).not.toHaveBeenCalled();
    const confirmButtons = bodyButtons("Закрыть").filter((b) => !wrapper.element.contains(b));
    expect(confirmButtons.length).toBeGreaterThan(0);

    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    expect(fakePost.mock.calls[0][0]).toBe("/incidents/{incident_id}/resolve");
    expect((fakePost.mock.calls[0][1] as { body: { postmortem_notes: string } }).body).toEqual({ postmortem_notes: "root cause: X" });
  });
});
