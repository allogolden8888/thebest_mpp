// Смоук-тест "Мои шаблоны": рендер списка через api.partner (partner_id
// НИКОГДА не отправляется/не отображается — см. doc-комментарий в самом
// TemplatesView.vue и в services/partner-self-service-api/internal/httpapi/
// templates.go за тем, почему), фильтры sender_id/category/status уходят в
// query-параметры запроса.
import { describe, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import naive from "naive-ui";
import TemplatesView from "./TemplatesView.vue";
import { apiClientsKey } from "../api/useApi";

const sampleTemplates = [
  {
    template_id: "tpl-1",
    partner_id: "click_uz",
    operator_id: "ucell",
    sender_id: "MYSHOP",
    channel: "sms",
    category: "SERVICE",
    pattern: "Ваш код подтверждения: {code}",
    version: 3,
    status: "active",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
  },
  {
    template_id: "tpl-2",
    partner_id: "click_uz",
    operator_id: null,
    sender_id: null,
    channel: "sms",
    category: "ADVERTISING",
    pattern: "Скидка 20% сегодня!",
    version: 1,
    status: "archived",
    created_at: "2025-12-01T00:00:00Z",
    updated_at: "2025-12-01T00:00:00Z",
  },
];

function mountView(getImpl: (options?: unknown) => Promise<{ data: unknown; error: unknown }>) {
  const stubApiClient = { GET: getImpl };
  // retry: false — иначе TanStack Query по умолчанию делает 3 повторных
  // попытки с реальными таймерами перед тем, как осесть в error-состоянии,
  // и тест на error-рендер не успевает его увидеть за flushPromises().
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return mount(TemplatesView, {
    global: {
      plugins: [naive, [VueQueryPlugin, { queryClient }]],
      provide: { [apiClientsKey as symbol]: { partner: stubApiClient, billing: stubApiClient } },
    },
  });
}

describe("TemplatesView", () => {
  it("рендерит список шаблонов, включая 'все отправители' для null sender_id", async () => {
    setActivePinia(createPinia());
    const wrapper = mountView(async () =>
      Promise.resolve({ data: { templates: sampleTemplates, limit: 20, offset: 0 }, error: undefined }),
    );
    await flushPromises();

    expect(wrapper.text()).toContain("MYSHOP");
    expect(wrapper.text()).toContain("все отправители");
    expect(wrapper.text()).toContain("Ваш код подтверждения: {code}");
    expect(wrapper.text()).toContain("SERVICE");
    expect(wrapper.text()).toContain("ADVERTISING");
  });

  it("никогда не рендерит поле/фильтр partner_id", async () => {
    setActivePinia(createPinia());
    const wrapper = mountView(async () =>
      Promise.resolve({ data: { templates: sampleTemplates, limit: 20, offset: 0 }, error: undefined }),
    );
    await flushPromises();

    const html = wrapper.html();
    expect(html).not.toContain("partner_id");
    // Значение partner_id ("click_uz") тоже не должно нигде отображаться —
    // подтверждает, что колонка/поле реально отсутствует, а не просто скрыт label.
    expect(wrapper.text()).not.toContain("click_uz");

    const inputs = wrapper.findAll("input");
    for (const input of inputs) {
      expect(input.attributes("placeholder")?.toLowerCase() ?? "").not.toContain("partner_id");
    }
  });

  it("передаёт sender_id/category/status как query-параметры при запросе", async () => {
    setActivePinia(createPinia());
    const getSpy = vi.fn(async () =>
      Promise.resolve({ data: { templates: [], limit: 20, offset: 0 }, error: undefined }),
    );
    const wrapper = mountView(getSpy);
    await flushPromises();

    expect(getSpy).toHaveBeenCalledWith(
      "/templates",
      expect.objectContaining({
        params: {
          query: expect.objectContaining({
            sender_id: undefined,
            category: undefined,
            status: undefined,
            limit: 20,
            offset: 0,
          }),
        },
      }),
    );

    await wrapper.find('input[placeholder="ID отправителя"]').setValue("MYSHOP");
    await flushPromises();

    expect(getSpy).toHaveBeenLastCalledWith(
      "/templates",
      expect.objectContaining({
        params: {
          query: expect.objectContaining({ sender_id: "MYSHOP", limit: 20, offset: 0 }),
        },
      }),
    );
  });

  it("показывает ошибку через extractErrorMessage при сбое запроса", async () => {
    setActivePinia(createPinia());
    const wrapper = mountView(async () =>
      Promise.resolve({ data: undefined, error: { message: "template-management-service недоступен" } }),
    );
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("template-management-service недоступен");
  });
});
