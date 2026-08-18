import { describe, expect, it } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import naive from "naive-ui";
import BillingView from "./BillingView.vue";
import { apiClientsKey } from "../api/useApi";

// Заглушка billing-клиента: маршрутизирует по path, возвращает канонические
// формы ответов billing-self-service-api (см. openapi-billing.yaml). custom:
// false и fee_known: false — намеренные "не знаю точных цифр" ответы, а не
// ошибки, проверяем именно это ниже.
function stubBillingClient(overrides: Partial<Record<string, unknown>> = {}) {
  const responses: Record<string, unknown> = {
    "/ledger": [
      {
        id: 1,
        charge_id: "chg1",
        account_id: "acc1",
        partner_id: "click_uz",
        amount: "3500.0000",
        currency: "UZS",
        entry_type: "charge",
        created_at: "2026-08-01T10:00:00Z",
      },
      {
        id: 2,
        charge_id: "chg2",
        account_id: "acc1",
        partner_id: "click_uz",
        amount: "1200.5000",
        currency: "UZS",
        entry_type: "compensating",
        source_charge_id: "chg1",
        created_at: "2026-08-02T10:00:00Z",
      },
    ],
    "/summary": [{ currency: "UZS", total: "2299.5000" }],
    "/tariff": { custom: false },
    "/recurring": [{ sender_id: "SENDER1", type: "ALPHANAME", fee_known: false }],
    ...overrides,
  };
  return {
    GET: async (path: string) => ({ data: responses[path], error: undefined }),
  };
}

function mountView(billing: ReturnType<typeof stubBillingClient>) {
  return mount(BillingView, {
    global: {
      plugins: [naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: { [apiClientsKey as symbol]: { partner: billing, billing } },
    },
  });
}

describe("BillingView", () => {
  it("рендерит ledger без парсинга amount как float (обрезает хвостовые нули строкой)", async () => {
    const wrapper = mountView(stubBillingClient());
    await flushPromises();

    expect(wrapper.text()).toContain("3500");
    expect(wrapper.text()).not.toContain("3500.0000");
    expect(wrapper.text()).toContain("1200.5");
  });

  it("custom=false рендерится как информативное сообщение, не ошибка, без угаданных цифр", async () => {
    const wrapper = mountView(stubBillingClient());
    await flushPromises();

    expect(wrapper.text()).toContain("не опубликован");
    expect(wrapper.find(".n-alert--error-type").exists()).toBe(false);
  });

  it("custom=true рендерит реальные цифры тарифа", async () => {
    const wrapper = mountView(
      stubBillingClient({
        "/tariff": {
          custom: true,
          tariff: {
            currency: "UZS",
            price_per_segment: { standard: 100, premium: 250 },
            default_category: "standard",
            recurring_charges: { alphaname_monthly_fee: 50000 },
          },
        },
      }),
    );
    await flushPromises();

    expect(wrapper.text()).toContain("100");
    expect(wrapper.text()).toContain("250");
    expect(wrapper.text()).toContain("50000");
    expect(wrapper.text()).not.toContain("не опубликован");
  });

  it("fee_known=false показывает 'уточняется', не 0 и не пустую ячейку", async () => {
    const wrapper = mountView(stubBillingClient());
    await flushPromises();

    expect(wrapper.text()).toContain("уточняется");
  });

  it("fee_known=true показывает конкретную сумму", async () => {
    const wrapper = mountView(
      stubBillingClient({
        "/recurring": [{ sender_id: "SENDER1", type: "ALPHANAME", fee_known: true, fee: 50000, currency: "UZS" }],
      }),
    );
    await flushPromises();

    expect(wrapper.text()).toContain("50000");
    expect(wrapper.text()).toContain("UZS");
    expect(wrapper.text()).not.toContain("уточняется");
  });
});
