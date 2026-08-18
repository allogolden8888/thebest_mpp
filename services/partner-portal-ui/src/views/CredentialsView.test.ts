// Covers the two things this screen exists to get right (see task brief /
// credentials.go doc-comments):
// 1. rotate is gated to partner-admin (:disabled on non-admin, same pattern
//    as backoffice-ui ConfigView.vue) and requires an explicit confirm
//    dialog before the request fires.
// 2. successful rotate opens a PERSISTENT reveal (no auto-close), the
//    plaintext_secret is rendered, and closing it invalidates the list
//    query so it can refetch.
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { mount, flushPromises, type VueWrapper } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createRouter, createMemoryHistory } from "vue-router";
import { VueQueryPlugin, QueryClient } from "@tanstack/vue-query";
import { h, defineComponent } from "vue";
import naive, { NMessageProvider, NDialogProvider } from "naive-ui";
import CredentialsView from "./CredentialsView.vue";
import { useAuthStore, PARTNER_ADMIN_ROLE } from "../stores/auth";
import { apiClientsKey } from "../api/useApi";
import { makeTestJwt } from "../test-utils/jwt";

// naive-ui overlays (dialog/modal) render via <Teleport> to document.body,
// outside `wrapper`'s own element subtree — same reasoning as
// backoffice-ui/src/views/ExecutionControlView.test.ts. Scoped to
// `.n-dialog__action`/`.n-card__footer` (dialog/modal button containers)
// rather than a plain text match, since the row's own "Ротировать" button
// shares its label with the confirm dialog's positive button and both live
// under document.body once teleported.
function dialogButtons(text: string): HTMLButtonElement[] {
  return Array.from(document.body.querySelectorAll(".n-dialog__action button, .n-card__footer button")).filter((b) =>
    b.textContent?.includes(text),
  ) as HTMLButtonElement[];
}

function bodyText(): string {
  return document.body.textContent ?? "";
}

const SUMMARY_ROW = {
  id: 1,
  partner_id: "acme",
  application_id: "app-1",
  credential_ref: "vault/acme/app-1/v1",
  secret_version: 1,
  status: "active" as const,
  issued_at: "2026-01-01T00:00:00Z",
  issued_by: "u1",
};

// Найдено при интеграции: без явного unmount() предыдущего теста naive-ui
// диалог-провайдер от него остаётся технически "живым" (Vue-инстанс не
// уничтожен, только его DOM стёрт через innerHTML="") — dialogButtons()
// в следующем тесте иногда матчит кнопку СТАРОГО диалога с уже
// осиротевшим/устаревшим onPositiveClick, что рушится как
// "onPositiveClick is not a function" асинхронно после клика. afterEach
// ниже явно размонтирует каждый wrapper.
let mountedWrapper: VueWrapper | null = null;

async function mountView(fakePartnerGet: ReturnType<typeof vi.fn>, fakePartnerPost: ReturnType<typeof vi.fn>) {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/", name: "root", component: { template: "<div/>" } }],
  });
  await router.push("/");
  await router.isReady();

  const Host = defineComponent({
    render: () =>
      h(NMessageProvider, null, {
        default: () => h(NDialogProvider, null, { default: () => h(CredentialsView) }),
      }),
  });

  const wrapper = mount(Host, {
    attachTo: document.body,
    global: {
      plugins: [router, naive, [VueQueryPlugin, { queryClient: new QueryClient() }]],
      provide: {
        [apiClientsKey as symbol]: {
          partner: { GET: fakePartnerGet, POST: fakePartnerPost },
          billing: { GET: vi.fn(), POST: vi.fn() },
        },
      },
    },
  });
  mountedWrapper = wrapper;
  return wrapper;
}

describe("CredentialsView", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
    document.body.innerHTML = "";
  });

  afterEach(() => {
    mountedWrapper?.unmount();
    mountedWrapper = null;
  });

  it("список credentials виден и не-admin (rotate заблокирован)", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: ["partner-viewer"] } }));
    const fakeGet = vi.fn(async () => ({ data: [SUMMARY_ROW], error: undefined }));
    const fakePost = vi.fn();

    const wrapper = await mountView(fakeGet, fakePost);
    await flushPromises();

    expect(wrapper.text()).toContain("app-1");
    expect(wrapper.text()).toContain("vault/acme/app-1/v1");
    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Ротировать"))!;
    expect(rotateButton.attributes("disabled")).toBeDefined();
  });

  it("admin: клик 'Ротировать' требует подтверждения в диалоге перед отправкой запроса", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [SUMMARY_ROW], error: undefined }));
    const fakePost = vi.fn(async () => ({
      data: { credential_ref: "vault/acme/app-1/v2", secret_version: 2, plaintext_secret: "sk_live_super_secret", issued_at: "2026-02-01T00:00:00Z" },
      error: undefined,
    }));

    const wrapper = await mountView(fakeGet, fakePost);
    await flushPromises();

    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Ротировать"))!;
    expect(rotateButton.attributes("disabled")).toBeUndefined();
    await rotateButton.trigger("click");
    await flushPromises();

    // Диалог показан, запрос ещё не ушёл.
    expect(fakePost).not.toHaveBeenCalled();
    expect(bodyText()).toContain("немедленно инвалидирует текущий секрет");

    const confirmButtons = dialogButtons("Ротировать");
    expect(confirmButtons.length).toBeGreaterThan(0);
    confirmButtons[0].click();
    await flushPromises();

    expect(fakePost).toHaveBeenCalledTimes(1);
    const [path, opts] = fakePost.mock.calls[0] as unknown as [string, { params: { path: { application_id: string } } }];
    expect(path).toBe("/applications/{application_id}/credentials/rotate");
    expect(opts.params.path.application_id).toBe("app-1");
  });

  it("успешная ротация открывает персистентную модалку с show-once секретом, которая не закрывается сама", async () => {
    const auth = useAuthStore();
    auth.setToken(makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } }));
    const fakeGet = vi.fn(async () => ({ data: [SUMMARY_ROW], error: undefined }));
    const fakePost = vi.fn(async () => ({
      data: { credential_ref: "vault/acme/app-1/v2", secret_version: 2, plaintext_secret: "sk_live_super_secret", issued_at: "2026-02-01T00:00:00Z" },
      error: undefined,
    }));

    const wrapper = await mountView(fakeGet, fakePost);
    await flushPromises();

    const rotateButton = wrapper.findAll("button").find((b) => b.text().includes("Ротировать"))!;
    await rotateButton.trigger("click");
    await flushPromises();
    const confirmButtons = dialogButtons("Ротировать");
    confirmButtons[0].click();
    await flushPromises();

    // plaintext_secret рендерится в <NInput readonly> — value input'а не
    // попадает в textContent, поэтому bodyText() здесь не подходит.
    const revealedInputValues = Array.from(document.body.querySelectorAll("input")).map((el) => (el as HTMLInputElement).value);
    expect(revealedInputValues).toContain("sk_live_super_secret");
    expect(bodyText()).toContain("никогда не будет доступно повторно");
    expect(fakeGet).toHaveBeenCalledTimes(1); // list not refetched yet — modal still open

    const closeButton = dialogButtons("Я сохранил секрет")[0];
    closeButton.click();
    await flushPromises();

    const remainingInputValues = Array.from(document.body.querySelectorAll("input")).map((el) => (el as HTMLInputElement).value);
    expect(remainingInputValues).not.toContain("sk_live_super_secret");
    expect(fakeGet).toHaveBeenCalledTimes(2); // list invalidated/refetched after close
  });
});
