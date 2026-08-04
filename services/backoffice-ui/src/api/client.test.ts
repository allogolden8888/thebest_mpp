// Тест реального openapi-fetch клиента (не мок интерфейса ApiClient) —
// подменяется только global.fetch (граница системы), проверяется, что
// middleware реально добавляет заголовок Authorization из Pinia auth store.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { createApiClient } from "./client";
import { useAuthStore } from "../stores/auth";

describe("createApiClient", () => {
  beforeEach(() => {
    localStorage.clear();
    setActivePinia(createPinia());
  });

  it("не добавляет Authorization, если токена нет", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ records: [] }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const client = createApiClient("http://localhost/v1");
    await client.GET("/dlq", { params: { query: {} } });

    const request = fetchMock.mock.calls[0][0] as Request;
    expect(request.headers.get("Authorization")).toBeNull();
  });

  it("добавляет Authorization: Bearer <token> из auth store", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-abc");

    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ records: [] }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const client = createApiClient("http://localhost/v1");
    await client.GET("/dlq", { params: { query: {} } });

    const request = fetchMock.mock.calls[0][0] as Request;
    expect(request.headers.get("Authorization")).toBe("Bearer jwt-abc");
    expect(request.url).toBe("http://localhost/v1/dlq");
  });

  // CODE_REVIEW.md HIGH finding (subagent-1 / backoffice-ui #4) — no 401
  // response interceptor existed at all; an expired/invalid token was sent
  // forever with no redirect-to-login signal.
  it("на 401 очищает токен и вызывает onUnauthorized", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-expired");

    const fetchMock = vi.fn(
      async () => new Response("токен невалиден", { status: 401, headers: { "Content-Type": "text/plain" } }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const onUnauthorized = vi.fn();
    const client = createApiClient("http://localhost/v1", { onUnauthorized });
    await client.GET("/dlq", { params: { query: {} } });

    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    expect(auth.token).toBeNull();
    expect(auth.isAuthenticated).toBe(false);
  });

  it("не вызывает onUnauthorized на успешный ответ", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-ok");

    const fetchMock = vi.fn(
      async () => new Response(JSON.stringify({ records: [] }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const onUnauthorized = vi.fn();
    const client = createApiClient("http://localhost/v1", { onUnauthorized });
    await client.GET("/dlq", { params: { query: {} } });

    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(auth.token).toBe("jwt-ok");
  });

  it("на 403 (валидный токен, не хватает роли) НЕ вызывает onUnauthorized — токен рабочий, редиректить не нужно", async () => {
    const auth = useAuthStore();
    auth.setToken("jwt-valid-non-admin");

    const fetchMock = vi.fn(
      async () => new Response('требуется роль "backoffice-admin"', { status: 403, headers: { "Content-Type": "text/plain" } }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const onUnauthorized = vi.fn();
    const client = createApiClient("http://localhost/v1", { onUnauthorized });
    const { error } = await client.GET("/dlq", { params: { query: {} } });

    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(auth.token).toBe("jwt-valid-non-admin");
    expect(error).toBe('требуется роль "backoffice-admin"');
  });
});