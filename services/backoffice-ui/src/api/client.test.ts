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
});