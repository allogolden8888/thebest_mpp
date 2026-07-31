// CODE_REVIEW.md MEDIUM finding (subagent-1 / backoffice-ui #5) —
// `message.error(String(err))` guaranteed "[object Object]" for any
// non-string/non-Error error value. This covers the fix and its root cause
// (openapi-fetch's `error` is untyped since openapi.yaml has no 4xx/5xx
// response schemas).
import { describe, expect, it } from "vitest";
import { extractErrorMessage } from "./errorMessage";

describe("extractErrorMessage", () => {
  it("возвращает как есть строку (обычный ответ backoffice-api через http.Error — plain text)", () => {
    expect(extractErrorMessage("reason обязателен для execution-control override")).toBe(
      "reason обязателен для execution-control override",
    );
  });

  it("возвращает message из настоящего Error", () => {
    expect(extractErrorMessage(new Error("network failure"))).toBe("network failure");
  });

  it("никогда не возвращает буквально '[object Object]' для произвольного объекта", () => {
    const result = extractErrorMessage({ foo: "bar" });
    expect(result).not.toBe("[object Object]");
  });

  it("достаёт message/error/detail/reason поле из JSON-подобного объекта ошибки", () => {
    expect(extractErrorMessage({ message: "insufficient scope" })).toBe("insufficient scope");
    expect(extractErrorMessage({ error: "forbidden" })).toBe("forbidden");
    expect(extractErrorMessage({ reason: "bad state" })).toBe("bad state");
  });

  it("возвращает разумное сообщение по умолчанию для null/undefined", () => {
    expect(extractErrorMessage(null)).toBe("Неизвестная ошибка");
    expect(extractErrorMessage(undefined)).toBe("Неизвестная ошибка");
  });
});
