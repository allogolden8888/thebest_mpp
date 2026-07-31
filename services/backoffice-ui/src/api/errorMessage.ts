// CODE_REVIEW.md MEDIUM finding (subagent-1 / backoffice-ui #5) — every view
// used to do `message.error(String(err))` on whatever `openapi-fetch`
// returned as `error`. `openapi.yaml` defines no 4xx/5xx response schema for
// any operation, so `error` is untyped (`unknown`) at the type level; at
// runtime, `backoffice-api` returns plain-text bodies via `http.Error`/
// `internalError` (see `backoffice-api/internal/httpapi/errors.go`) for
// almost every error path today, but nothing guarantees that stays true
// forever (a future handler could return a JSON body, and network-level
// failures throw a real `Error`/`DOMException`, not a string). `String()` on
// a non-string/non-Error object always yields the literal "[object Object]",
// which is exactly the failure mode this exists to prevent, especially
// during an active incident — precisely when this tool gets used.
export function extractErrorMessage(err: unknown): string {
  if (err == null) return "Неизвестная ошибка";
  if (typeof err === "string") return err.trim() || "Неизвестная ошибка";
  if (err instanceof Error) return err.message || "Неизвестная ошибка";
  if (typeof err === "object") {
    const obj = err as Record<string, unknown>;
    for (const key of ["message", "error", "detail", "reason"]) {
      const v = obj[key];
      if (typeof v === "string" && v.trim()) return v;
    }
    try {
      return JSON.stringify(err);
    } catch {
      return "Неизвестная ошибка (не удалось сериализовать)";
    }
  }
  return String(err);
}
