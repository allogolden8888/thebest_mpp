// Тот же паттерн, что services/backoffice-ui/src/api/errorMessage.ts — см.
// doc-комментарий там за полным обоснованием (partner-self-service-api/
// billing-self-service-api тоже возвращают plain-text тела через
// http.Error, ничего не гарантирует, что так будет всегда).
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
