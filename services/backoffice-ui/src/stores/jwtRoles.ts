// CODE_REVIEW.md CRITICAL finding (subagent-1 / backoffice-ui #1) — client-side
// mirror of the RBAC barrier added this session to backoffice-api
// (backoffice-api/internal/auth/jwt.go): parses the standard Keycloak
// `realm_access.roles` claim out of the (already-authenticated, already
// server-validated-per-request) bearer token so the UI can hide/disable
// destructive actions for non-admins.
//
// This is NOT a security boundary on its own — the UI never verifies the
// token's signature (that's the backend's job on every request, and it's
// enforced there via auth.RequireRole). A malicious client can trivially
// forge a fake `realm_access.roles` claim to get past this check, but the
// forged token still fails signature validation server-side and every
// destructive route still 403s. This module exists purely so a legitimate
// non-admin user gets a clear "you don't have this role" UI instead of
// either (a) seeing controls they can't use, or (b) a confusing raw 403
// after clicking through a form.
export const ADMIN_ROLE = "backoffice-admin";

/** Decodes a JWT's payload (middle segment) without verifying the signature. */
export function decodeJwtPayload(token: string): Record<string, unknown> | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    const base64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
    const json = decodeURIComponent(
      atob(padded)
        .split("")
        .map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0"))
        .join(""),
    );
    return JSON.parse(json) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** Extracts `realm_access.roles` (standard Keycloak claim shape) from a JWT, [] on any parse failure. */
export function decodeRealmRoles(token: string | null): string[] {
  if (!token) return [];
  const payload = decodeJwtPayload(token);
  const realmAccess = payload?.["realm_access"];
  if (
    realmAccess &&
    typeof realmAccess === "object" &&
    Array.isArray((realmAccess as { roles?: unknown }).roles)
  ) {
    return (realmAccess as { roles: unknown[] }).roles.filter((r): r is string => typeof r === "string");
  }
  return [];
}
