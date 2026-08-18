// Client-side mirror of the RBAC barrier partner-self-service-api enforces
// server-side (internal/auth/jwt.go RequireAdmin, role `partner-admin` in
// the standard Keycloak `realm_access.roles` claim shape) — same pattern as
// services/backoffice-ui/src/stores/jwtRoles.ts.
//
// This is NOT a security boundary on its own — the UI never verifies the
// token's signature (that's the backend's job on every request). A
// malicious client can trivially forge a fake `realm_access.roles` claim to
// get past this check, but the forged token still fails signature
// validation server-side and every destructive route still 403s. This
// module exists purely so a legitimate partner-viewer gets a clear "you
// don't have this role" UI instead of a confusing raw 403 after clicking
// through a form.
export const PARTNER_ADMIN_ROLE = "partner-admin";

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

/** Extracts `partner_id` (partner-self-service-api/internal/auth/jwt.go Claims shape) from a JWT, "" on any parse failure. */
export function decodePartnerID(token: string | null): string {
  if (!token) return "";
  const payload = decodeJwtPayload(token);
  const partnerID = payload?.["partner_id"];
  return typeof partnerID === "string" ? partnerID : "";
}
