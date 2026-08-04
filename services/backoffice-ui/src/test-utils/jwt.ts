// Test-only helper — builds a syntactically-valid (unsigned) JWT string so
// tests can exercise src/stores/jwtRoles.ts's payload decode without a real
// Keycloak-issued token. Never used outside tests.
export function makeTestJwt(payload: Record<string, unknown>): string {
  const base64url = (obj: unknown) =>
    btoa(JSON.stringify(obj)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const header = base64url({ alg: "RS256", typ: "JWT" });
  const body = base64url(payload);
  return `${header}.${body}.test-signature`;
}
