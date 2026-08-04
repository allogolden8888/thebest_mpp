import { describe, expect, it } from "vitest";
import { decodeJwtPayload, decodeRealmRoles, ADMIN_ROLE } from "./jwtRoles";
import { makeTestJwt } from "../test-utils/jwt";

describe("decodeJwtPayload", () => {
  it("возвращает null для не-JWT строки", () => {
    expect(decodeJwtPayload("not-a-jwt")).toBeNull();
  });

  it("возвращает null для мусора вместо валидного base64/JSON payload", () => {
    expect(decodeJwtPayload("header.not-base64-json!!!.sig")).toBeNull();
  });

  it("декодирует payload реального формата JWT", () => {
    const token = makeTestJwt({ sub: "u1", realm_access: { roles: ["backoffice-admin"] } });
    expect(decodeJwtPayload(token)).toEqual({ sub: "u1", realm_access: { roles: ["backoffice-admin"] } });
  });
});

describe("decodeRealmRoles", () => {
  it("возвращает [] для null токена", () => {
    expect(decodeRealmRoles(null)).toEqual([]);
  });

  it("возвращает [] для токена без realm_access", () => {
    const token = makeTestJwt({ sub: "u1" });
    expect(decodeRealmRoles(token)).toEqual([]);
  });

  it("возвращает [] для битого токена, не кидает исключение", () => {
    expect(decodeRealmRoles("garbage")).toEqual([]);
  });

  it("извлекает realm_access.roles из стандартного Keycloak-claim", () => {
    const token = makeTestJwt({ sub: "u1", realm_access: { roles: ["backoffice-admin", "support"] } });
    expect(decodeRealmRoles(token)).toEqual(["backoffice-admin", "support"]);
    expect(decodeRealmRoles(token)).toContain(ADMIN_ROLE);
  });

  it("игнорирует нестроковые элементы в roles (не даёт битому токену притвориться admin'ом)", () => {
    const token = makeTestJwt({ sub: "u1", realm_access: { roles: ["ok", 123, null] } });
    expect(decodeRealmRoles(token)).toEqual(["ok"]);
  });
});
