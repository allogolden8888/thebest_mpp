import { describe, expect, it } from "vitest";
import { decodeJwtPayload, decodeRealmRoles, decodePartnerID, PARTNER_ADMIN_ROLE } from "./jwtRoles";
import { makeTestJwt } from "../test-utils/jwt";

describe("decodeJwtPayload", () => {
  it("возвращает null для не-JWT строки", () => {
    expect(decodeJwtPayload("not-a-jwt")).toBeNull();
  });

  it("возвращает null для мусора вместо валидного base64/JSON payload", () => {
    expect(decodeJwtPayload("header.not-base64-json!!!.sig")).toBeNull();
  });

  it("декодирует payload реального формата JWT", () => {
    const token = makeTestJwt({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } });
    expect(decodeJwtPayload(token)).toEqual({ sub: "u1", partner_id: "acme", realm_access: { roles: [PARTNER_ADMIN_ROLE] } });
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
    const token = makeTestJwt({ sub: "u1", realm_access: { roles: [PARTNER_ADMIN_ROLE, "partner-viewer"] } });
    expect(decodeRealmRoles(token)).toEqual([PARTNER_ADMIN_ROLE, "partner-viewer"]);
    expect(decodeRealmRoles(token)).toContain(PARTNER_ADMIN_ROLE);
  });

  it("игнорирует нестроковые элементы в roles (не даёт битому токену притвориться admin'ом)", () => {
    const token = makeTestJwt({ sub: "u1", realm_access: { roles: ["ok", 123, null] } });
    expect(decodeRealmRoles(token)).toEqual(["ok"]);
  });
});

describe("decodePartnerID", () => {
  it("возвращает '' для null токена", () => {
    expect(decodePartnerID(null)).toBe("");
  });

  it("возвращает '' для токена без partner_id", () => {
    expect(decodePartnerID(makeTestJwt({ sub: "u1" }))).toBe("");
  });

  it("извлекает partner_id из claim'а", () => {
    expect(decodePartnerID(makeTestJwt({ sub: "u1", partner_id: "click_uz" }))).toBe("click_uz");
  });
});
