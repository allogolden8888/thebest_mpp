-- V035__partner_portal_credentials.sql
-- BACKOFFICE_ROADMAP.md Production Readiness Review P0 item 5
-- ("Identity-контур не production-класса"). iam.partner_portal_users (V025)
-- was seeded ahead of Фаза 3 as an empty FK target for
-- iam.partner_portal_role_assignments, on the assumption that rows would be
-- JIT-provisioned from a Keycloak `sub` claim on first login (see
-- PartnerUsersView.vue comment: "external_id должен уже существовать...
-- заводится при первом логине"). That assumption never became true — no
-- real Keycloak is deployed (partner-portal-ui's LoginView.vue accepted a
-- pasted JWT in a textarea, no HTTP round-trip at login, so nothing has
-- ever inserted a row here; verified by grep — zero INSERT statements into
-- this table anywhere in the repo before this migration). Replacing the
-- paste-a-JWT flow with real username/password login needs credentials to
-- verify against, and JIT provisioning doesn't make sense for a password
-- login (you can't auto-create an account from a password nobody vouched
-- for) — so this is a real, new admin-side "create partner portal user"
-- step, not just wiring up something that already existed.
--
-- Same shape as iam.staff_accounts (V031): username/password_hash/active/
-- created_by, bcrypt inside iam-service, hash never crosses the gRPC
-- boundary. Difference from V031: no backfill dilemma here (the table is
-- genuinely empty, confirmed above), so username/password_hash/created_by
-- can be added NOT NULL directly instead of needing invalid-hash
-- placeholder rows first.
ALTER TABLE iam.partner_portal_users
    ADD COLUMN username      TEXT NOT NULL,
    ADD COLUMN password_hash TEXT NOT NULL,
    ADD COLUMN active        BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN created_by    TEXT NOT NULL;

CREATE UNIQUE INDEX partner_portal_users_username_idx ON iam.partner_portal_users (username);
