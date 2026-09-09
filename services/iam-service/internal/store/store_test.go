package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool — реальный локальный PostgreSQL 17 (brew), migrations/V025__iam.sql
// уже применена в этой песочнице. Пропускается, если Postgres недоступен —
// тот же обход, что в остальной сессии.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("IAM_SERVICE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func cleanupAssignments(t *testing.T, pool *pgxpool.Pool, externalID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM iam.staff_role_assignments WHERE external_id = $1`, externalID)
		_, _ = pool.Exec(ctx, `DELETE FROM iam.identity_audit WHERE target LIKE $1`, externalID+":%")
	})
}

// createStaffAccount — V031__staff_accounts.sql: staff_role_assignments.
// external_id FK-ит на iam.staff_accounts (перестало быть свободной
// строкой) — тесты на назначение роли сотруднику сначала должны завести
// сам аккаунт, тот же паттерн, что createPartnerPortalUser ниже для
// партнёрской стороны.
func createStaffAccount(t *testing.T, pool *pgxpool.Pool, externalID string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO iam.staff_accounts (external_id, username, password_hash, display_name, created_by)
		VALUES ($1, $1, 'x', 'Test Staff', 'test')`, externalID)
	if err != nil {
		t.Fatalf("createStaffAccount: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.staff_role_assignments WHERE external_id = $1`, externalID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.identity_audit WHERE target LIKE $1`, externalID+":%")
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.staff_accounts WHERE external_id = $1`, externalID)
	})
}

// createPartnerPortalUser — iam.partner_portal_role_assignments.external_id
// FK-ит на iam.partner_portal_users (в отличие от staff_role_assignments,
// где external_id — свободная строка), так что тесты на назначение роли
// партнёрскому пользователю сначала должны завести саму строку пользователя.
func createPartnerPortalUser(t *testing.T, pool *pgxpool.Pool, externalID string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO iam.partner_portal_users (external_id, partner_id, display_name)
		VALUES ($1, 'test-partner', 'Test User')`, externalID)
	if err != nil {
		t.Fatalf("createPartnerPortalUser: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.partner_portal_role_assignments WHERE external_id = $1`, externalID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.identity_audit WHERE target LIKE $1`, externalID+":%")
		_, _ = pool.Exec(context.Background(), `DELETE FROM iam.partner_portal_users WHERE external_id = $1`, externalID)
	})
}

func TestListRolesSeedDataHasBackofficeAdminWithAllPermissions(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)

	roles, err := pg.ListRoles(context.Background())
	if err != nil {
		t.Fatalf("ListRoles: %v", err)
	}

	var admin *Role
	for i := range roles {
		if roles[i].Name == "backoffice-admin" {
			admin = &roles[i]
		}
	}
	if admin == nil {
		t.Fatalf("ожидали роль backoffice-admin в seed-данных V025, не нашли среди %d ролей", len(roles))
	}
	if len(admin.Permissions) < 6 {
		t.Errorf("backoffice-admin должен нести все затравочные права (>=6), получили %d: %v", len(admin.Permissions), admin.Permissions)
	}
}

func TestAssignCheckAndRevokeStaffRoleRoundTrip(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	externalID := "test-" + uuid.NewString()
	createStaffAccount(t, pool, externalID)

	// До назначения — allowed=false, никаких ролей.
	allowed, roles, err := pg.CheckPermission(ctx, externalID, "ops:read")
	if err != nil {
		t.Fatalf("CheckPermission (до назначения): %v", err)
	}
	if allowed || len(roles) != 0 {
		t.Fatalf("ожидали allowed=false до назначения роли, получили allowed=%v roles=%v", allowed, roles)
	}

	assignment, err := pg.AssignStaffRole(ctx, externalID, "ops-viewer", "admin-1")
	if err != nil {
		t.Fatalf("AssignStaffRole: %v", err)
	}
	if assignment.ExternalID != externalID || assignment.Role != "ops-viewer" || assignment.GrantedBy != "admin-1" {
		t.Errorf("неожиданный assignment: %+v", assignment)
	}

	// ops-viewer несёт ops:read (V025 seed) — теперь allowed=true.
	allowed, roles, err = pg.CheckPermission(ctx, externalID, "ops:read")
	if err != nil {
		t.Fatalf("CheckPermission (после назначения): %v", err)
	}
	if !allowed || len(roles) != 1 || roles[0] != "ops-viewer" {
		t.Fatalf("ожидали allowed=true roles=[ops-viewer], получили allowed=%v roles=%v", allowed, roles)
	}

	// ops-viewer НЕ несёт audit:read — изоляция прав между ролями.
	allowed, _, err = pg.CheckPermission(ctx, externalID, "audit:read")
	if err != nil {
		t.Fatalf("CheckPermission (audit:read): %v", err)
	}
	if allowed {
		t.Errorf("ops-viewer не должен нести audit:read — узкие роли не должны утекать в чужие права")
	}

	// Повторное назначение той же (external_id, role) — ErrAlreadyAssigned,
	// не тихий дубль (уникальный частичный индекс staff_role_assignments_active_unique).
	if _, err := pg.AssignStaffRole(ctx, externalID, "ops-viewer", "admin-1"); err != ErrAlreadyAssigned {
		t.Errorf("ожидали ErrAlreadyAssigned на повторное назначение, получили %v", err)
	}

	revoked, err := pg.RevokeStaffRole(ctx, externalID, "ops-viewer", "admin-2")
	if err != nil {
		t.Fatalf("RevokeStaffRole: %v", err)
	}
	if !revoked {
		t.Fatalf("ожидали revoked=true для активного назначения")
	}

	// После отзыва — снова allowed=false (revoked_at IS NULL больше не матчит).
	allowed, _, err = pg.CheckPermission(ctx, externalID, "ops:read")
	if err != nil {
		t.Fatalf("CheckPermission (после отзыва): %v", err)
	}
	if allowed {
		t.Errorf("ожидали allowed=false после отзыва роли")
	}

	// Повторный отзыв уже отозванной роли — идемпотентно, revoked=false, не ошибка.
	revoked, err = pg.RevokeStaffRole(ctx, externalID, "ops-viewer", "admin-2")
	if err != nil {
		t.Fatalf("RevokeStaffRole (повторный): %v", err)
	}
	if revoked {
		t.Errorf("повторный отзыв уже отозванной роли должен вернуть revoked=false")
	}
}

func TestAssignStaffRoleUnknownRoleReturnsErrRoleNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	externalID := "test-" + uuid.NewString()
	cleanupAssignments(t, pool, externalID)

	_, err := pg.AssignStaffRole(context.Background(), externalID, "does-not-exist-role", "admin-1")
	if err != ErrRoleNotFound {
		t.Errorf("ожидали ErrRoleNotFound, получили %v", err)
	}
}

func TestAssignStaffRoleWritesIdentityAudit(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	externalID := "test-" + uuid.NewString()
	createStaffAccount(t, pool, externalID)

	if _, err := pg.AssignStaffRole(ctx, externalID, "ops-viewer", "admin-1"); err != nil {
		t.Fatalf("AssignStaffRole: %v", err)
	}

	var count int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM iam.identity_audit
		WHERE actor = 'admin-1' AND action = 'ROLE_GRANTED' AND target = $1`,
		externalID+":ops-viewer").Scan(&count)
	if err != nil {
		t.Fatalf("readback identity_audit: %v", err)
	}
	if count != 1 {
		t.Errorf("ожидали ровно одну ROLE_GRANTED запись в iam.identity_audit, получили %d", count)
	}
}

func TestListStaffAssignmentsFiltersByExternalID(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	externalID := "test-" + uuid.NewString()
	otherID := "test-" + uuid.NewString()
	createStaffAccount(t, pool, externalID)
	createStaffAccount(t, pool, otherID)

	if _, err := pg.AssignStaffRole(ctx, externalID, "ops-viewer", "admin-1"); err != nil {
		t.Fatalf("AssignStaffRole: %v", err)
	}
	if _, err := pg.AssignStaffRole(ctx, otherID, "incident-manager", "admin-1"); err != nil {
		t.Fatalf("AssignStaffRole (other): %v", err)
	}

	filtered, err := pg.ListStaffAssignments(ctx, externalID)
	if err != nil {
		t.Fatalf("ListStaffAssignments (filtered): %v", err)
	}
	if len(filtered) != 1 || filtered[0].ExternalID != externalID {
		t.Fatalf("ожидали ровно одно назначение для %q, получили %+v", externalID, filtered)
	}

	all, err := pg.ListStaffAssignments(ctx, "")
	if err != nil {
		t.Fatalf("ListStaffAssignments (все): %v", err)
	}
	found := 0
	for _, a := range all {
		if a.ExternalID == externalID || a.ExternalID == otherID {
			found++
		}
	}
	if found != 2 {
		t.Errorf("ожидали найти оба тестовых назначения без фильтра, нашли %d из 2", found)
	}
}

func TestAssignCheckAndRevokePartnerPortalRoleRoundTrip(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	externalID := "test-pp-" + uuid.NewString()
	createPartnerPortalUser(t, pool, externalID)

	assignment, err := pg.AssignPartnerPortalRole(ctx, externalID, "partner-admin", "admin-1")
	if err != nil {
		t.Fatalf("AssignPartnerPortalRole: %v", err)
	}
	if assignment.ExternalID != externalID || assignment.Role != "partner-admin" || assignment.GrantedBy != "admin-1" {
		t.Errorf("неожиданный assignment: %+v", assignment)
	}

	listed, err := pg.ListPartnerPortalAssignments(ctx, externalID)
	if err != nil {
		t.Fatalf("ListPartnerPortalAssignments: %v", err)
	}
	if len(listed) != 1 || listed[0].Role != "partner-admin" {
		t.Fatalf("ожидали одно активное назначение partner-admin, получили %+v", listed)
	}

	revoked, err := pg.RevokePartnerPortalRole(ctx, externalID, "partner-admin", "admin-2")
	if err != nil {
		t.Fatalf("RevokePartnerPortalRole: %v", err)
	}
	if !revoked {
		t.Fatalf("ожидали revoked=true для активного назначения")
	}

	listed, err = pg.ListPartnerPortalAssignments(ctx, externalID)
	if err != nil {
		t.Fatalf("ListPartnerPortalAssignments (после отзыва): %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("ожидали 0 активных назначений после отзыва, получили %+v", listed)
	}

	// Повторный отзыв — идемпотентно, revoked=false, не ошибка.
	revoked, err = pg.RevokePartnerPortalRole(ctx, externalID, "partner-admin", "admin-2")
	if err != nil {
		t.Fatalf("RevokePartnerPortalRole (повторный): %v", err)
	}
	if revoked {
		t.Errorf("повторный отзыв уже отозванной роли должен вернуть revoked=false")
	}
}

func TestAssignPartnerPortalRoleRejectsInvalidRole(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	externalID := "test-pp-" + uuid.NewString()
	createPartnerPortalUser(t, pool, externalID)

	_, err := pg.AssignPartnerPortalRole(context.Background(), externalID, "super-admin", "admin-1")
	if err != ErrInvalidPartnerPortalRole {
		t.Errorf("ожидали ErrInvalidPartnerPortalRole, получили %v", err)
	}
}

func TestAssignPartnerPortalRoleUnknownUserReturnsErrPartnerPortalUserNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	externalID := "test-pp-does-not-exist-" + uuid.NewString()

	_, err := pg.AssignPartnerPortalRole(context.Background(), externalID, "partner-admin", "admin-1")
	if err != ErrPartnerPortalUserNotFound {
		t.Errorf("ожидали ErrPartnerPortalUserNotFound (FK violation на iam.partner_portal_users), получили %v", err)
	}
}

func TestAssignPartnerPortalRoleWritesIdentityAudit(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	externalID := "test-pp-" + uuid.NewString()
	createPartnerPortalUser(t, pool, externalID)

	if _, err := pg.AssignPartnerPortalRole(ctx, externalID, "partner-viewer", "admin-1"); err != nil {
		t.Fatalf("AssignPartnerPortalRole: %v", err)
	}

	var count int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM iam.identity_audit
		WHERE actor = 'admin-1' AND action = 'PARTNER_PORTAL_ROLE_GRANTED' AND target = $1`,
		externalID+":partner-viewer").Scan(&count)
	if err != nil {
		t.Fatalf("readback identity_audit: %v", err)
	}
	if count != 1 {
		t.Errorf("ожидали ровно одну PARTNER_PORTAL_ROLE_GRANTED запись в iam.identity_audit, получили %d", count)
	}
}

func cleanupStaffAccount(t *testing.T, pool *pgxpool.Pool, externalID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM iam.identity_audit WHERE target = $1`, externalID)
		_, _ = pool.Exec(ctx, `DELETE FROM iam.staff_accounts WHERE external_id = $1`, externalID)
	})
}

func TestCreateAndVerifyStaffCredentialsRoundTrip(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	username := "test-" + uuid.NewString()
	cleanupStaffAccount(t, pool, username)

	account, err := pg.CreateStaffAccount(ctx, username, "correct-horse-battery-staple", "Test Person", "admin-1")
	if err != nil {
		t.Fatalf("CreateStaffAccount: %v", err)
	}
	if account.ExternalID != username || account.Username != username || !account.Active {
		t.Errorf("неожиданный account: %+v", account)
	}

	externalID, ok, err := pg.VerifyStaffCredentials(ctx, username, "correct-horse-battery-staple")
	if err != nil {
		t.Fatalf("VerifyStaffCredentials (correct password): %v", err)
	}
	if !ok || externalID != username {
		t.Fatalf("ожидали ok=true external_id=%q, получили ok=%v external_id=%q", username, ok, externalID)
	}

	_, ok, err = pg.VerifyStaffCredentials(ctx, username, "wrong-password")
	if err != nil {
		t.Fatalf("VerifyStaffCredentials (wrong password): %v", err)
	}
	if ok {
		t.Fatalf("ожидали ok=false для неверного пароля")
	}

	_, ok, err = pg.VerifyStaffCredentials(ctx, "does-not-exist-"+uuid.NewString(), "irrelevant")
	if err != nil {
		t.Fatalf("VerifyStaffCredentials (unknown username): %v", err)
	}
	if ok {
		t.Fatalf("ожидали ok=false для неизвестного username")
	}
}

func TestCreateStaffAccountDuplicateUsernameReturnsErrUsernameTaken(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	username := "test-" + uuid.NewString()
	cleanupStaffAccount(t, pool, username)

	if _, err := pg.CreateStaffAccount(ctx, username, "pw", "Test Person", "admin-1"); err != nil {
		t.Fatalf("CreateStaffAccount (first): %v", err)
	}
	if _, err := pg.CreateStaffAccount(ctx, username, "pw2", "Someone Else", "admin-1"); err != ErrUsernameTaken {
		t.Errorf("ожидали ErrUsernameTaken, получили %v", err)
	}
}

func TestDeactivateStaffAccountPreventsLogin(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	username := "test-" + uuid.NewString()
	cleanupStaffAccount(t, pool, username)

	if _, err := pg.CreateStaffAccount(ctx, username, "pw", "Test Person", "admin-1"); err != nil {
		t.Fatalf("CreateStaffAccount: %v", err)
	}

	deactivated, err := pg.DeactivateStaffAccount(ctx, username, "admin-2")
	if err != nil {
		t.Fatalf("DeactivateStaffAccount: %v", err)
	}
	if !deactivated {
		t.Fatalf("ожидали deactivated=true")
	}

	_, ok, err := pg.VerifyStaffCredentials(ctx, username, "pw")
	if err != nil {
		t.Fatalf("VerifyStaffCredentials (deactivated): %v", err)
	}
	if ok {
		t.Fatalf("деактивированный аккаунт не должен проходить VerifyStaffCredentials даже с верным паролем")
	}

	// Повторная деактивация — идемпотентно, deactivated=false, не ошибка.
	deactivated, err = pg.DeactivateStaffAccount(ctx, username, "admin-2")
	if err != nil {
		t.Fatalf("DeactivateStaffAccount (повторный): %v", err)
	}
	if deactivated {
		t.Errorf("повторная деактивация уже неактивного аккаунта должна вернуть deactivated=false")
	}
}

func TestListStaffAccountsActiveOnlyFilter(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	activeUsername := "test-active-" + uuid.NewString()
	inactiveUsername := "test-inactive-" + uuid.NewString()
	cleanupStaffAccount(t, pool, activeUsername)
	cleanupStaffAccount(t, pool, inactiveUsername)

	if _, err := pg.CreateStaffAccount(ctx, activeUsername, "pw", "Active Person", "admin-1"); err != nil {
		t.Fatalf("CreateStaffAccount (active): %v", err)
	}
	if _, err := pg.CreateStaffAccount(ctx, inactiveUsername, "pw", "Inactive Person", "admin-1"); err != nil {
		t.Fatalf("CreateStaffAccount (inactive): %v", err)
	}
	if _, err := pg.DeactivateStaffAccount(ctx, inactiveUsername, "admin-2"); err != nil {
		t.Fatalf("DeactivateStaffAccount: %v", err)
	}

	all, err := pg.ListStaffAccounts(ctx, false)
	if err != nil {
		t.Fatalf("ListStaffAccounts (all): %v", err)
	}
	found := map[string]bool{}
	for _, a := range all {
		found[a.Username] = true
	}
	if !found[activeUsername] || !found[inactiveUsername] {
		t.Fatalf("ожидали найти оба тестовых аккаунта без фильтра, нашли %v", found)
	}

	activeOnly, err := pg.ListStaffAccounts(ctx, true)
	if err != nil {
		t.Fatalf("ListStaffAccounts (active_only): %v", err)
	}
	found = map[string]bool{}
	for _, a := range activeOnly {
		found[a.Username] = true
	}
	if !found[activeUsername] {
		t.Errorf("активный тестовый аккаунт должен присутствовать в active_only=true")
	}
	if found[inactiveUsername] {
		t.Errorf("неактивный тестовый аккаунт НЕ должен присутствовать в active_only=true")
	}
}
