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
	cleanupAssignments(t, pool, externalID)

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
	cleanupAssignments(t, pool, externalID)

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
	cleanupAssignments(t, pool, externalID)
	cleanupAssignments(t, pool, otherID)

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
