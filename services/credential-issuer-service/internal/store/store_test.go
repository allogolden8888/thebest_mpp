package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool — реальный локальный PostgreSQL 17 (brew), migrations/V027__credentials.sql
// уже применена в этой песочнице. Пропускается, если Postgres недоступен.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CREDENTIAL_ISSUER_TEST_DSN")
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

func insertTestPartnerConfig(t *testing.T, pool *pgxpool.Pool, partnerID, applicationID, credentialRef string) {
	t.Helper()
	ctx := context.Background()
	payload := `{"partner_id": "` + partnerID + `", "version": 1, "status": "active", "applications": [` +
		`{"application_id": "` + applicationID + `", "display_name": "d", ` +
		`"auth": {"type": "API_KEY", "credential_ref": "` + credentialRef + `"}, ` +
		`"ip_allowlist": [], "rate_limit_tps": 100, "allowed_channels": ["SMS"]}]}`
	_, err := pool.Exec(ctx, `
		INSERT INTO config.config_versions (entity_type, entity_id, version, payload, status, created_by)
		VALUES ('partner', $1, 1, $2::jsonb, 'active', 'test-setup')`, partnerID, payload)
	if err != nil {
		t.Fatalf("insertTestPartnerConfig: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM config.config_versions WHERE entity_type = 'partner' AND entity_id = $1`, partnerID)
	})
}

func cleanupCredentials(t *testing.T, pool *pgxpool.Pool, partnerID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM credentials.issued_secrets WHERE partner_id = $1`, partnerID)
		_, _ = pool.Exec(ctx, `DELETE FROM credentials.rotation_audit WHERE target LIKE $1`, partnerID+":%")
	})
}

func TestLookupCredentialRefFindsRealPartnerConfig(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	partnerID := "test-partner-" + uuid.NewString()
	insertTestPartnerConfig(t, pool, partnerID, "main", "vault://partners/"+partnerID+"/main/api_key")

	ref, err := pg.LookupCredentialRef(context.Background(), partnerID, "main")
	if err != nil {
		t.Fatalf("LookupCredentialRef: %v", err)
	}
	want := "vault://partners/" + partnerID + "/main/api_key"
	if ref != want {
		t.Errorf("LookupCredentialRef = %q, хотели %q", ref, want)
	}
}

func TestLookupCredentialRefUnknownPartnerReturnsErrPartnerNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)

	_, err := pg.LookupCredentialRef(context.Background(), "does-not-exist-"+uuid.NewString(), "main")
	if err != ErrPartnerNotFound {
		t.Errorf("ожидали ErrPartnerNotFound, получили %v", err)
	}
}

func TestLookupCredentialRefUnknownApplicationReturnsErrApplicationNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	partnerID := "test-partner-" + uuid.NewString()
	insertTestPartnerConfig(t, pool, partnerID, "main", "vault://partners/"+partnerID+"/main/api_key")

	_, err := pg.LookupCredentialRef(context.Background(), partnerID, "does-not-exist")
	if err != ErrApplicationNotFound {
		t.Errorf("ожидали ErrApplicationNotFound, получили %v", err)
	}
}

func TestRecordRotationFirstIssueThenRotateVersionsIncrementAndPreviousRevoked(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	partnerID := "test-partner-" + uuid.NewString()
	cleanupCredentials(t, pool, partnerID)
	credentialRef := "vault://partners/" + partnerID + "/main/api_key"

	first, err := pg.RecordRotation(ctx, partnerID, "main", credentialRef, "admin-1")
	if err != nil {
		t.Fatalf("RecordRotation (первый выпуск): %v", err)
	}
	if first.SecretVersion != 1 || first.Status != "active" {
		t.Fatalf("первый выпуск: ожидали version=1 status=active, получили %+v", first)
	}

	second, err := pg.RecordRotation(ctx, partnerID, "main", credentialRef, "admin-2")
	if err != nil {
		t.Fatalf("RecordRotation (ротация): %v", err)
	}
	if second.SecretVersion != 2 || second.Status != "active" {
		t.Fatalf("ротация: ожидали version=2 status=active, получили %+v", second)
	}

	secrets, err := pg.ListIssuedSecrets(ctx, partnerID)
	if err != nil {
		t.Fatalf("ListIssuedSecrets: %v", err)
	}
	if len(secrets) != 2 {
		t.Fatalf("ожидали 2 строки (v1 revoked + v2 active), получили %d", len(secrets))
	}
	byVersion := map[int32]IssuedSecret{}
	for _, s := range secrets {
		byVersion[s.SecretVersion] = s
	}
	if byVersion[1].Status != "revoked" {
		t.Errorf("v1 должна была стать revoked после ротации, получили status=%q", byVersion[1].Status)
	}
	if byVersion[2].Status != "active" {
		t.Errorf("v2 должна быть active, получили status=%q", byVersion[2].Status)
	}
}

func TestRecordRotationWritesRotationAuditWithCorrectAction(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	partnerID := "test-partner-" + uuid.NewString()
	cleanupCredentials(t, pool, partnerID)
	credentialRef := "vault://partners/" + partnerID + "/main/api_key"

	if _, err := pg.RecordRotation(ctx, partnerID, "main", credentialRef, "admin-1"); err != nil {
		t.Fatalf("RecordRotation (первый): %v", err)
	}
	if _, err := pg.RecordRotation(ctx, partnerID, "main", credentialRef, "admin-2"); err != nil {
		t.Fatalf("RecordRotation (второй): %v", err)
	}

	var issuedCount, rotatedCount int
	target := partnerID + ":main"
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM credentials.rotation_audit WHERE target = $1 AND action = 'ISSUED'`, target).Scan(&issuedCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM credentials.rotation_audit WHERE target = $1 AND action = 'ROTATED'`, target).Scan(&rotatedCount)
	if issuedCount != 1 {
		t.Errorf("ожидали ровно 1 ISSUED запись, получили %d", issuedCount)
	}
	if rotatedCount != 1 {
		t.Errorf("ожидали ровно 1 ROTATED запись, получили %d", rotatedCount)
	}
}
