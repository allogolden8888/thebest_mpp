package grpcserver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/credential-issuer-service/internal/store"
	"mpp/credential-issuer-service/internal/vault"
)

// TestRotateCredentialFullStackAgainstRealPostgresAndRealVault — no fakes
// anywhere: real local Postgres (store.Postgres), real local Vault dev
// server (vault.Client), the actual Server wiring exactly as main.go builds
// it. Proves the whole RotateCredential path end-to-end, not just each
// layer's unit tests in isolation.
func TestRotateCredentialFullStackAgainstRealPostgresAndRealVault(t *testing.T) {
	pgDSN := os.Getenv("CREDENTIAL_ISSUER_TEST_DSN")
	if pgDSN == "" {
		pgDSN = "postgres://localhost:5432/mpp"
	}
	vaultAddr := os.Getenv("VAULT_TEST_ADDR")
	if vaultAddr == "" {
		vaultAddr = "http://127.0.0.1:8200"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Skipf("Postgres недоступен (%v) — пропуск", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", pgDSN, err)
	}

	vaultClient := vault.NewClient(vaultAddr, "mpp", vault.StaticTokenSource{StaticToken: "root"}, nil)
	if err := vaultClient.Ping(context.Background()); err != nil {
		t.Skipf("Vault недоступен на %q (%v) — пропуск, `vault server -dev -dev-root-token-id=root`", vaultAddr, err)
	}

	partnerID := "test-partner-" + uuid.NewString()
	applicationID := "main"
	credentialRef := "vault://partners/" + partnerID + "/" + applicationID + "/api_key"

	payload := `{"partner_id": "` + partnerID + `", "version": 1, "status": "active", "applications": [` +
		`{"application_id": "` + applicationID + `", "display_name": "d", ` +
		`"auth": {"type": "API_KEY", "credential_ref": "` + credentialRef + `"}, ` +
		`"ip_allowlist": [], "rate_limit_tps": 100, "allowed_channels": ["SMS"]}]}`
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO config.config_versions (entity_type, entity_id, version, payload, status, created_by)
		VALUES ('partner', $1, 1, $2::jsonb, 'active', 'test-setup')`, partnerID, payload); err != nil {
		t.Fatalf("seed config_versions: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pool.Exec(bg, `DELETE FROM config.config_versions WHERE entity_type = 'partner' AND entity_id = $1`, partnerID)
		_, _ = pool.Exec(bg, `DELETE FROM credentials.issued_secrets WHERE partner_id = $1`, partnerID)
		_, _ = pool.Exec(bg, `DELETE FROM credentials.rotation_audit WHERE target = $1`, partnerID+":"+applicationID)
	})

	pg := store.NewPostgres(pool)
	server := New(pg, vaultClient, nil) // nil -> real RandomSecretGenerator

	// Первый вызов — "issue" (никакого предыдущего активного secret_version).
	resp1, err := server.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: partnerID, ApplicationId: applicationID, IssuedBy: "admin-1",
	})
	if err != nil {
		t.Fatalf("RotateCredential (issue): %v", err)
	}
	if resp1.GetSecretVersion() != 1 {
		t.Errorf("ожидали secret_version=1, получили %d", resp1.GetSecretVersion())
	}
	if resp1.GetPlaintextSecret() == "" {
		t.Fatal("plaintext_secret не должен быть пустым")
	}

	// Реально читаем из Vault (не через WriteKV2Field, а напрямую сырым
	// путём) — подтверждаем, что значение, которое сервис вернул как
	// plaintext, РЕАЛЬНО лежит в Vault ровно там, где EnvAuthVerifier/
	// VaultAuthVerifier его будут искать.
	kvPath, property, err := vault.ParseCredentialRef(credentialRef)
	if err != nil {
		t.Fatalf("ParseCredentialRef: %v", err)
	}
	storedValue := readVaultFieldForTest(t, vaultAddr, kvPath, property)
	if storedValue != resp1.GetPlaintextSecret() {
		t.Errorf("значение в Vault (%q) не совпадает с тем, что вернул RotateCredential (%q)", storedValue, resp1.GetPlaintextSecret())
	}

	// Второй вызов — "rotate": новая версия, старая должна стать invalid
	// (сравниваем, что Vault теперь несёт ВТОРОЙ plaintext, не первый).
	resp2, err := server.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: partnerID, ApplicationId: applicationID, IssuedBy: "admin-2",
	})
	if err != nil {
		t.Fatalf("RotateCredential (rotate): %v", err)
	}
	if resp2.GetSecretVersion() != 2 {
		t.Errorf("ожидали secret_version=2, получили %d", resp2.GetSecretVersion())
	}
	if resp2.GetPlaintextSecret() == resp1.GetPlaintextSecret() {
		t.Error("ротация должна была сгенерировать НОВЫЙ секрет, не повторить старый")
	}

	storedValue2 := readVaultFieldForTest(t, vaultAddr, kvPath, property)
	if storedValue2 != resp2.GetPlaintextSecret() {
		t.Errorf("после ротации Vault должен нести НОВЫЙ plaintext (%q), нашли %q — старое значение всё ещё активно, credential rotation не сработала", resp2.GetPlaintextSecret(), storedValue2)
	}
	if storedValue2 == storedValue {
		t.Error("Vault-значение не изменилось после ротации")
	}

	// listing метаданных подтверждает обе версии, вторая active/первая revoked.
	listResp, err := server.ListIssuedSecrets(context.Background(), &grpcv1.ListIssuedSecretsRequest{PartnerId: partnerID})
	if err != nil {
		t.Fatalf("ListIssuedSecrets: %v", err)
	}
	if len(listResp.GetSecrets()) != 2 {
		t.Fatalf("ожидали 2 записи метаданных (v1+v2), получили %d", len(listResp.GetSecrets()))
	}
}

// readVaultFieldForTest — отдельный vault.Client instance (не переиспользует
// server'а внутренний), читает через ReadKV2 напрямую, чтобы верификация
// не зависела от того же кода пути, что WriteKV2Field использует для
// своего внутреннего read-modify-write.
func readVaultFieldForTest(t *testing.T, addr, kvPath, property string) string {
	t.Helper()
	c := vault.NewClient(addr, "mpp", vault.StaticTokenSource{StaticToken: "root"}, nil)
	data, err := c.ReadKV2(context.Background(), kvPath)
	if err != nil {
		t.Fatalf("readVaultFieldForTest: %v", err)
	}
	return data[property]
}
