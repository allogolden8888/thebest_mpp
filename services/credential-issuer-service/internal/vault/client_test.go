package vault

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testAddr — реальный локальный Vault dev-server (`vault server -dev`),
// тот же обход, что Postgres/Redis в остальной сессии: реальный I/O там,
// где окружение позволяет, честно пропущено (t.Skip), где нет.
func testAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("VAULT_TEST_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8200"
	}
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	if err := client.Ping(context.Background()); err != nil {
		t.Skipf("локальный Vault недоступен на %q (%v) — пропуск, `vault server -dev -dev-root-token-id=root`", addr, err)
	}
	return addr
}

func TestParseCredentialRef(t *testing.T) {
	cases := []struct {
		ref      string
		wantPath string
		wantProp string
		wantErr  bool
	}{
		{"vault://partners/click_uz/main/api_key", "partners/click_uz/main", "api_key", false},
		{"vault://partners/click_uz/main/nested/api_key", "partners/click_uz/main/nested", "api_key", false},
		{"not-a-vault-ref", "", "", true},
		{"vault://", "", "", true},
		{"vault://no-slash-at-all", "", "", true},
	}
	for _, tc := range cases {
		path, prop, err := ParseCredentialRef(tc.ref)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseCredentialRef(%q): ожидали ошибку, получили path=%q prop=%q", tc.ref, path, prop)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCredentialRef(%q): неожиданная ошибка: %v", tc.ref, err)
			continue
		}
		if path != tc.wantPath || prop != tc.wantProp {
			t.Errorf("ParseCredentialRef(%q) = (%q, %q), хотели (%q, %q)", tc.ref, path, prop, tc.wantPath, tc.wantProp)
		}
	}
}

func TestWriteKV2FieldRoundTripAgainstRealVault(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	ctx := context.Background()
	kvPath := "partners/test-partner/test-app-" + time.Now().Format("150405.000000000")

	if err := client.WriteKV2Field(ctx, kvPath, "api_key", "secret-v1"); err != nil {
		t.Fatalf("WriteKV2Field (первая запись): %v", err)
	}

	data, err := client.readKV2Raw(ctx, kvPath)
	if err != nil {
		t.Fatalf("readKV2Raw: %v", err)
	}
	if data["api_key"] != "secret-v1" {
		t.Errorf("ожидали api_key=secret-v1, получили %v", data)
	}
}

func TestWriteKV2FieldMergesNotOverwritesOtherFields(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	ctx := context.Background()
	kvPath := "partners/test-partner/test-merge-" + time.Now().Format("150405.000000000")

	if err := client.WriteKV2Field(ctx, kvPath, "field_a", "value-a"); err != nil {
		t.Fatalf("WriteKV2Field (field_a): %v", err)
	}
	if err := client.WriteKV2Field(ctx, kvPath, "field_b", "value-b"); err != nil {
		t.Fatalf("WriteKV2Field (field_b): %v", err)
	}

	data, err := client.readKV2Raw(ctx, kvPath)
	if err != nil {
		t.Fatalf("readKV2Raw: %v", err)
	}
	if data["field_a"] != "value-a" || data["field_b"] != "value-b" {
		t.Errorf("запись field_b не должна была стереть field_a — получили %v", data)
	}
}

func TestWriteKV2FieldRotationOverwritesSameField(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	ctx := context.Background()
	kvPath := "partners/test-partner/test-rotate-" + time.Now().Format("150405.000000000")

	if err := client.WriteKV2Field(ctx, kvPath, "api_key", "secret-v1"); err != nil {
		t.Fatalf("WriteKV2Field (v1): %v", err)
	}
	if err := client.WriteKV2Field(ctx, kvPath, "api_key", "secret-v2"); err != nil {
		t.Fatalf("WriteKV2Field (v2, ротация): %v", err)
	}

	data, err := client.readKV2Raw(ctx, kvPath)
	if err != nil {
		t.Fatalf("readKV2Raw: %v", err)
	}
	if data["api_key"] != "secret-v2" {
		t.Errorf("ротация должна была перезаписать значение — получили %v, ожидали secret-v2", data["api_key"])
	}
}

func TestPingAgainstRealVault(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	if err := client.Ping(context.Background()); err != nil {
		t.Errorf("Ping: %v", err)
	}
}

func TestPingRejectsUnreachableAddr(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "mpp", StaticTokenSource{StaticToken: "root"}, &http.Client{Timeout: 500 * time.Millisecond})
	if err := client.Ping(context.Background()); err == nil {
		t.Error("ожидали ошибку от заведомо недоступного адреса")
	}
}

// TestKubernetesAuthTokenSourceLoginAndCache — фейковый Vault-сервер
// (httptest), поскольку реального Kubernetes ServiceAccount JWT/kubernetes
// auth method здесь нет (это локальная песочница, не k8s-кластер) — тот же
// класс обхода, что internal/signals/prometheus_test.go у
// execution-control-service (реальный HTTP round-trip против httptest.Server,
// не мок транспорта).
func TestKubernetesAuthTokenSourceLoginAndCache(t *testing.T) {
	loginCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/kubernetes/login" {
			t.Errorf("неожиданный путь: %s", r.URL.Path)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["role"] != "credential-issuer-service" {
			t.Errorf("неожиданная role в login-запросе: %q", body["role"])
		}
		if body["jwt"] != "fake-service-account-jwt" {
			t.Errorf("неожиданный jwt в login-запросе: %q", body["jwt"])
		}
		loginCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"auth": map[string]any{
				"client_token":   "vault-token-from-login",
				"lease_duration": 3600,
			},
		})
	}))
	defer server.Close()

	jwtPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(jwtPath, []byte("fake-service-account-jwt\n"), 0o600); err != nil {
		t.Fatalf("запись фейкового JWT: %v", err)
	}

	source := &KubernetesAuthTokenSource{
		Addr:       server.URL,
		Role:       "credential-issuer-service",
		JWTPath:    jwtPath,
		HTTPClient: server.Client(),
	}

	token, err := source.Token(context.Background())
	if err != nil {
		t.Fatalf("Token (первый вызов): %v", err)
	}
	if token != "vault-token-from-login" {
		t.Errorf("неожиданный токен: %q", token)
	}

	// Второй вызов ДО истечения TTL не должен снова логиниться — кеш.
	if _, err := source.Token(context.Background()); err != nil {
		t.Fatalf("Token (второй вызов): %v", err)
	}
	if loginCalls != 1 {
		t.Errorf("ожидали 1 login-вызов (кеш работает), получили %d", loginCalls)
	}
}

func TestKubernetesAuthTokenSourceRelogsAfterExpiry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"auth": map[string]any{
				"client_token":   "short-lived-token",
				"lease_duration": 1, // 1с — тривиально истечёт быстрее renewMargin (30с)
			},
		})
	}))
	defer server.Close()

	jwtPath := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(jwtPath, []byte("fake-jwt"), 0o600)

	source := &KubernetesAuthTokenSource{
		Addr: server.URL, Role: "r", JWTPath: jwtPath, HTTPClient: server.Client(),
	}
	source.cachedToken = "short-lived-token"
	source.expiresAt = time.Now().Add(1 * time.Second) // уже внутри renewMargin

	token, err := source.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token != "short-lived-token" {
		t.Errorf("неожиданный токен после релогина: %q", token)
	}
}

func TestKubernetesAuthTokenSourceMissingJWTFileReturnsError(t *testing.T) {
	source := &KubernetesAuthTokenSource{
		Addr: "http://unused", Role: "r", JWTPath: "/does/not/exist", HTTPClient: http.DefaultClient,
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Error("ожидали ошибку при отсутствующем JWT-файле")
	}
}

func TestKubernetesAuthTokenSourceLoginRejectedReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
	}))
	defer server.Close()

	jwtPath := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(jwtPath, []byte("fake-jwt"), 0o600)

	source := &KubernetesAuthTokenSource{
		Addr: server.URL, Role: "r", JWTPath: jwtPath, HTTPClient: server.Client(),
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Error("ожидали ошибку при отклонённом login")
	}
}
