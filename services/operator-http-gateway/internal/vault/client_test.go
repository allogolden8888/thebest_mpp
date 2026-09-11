package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testAddr — реальный локальный Vault dev-server (`vault server -dev`), тот
// же обход, что credential-issuer-service/internal/vault/client_test.go:
// реальный I/O там, где окружение позволяет, честно пропущено (t.Skip),
// где нет.
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
		{"vault://operators/beeline_uz/webhook_bearer", "operators/beeline_uz", "webhook_bearer", false},
		{"vault://operators/beeline_uz/nested/webhook_bearer", "operators/beeline_uz/nested", "webhook_bearer", false},
		{"not-a-vault-ref", "", "", true},
		{"vault://", "", "", true},
		{"vault://no-slash-at-all", "", "", true},
		{"vault://operators/beeline_uz/", "", "", true},
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

func TestReadKV2PropertyAgainstRealVault(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	ctx := context.Background()
	kvPath := "operators/test-operator-" + time.Now().Format("150405.000000000")

	writeRawKV2(t, addr, kvPath, map[string]string{"webhook_bearer": "real-secret-value"})

	got, err := client.ReadKV2Property(ctx, kvPath, "webhook_bearer")
	if err != nil {
		t.Fatalf("ReadKV2Property: %v", err)
	}
	if got != "real-secret-value" {
		t.Errorf("получили %q, ожидали real-secret-value", got)
	}
}

func TestReadKV2PropertyMissingPathFailsClosed(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	if _, err := client.ReadKV2Property(context.Background(), "operators/does-not-exist-at-all", "webhook_bearer"); err == nil {
		t.Error("ожидали ошибку для несуществующего пути")
	}
}

func TestReadKV2PropertyMissingFieldOnExistingPathFailsClosed(t *testing.T) {
	addr := testAddr(t)
	client := NewClient(addr, "mpp", StaticTokenSource{StaticToken: "root"}, nil)
	kvPath := "operators/test-operator-missing-field-" + time.Now().Format("150405.000000000")
	writeRawKV2(t, addr, kvPath, map[string]string{"other_field": "irrelevant"})

	if _, err := client.ReadKV2Property(context.Background(), kvPath, "webhook_bearer"); err == nil {
		t.Error("ожидали ошибку для отсутствующего поля")
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

func TestReadKV2PropertyUnreachableAddrFailsClosedNotPanics(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "mpp", StaticTokenSource{StaticToken: "root"}, &http.Client{Timeout: 500 * time.Millisecond})
	if _, err := client.ReadKV2Property(context.Background(), "operators/beeline_uz", "webhook_bearer"); err == nil {
		t.Error("ожидали ошибку от заведомо недоступного адреса")
	}
}

// writeRawKV2 — прямой write через root token, независимый от read-пути,
// который тестируется (тот же приём, что credential-issuer-service's
// client_test.go верифицирует запись отдельным ReadKV2, здесь наоборот:
// пишем напрямую мимо тестируемого кода, читаем через него).
func writeRawKV2(t *testing.T, addr, kvPath string, fields map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"data": fields})
	if err != nil {
		t.Fatalf("marshal seed data: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, addr+"/v1/mpp/data/"+kvPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build seed request: %v", err)
	}
	req.Header.Set("X-Vault-Token", "root")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("seed write request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("seed write отклонён, статус %d", resp.StatusCode)
	}
}

// ---- TokenSource (Kubernetes login) — фейковый сервер, тот же класс
// обхода, что credential-issuer-service/internal/vault/client_test.go
// (реального k8s ServiceAccount JWT/kubernetes auth method здесь нет).

func TestKubernetesAuthTokenSourceLoginAndCache(t *testing.T) {
	loginCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/kubernetes/login" {
			t.Errorf("неожиданный путь: %s", r.URL.Path)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["role"] != "operator-webhook-credential-readers" {
			t.Errorf("неожиданная role в login-запросе: %q", body["role"])
		}
		if body["jwt"] != "fake-service-account-jwt" {
			t.Errorf("неожиданный jwt в login-запросе: %q", body["jwt"])
		}
		loginCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"auth": map[string]any{"client_token": "vault-token-from-login", "lease_duration": 3600},
		})
	}))
	defer server.Close()

	jwtPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(jwtPath, []byte("fake-service-account-jwt\n"), 0o600); err != nil {
		t.Fatalf("запись фейкового JWT: %v", err)
	}

	source := &KubernetesAuthTokenSource{
		Addr:       server.URL,
		Role:       "operator-webhook-credential-readers",
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

	if _, err := source.Token(context.Background()); err != nil {
		t.Fatalf("Token (второй вызов): %v", err)
	}
	if loginCalls != 1 {
		t.Errorf("ожидали 1 login-вызов (кеш работает), получили %d", loginCalls)
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
