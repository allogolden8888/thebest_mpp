// Package vault — read-only Vault KV v2 HTTP client for operator-http-gateway
// (BACKOFFICE_ROADMAP.md P0#1, per-operator webhook credentials). Byte-for-
// byte mirror of services/credential-issuer-service/internal/vault/client.go
// and services/partner-rest-receiver/src/vault_auth.rs (three independent
// implementations of the same convention — TokenSource/ParseCredentialRef/
// KV v2 read must agree, or the path this service reads diverges from the
// path an operator's credential was actually written under).
//
// Unlike credential-issuer-service, this client never WRITES a secret — this
// service only ever needs to READ the current webhook token an operator was
// provisioned with (out-of-band, e.g. `vault kv put mpp/operators/<id>
// webhook_bearer=...` — see README "Что НЕ реализовано" for why there is no
// RotateCredential-equivalent for operators yet), so WriteKV2Field/
// read-modify-write merging (needed there to avoid clobbering sibling
// fields) has no counterpart here.
package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// TokenSource — how to obtain a valid Vault client token. Production:
// KubernetesAuthTokenSource (real login via the k8s auth method). Local/
// dev/test: StaticTokenSource (VAULT_TOKEN directly, the same escape hatch
// `vault` CLI itself supports via env var).
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

type StaticTokenSource struct {
	StaticToken string
}

func (s StaticTokenSource) Token(_ context.Context) (string, error) {
	return s.StaticToken, nil
}

// renewMargin — refresh this long before the token's TTL actually expires,
// not right at the edge — same value/reasoning as credential-issuer-
// service's renewMargin.
const renewMargin = 30 * time.Second

// KubernetesAuthTokenSource — exchanges the pod's projected ServiceAccount
// JWT for a Vault client token via POST /v1/auth/kubernetes/login
// {"role","jwt"}, caches until renewMargin before expiry.
type KubernetesAuthTokenSource struct {
	Addr       string
	Role       string
	JWTPath    string
	HTTPClient *http.Client

	mu          sync.Mutex
	cachedToken string
	expiresAt   time.Time
}

func (k *KubernetesAuthTokenSource) Token(ctx context.Context) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.cachedToken != "" && time.Now().Before(k.expiresAt.Add(-renewMargin)) {
		return k.cachedToken, nil
	}

	jwtBytes, err := os.ReadFile(k.JWTPath)
	if err != nil {
		return "", fmt.Errorf("vault: чтение ServiceAccount JWT (%s): %w", k.JWTPath, err)
	}

	body, err := json.Marshal(map[string]string{
		"role": k.Role,
		"jwt":  strings.TrimSpace(string(jwtBytes)),
	})
	if err != nil {
		return "", fmt.Errorf("vault: маршалинг login-запроса: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, k.Addr+"/v1/auth/kubernetes/login", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("vault: сборка login-запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := k.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("vault: login-запрос к %s: %w", k.Addr, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("vault: чтение тела login-ответа: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("vault: login отклонён, статус %d: %s", resp.StatusCode, string(respBody))
	}

	var loginResp struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(respBody, &loginResp); err != nil {
		return "", fmt.Errorf("vault: разбор login-ответа: %w", err)
	}
	if loginResp.Auth.ClientToken == "" {
		return "", fmt.Errorf("vault: login-ответ без auth.client_token: %s", string(respBody))
	}

	k.cachedToken = loginResp.Auth.ClientToken
	k.expiresAt = time.Now().Add(time.Duration(loginResp.Auth.LeaseDuration) * time.Second)
	return k.cachedToken, nil
}

// Client — thin wrapper over the Vault KV v2 HTTP API, mount "mpp"
// (infra/terraform/vault-secrets.tf vault_mount.mpp) — same mount as
// credential-issuer-service/partner-rest-receiver.
type Client struct {
	Addr        string
	Mount       string
	TokenSource TokenSource
	HTTPClient  *http.Client
}

func NewClient(addr, mount string, tokenSource TokenSource, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{Addr: addr, Mount: mount, TokenSource: tokenSource, HTTPClient: httpClient}
}

// ParseCredentialRef — `vault://operators/beeline_uz/webhook_bearer` ->
// (`operators/beeline_uz`, `webhook_bearer`). Byte-for-byte the same logic
// as credential-issuer-service's vault.ParseCredentialRef and Rust
// parse_credential_ref (partner-rest-receiver/src/vault_auth.rs) — all three
// must agree, or the path a credential was written under diverges from the
// path this service reads.
func ParseCredentialRef(credentialRef string) (kvPath, property string, err error) {
	const prefix = "vault://"
	if !strings.HasPrefix(credentialRef, prefix) {
		return "", "", fmt.Errorf("vault: credential_ref %q не начинается с %q", credentialRef, prefix)
	}
	path := strings.TrimPrefix(credentialRef, prefix)
	idx := strings.LastIndex(path, "/")
	if idx <= 0 || idx == len(path)-1 {
		return "", "", fmt.Errorf("vault: credential_ref %q не имеет формы path/property", credentialRef)
	}
	return path[:idx], path[idx+1:], nil
}

type notFoundError struct{ path string }

func (e notFoundError) Error() string { return fmt.Sprintf("vault: путь %s не найден", e.path) }

// ReadKV2Property — GET по KV v2 пути, возвращает одно поле. Возвращает
// ошибку и на отсутствующий путь, и на отсутствующее в существующем пути
// поле — вызывающая сторона (webhookauth.Lookup) обязана трактовать ЛЮБУЮ
// ошибку отсюда как "нет действующего секрета для этого operator_id", не
// пытаться отличать 404 от "путь есть, поля нет" — тот же fail-closed
// принцип, что VaultAuthVerifier (partner-rest-receiver/src/vault_auth.rs).
func (c *Client) ReadKV2Property(ctx context.Context, kvPath, property string) (string, error) {
	raw, err := c.readKV2Raw(ctx, kvPath)
	if err != nil {
		return "", err
	}
	value, ok := raw[property]
	if !ok {
		return "", fmt.Errorf("vault: поле %s отсутствует по пути %s", property, kvPath)
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("vault: поле %s по пути %s не строка", property, kvPath)
	}
	return s, nil
}

func (c *Client) readKV2Raw(ctx context.Context, kvPath string) (map[string]any, error) {
	token, err := c.TokenSource.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault: получение токена: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dataURL(kvPath), nil)
	if err != nil {
		return nil, fmt.Errorf("vault: сборка read-запроса: %w", err)
	}
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault: read-запрос к %s: %w", kvPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, notFoundError{path: kvPath}
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("vault: чтение тела read-ответа: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault: read отклонён, статус %d: %s", resp.StatusCode, string(respBody))
	}

	var parsed struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("vault: разбор read-ответа: %w", err)
	}
	return parsed.Data.Data, nil
}

// Ping — GET /v1/sys/health для /readyz, без токена (одна из немногих
// Vault-ручек, открытых без аутентификации специально для health-проб).
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Addr+"/v1/sys/health", nil)
	if err != nil {
		return fmt.Errorf("vault: сборка health-запроса: %w", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("vault: health-запрос к %s: %w", c.Addr, err)
	}
	defer resp.Body.Close()
	// 200 = unsealed+active, 429 = unsealed+standby (тоже ОК — standby-реплика
	// всё ещё обслуживает трафик через HA-прокси в реальном кластере).
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTooManyRequests {
		return fmt.Errorf("vault: health-статус %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) dataURL(kvPath string) string {
	return fmt.Sprintf("%s/v1/%s/data/%s", c.Addr, c.Mount, kvPath)
}
