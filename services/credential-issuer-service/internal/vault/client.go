// Package vault — первый реальный Vault-клиент в этой сессии (до Фазы 1
// ни один сервис не читал/не писал Vault по-настоящему, только
// infra/secrets/generate_external_secrets.py генерировал ExternalSecret
// YAML, который ЧИТАЕТ Vault через External Secrets Operator, не сам
// сервис напрямую). KV v2 API (не клиентская библиотека
// github.com/hashicorp/vault/api — прямой HTTP, тот же принцип
// минимализма, что у остальных "маленьких Go control-plane" сервисов этой
// сессии: используем http.Client, не тянем полноценный Vault SDK ради
// трёх операций).
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

// TokenSource — способ получить действующий Vault client token.
// Production: KubernetesAuthTokenSource (реальный login через k8s auth
// method, infra/terraform/vault-secrets.tf vault_kubernetes_auth_backend_role).
// Local/dev/test: StaticTokenSource (VAULT_TOKEN напрямую, тот же escape
// hatch, что `vault` CLI сам поддерживает через переменную окружения —
// не изобретаем новую конвенцию).
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

type StaticTokenSource struct {
	StaticToken string
}

func (s StaticTokenSource) Token(_ context.Context) (string, error) {
	return s.StaticToken, nil
}

// KubernetesAuthTokenSource — обменивает projected ServiceAccount JWT
// (jwtPath) на Vault client token через POST /v1/auth/kubernetes/login
// {"role": role, "jwt": <файл>}. Кеширует токен до истечения TTL с запасом
// (renewMargin) — не логинится заново на каждый вызов.
type KubernetesAuthTokenSource struct {
	Addr       string
	Role       string
	JWTPath    string
	HTTPClient *http.Client

	mu          sync.Mutex
	cachedToken string
	expiresAt   time.Time
}

// renewMargin — обновляем токен за это время до истечения его TTL, не
// впритык — та же логика, что RedisTokenBucket's refill: избегаем узкого
// окна, где токен формально ещё жив, но истечёт до того, как запрос,
// использующий его, дойдёт до Vault.
const renewMargin = 30 * time.Second

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

// Client — тонкая обёртка над Vault KV v2 HTTP API, mount "mpp"
// (infra/terraform/vault-secrets.tf vault_mount.mpp), тот же mount, что
// уже читает ExternalSecret (infra/secrets/generate_external_secrets.py
// VAULT_KV_MOUNT).
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

// ParseCredentialRef — `vault://partners/click_uz/main/api_key` ->
// (`partners/click_uz/main`, `api_key`). Байт-в-байт та же логика, что
// infra/secrets/generate_external_secrets.py::vault_kv_path_and_property
// (Python) — ОБЕ реализации должны совпадать, иначе путь, по которому
// credential-issuer-service пишет, разойдётся с путём, который
// ExternalSecret/VaultAuthVerifier читают.
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

// WriteKV2Field — пишет ОДНО поле по KV v2 пути, СЛИВАЯ его с уже
// существующими полями по этому же пути (read-modify-write), а не
// перезаписывая весь secret целиком. Критично: один KV-путь
// (`partners/<partner_id>/<application_id>`) может нести несколько полей
// (например несколько credential_ref разных auth-механизмов на одно
// приложение в будущем) — слепой PUT данных с одним полем стёр бы остальные.
func (c *Client) WriteKV2Field(ctx context.Context, kvPath, property, value string) error {
	existing, err := c.readKV2Raw(ctx, kvPath)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("vault: чтение перед записью %s: %w", kvPath, err)
	}
	if existing == nil {
		existing = map[string]any{}
	}
	existing[property] = value

	body, err := json.Marshal(map[string]any{"data": existing})
	if err != nil {
		return fmt.Errorf("vault: маршалинг write-запроса: %w", err)
	}

	token, err := c.TokenSource.Token(ctx)
	if err != nil {
		return fmt.Errorf("vault: получение токена: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dataURL(kvPath), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("vault: сборка write-запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("vault: write-запрос к %s: %w", kvPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vault: write отклонён, статус %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

type notFoundError struct{ path string }

func (e notFoundError) Error() string { return fmt.Sprintf("vault: путь %s не найден", e.path) }

func isNotFound(err error) bool {
	_, ok := err.(notFoundError)
	return ok
}

// ReadKV2 — читает все поля по KV v2 пути. Экспортирован (не только
// внутренний readKV2Raw, вызываемый WriteKV2Field для read-modify-write)
// ради независимой верификации в тестах и на случай будущего "показать
// текущий статус secret" сценария — сам credential-issuer-service его в
// рабочем пути не использует (пишет, не читает секреты обратно).
func (c *Client) ReadKV2(ctx context.Context, kvPath string) (map[string]string, error) {
	raw, err := c.readKV2Raw(ctx, kvPath)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out, nil
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

// Ping — GET /v1/sys/health для /readyz. Не требует токена (sys/health —
// одна из немногих Vault-ручек, открытых без аутентификации, специально
// для health-проб).
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
	// 200 = unsealed+active, 429 = unsealed+standby (тоже ОК для наших
	// целей — standby-реплика всё ещё обслуживает read/write через
	// HA-прокси в реальном кластере), остальное — реальная проблема.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTooManyRequests {
		return fmt.Errorf("vault: health-статус %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) dataURL(kvPath string) string {
	return fmt.Sprintf("%s/v1/%s/data/%s", c.Addr, c.Mount, kvPath)
}
