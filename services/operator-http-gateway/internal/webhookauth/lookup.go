// Package webhookauth composes internal/opconfig (Configuration Redis:
// operator_id -> credential_ref) and internal/vault (credential_ref ->
// actual secret value) into a single webhook.ExpectedTokenLookup, exactly
// the same two-hop shape as partner-rest-receiver's VaultAuthVerifier
// (src/vault_auth.rs): partner.schema.json's credential_ref is only ever a
// POINTER to a Vault path, never the secret itself — the config layer never
// carries the value, only the reference.
//
// TTL cache (default 30s, same order of magnitude as
// KubernetesAuthTokenSource's renewMargin) on top of the resolved secret
// VALUE — same reasoning as VaultAuthVerifier's cache: this lookup runs on
// every single inbound DLR webhook (a real, latency-sensitive hot path, not
// an admin action), so a network round trip to Vault on every request would
// be both added latency and unnecessary Vault load at meaningful TPS. A
// cache hit makes zero network calls at all (neither Redis nor Vault) —
// see lookup_test.go.
package webhookauth

import (
	"context"
	"sync"
	"time"

	"mpp/operator-http-gateway/internal/vault"
)

// ConfigReader — минимальный интерфейс от opconfig.Reader (тестируемость
// без реального Redis).
type ConfigReader interface {
	WebhookCredentialRef(ctx context.Context, operatorID string) (credentialRef string, found bool, err error)
}

// VaultReader — минимальный интерфейс от vault.Client.
type VaultReader interface {
	ReadKV2Property(ctx context.Context, kvPath, property string) (string, error)
}

// DefaultCacheTTL — 30с, тот же порядок величины, что
// partner-rest-receiver's VaultAuthVerifier DEFAULT_CACHE_TTL: на порядок
// больше одного HTTP-запроса, на порядок меньше "заметная задержка
// распространения ротации" секрета.
const DefaultCacheTTL = 30 * time.Second

type cacheEntry struct {
	token     string
	fetchedAt time.Time
}

// Lookup — webhook.ExpectedTokenLookup, реализованный через
// ConfigReader+VaultReader. Fail-closed на любую ошибку (Redis
// недоступен, Vault недоступен, credential_ref не парсится, поле
// отсутствует) — тот же принцип, что VaultAuthVerifier
// (partner-rest-receiver) и iam-service::CheckPermission "fail-closed по
// контракту": сервис, проверяющий credentials, не должен по умолчанию
// открываться, когда его зависимость недоступна.
type Lookup struct {
	Config ConfigReader
	Vault  VaultReader
	TTL    time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
}

func New(config ConfigReader, vaultClient VaultReader) *Lookup {
	return &Lookup{Config: config, Vault: vaultClient, TTL: DefaultCacheTTL, cache: make(map[string]cacheEntry)}
}

// ExpectedToken — webhook.ExpectedTokenLookup. found=false для любой из:
// оператор не сконфигурирован в Configuration Redis, сконфигурирован без
// webhook credential_ref (MTLS, или http_profile вообще отсутствует),
// credential_ref не парсится, либо соответствующее поле отсутствует в
// Vault по этому пути — все трактуются одинаково вызывающей стороной
// (webhook.OperatorTokenAuthenticator), см. её doc-комментарий про
// timing-safety.
func (l *Lookup) ExpectedToken(ctx context.Context, operatorID string) (string, bool, error) {
	if cached, ok := l.cachedToken(operatorID); ok {
		return cached, true, nil
	}

	ref, found, err := l.Config.WebhookCredentialRef(ctx, operatorID)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}

	kvPath, property, err := vault.ParseCredentialRef(ref)
	if err != nil {
		return "", false, err
	}

	token, err := l.Vault.ReadKV2Property(ctx, kvPath, property)
	if err != nil {
		return "", false, err
	}

	l.mu.Lock()
	l.cache[operatorID] = cacheEntry{token: token, fetchedAt: time.Now()}
	l.mu.Unlock()
	return token, true, nil
}

func (l *Lookup) cachedToken(operatorID string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.cache[operatorID]
	if !ok || time.Since(entry.fetchedAt) >= l.ttl() {
		return "", false
	}
	return entry.token, true
}

func (l *Lookup) ttl() time.Duration {
	if l.TTL <= 0 {
		return DefaultCacheTTL
	}
	return l.TTL
}
