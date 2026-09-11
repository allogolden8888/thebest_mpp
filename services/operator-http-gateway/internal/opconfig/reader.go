// Package opconfig — читает OPERATOR config (config_schemas/operator.schema.json)
// из Configuration Redis, тот же bootstrap/cache-miss паттерн, что уже
// использует billing-service (TariffCache, data_infrastructure_spec.md
// §2.2): ключи ровно те, что пишет config-cache-projector для
// entity_type=operator —
//
//	config:current:operator:{operator_id}          STRING номер активной версии
//	config:version:operator:{operator_id}:{version} STRING JSON payload
//
// Только читающая сторона (config-cache-projector — единственный писатель).
// Извлекает http_profile.webhook_auth.credential_ref — единственное поле,
// которое нужно OperatorTokenAuthenticator (internal/webhook), не весь
// документ.
//
// Кеш в памяти НАВСЕГДА (нет TTL/инвалидации по config.changes в этом
// проходе) — тот же осознанный компромисс и та же формулировка, что
// TariffCache.java: credential_ref сам по себе меняется редко (только когда
// оператор переходит на другой auth.type или Vault path — не при каждой
// ротации значения секрета, та часть — VaultClient.ReadKV2Property с
// собственным TTL, см. internal/webhookauth). Hook под будущий
// config.changes-консьюмер — Invalidate — явно предусмотрен, не реализован.
package opconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// operatorDocument — подмножество config_schemas/operator.schema.json,
// нужное здесь. additionalProperties: false в самой схеме не мешает этой
// структуре описывать только часть полей — encoding/json по умолчанию
// молча пропускает поля JSON, отсутствующие в Go-структуре.
type operatorDocument struct {
	HTTPProfile *struct {
		WebhookAuth struct {
			Type          string `json:"type"`
			CredentialRef string `json:"credential_ref"`
		} `json:"webhook_auth"`
	} `json:"http_profile"`
}

func currentKey(operatorID string) string {
	return "config:current:operator:" + operatorID
}

func versionKey(operatorID, version string) string {
	return "config:version:operator:" + operatorID + ":" + version
}

// Reader — читает Configuration Redis, кеширует в памяти навсегда (см.
// package doc). found=false (без ошибки) — реальный, ожидаемый результат
// для оператора, для которого ни одна OPERATOR-версия ещё не опубликована
// (webhookauth трактует его как fail-closed "нет действующего токена", не
// как ошибку конфигурации).
type Reader struct {
	rdb *redis.Client

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	credentialRef string
	found         bool
}

func NewReader(addr, password string) *Reader {
	return &Reader{
		rdb:   redis.NewClient(&redis.Options{Addr: addr, Password: password}),
		cache: make(map[string]cacheEntry),
	}
}

func NewReaderFromRedis(rdb *redis.Client) *Reader {
	return &Reader{rdb: rdb, cache: make(map[string]cacheEntry)}
}

func (r *Reader) Close() error { return r.rdb.Close() }

// Ping — /readyz dependency check.
func (r *Reader) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

// WebhookCredentialRef — http_profile.webhook_auth.credential_ref для
// operatorID. found=false: либо ни одна OPERATOR-версия не опубликована для
// этого operator_id, либо активная версия не несёт http_profile вообще
// (SMPP-only оператор — anyOf в схеме допускает отсутствие http_profile),
// либо credential_ref пуст (MTLS, где он не требуется схемой). Все три
// случая неразличимы для вызывающей стороны намеренно — во всех трёх нет
// действующего HTTP webhook bearer/hmac секрета, значит webhookauth обязан
// отказать по умолчанию.
func (r *Reader) WebhookCredentialRef(ctx context.Context, operatorID string) (string, bool, error) {
	r.mu.Lock()
	if entry, ok := r.cache[operatorID]; ok {
		r.mu.Unlock()
		return entry.credentialRef, entry.found, nil
	}
	r.mu.Unlock()

	ref, found, err := r.loadFromRedis(ctx, operatorID)
	if err != nil {
		// Сетевая/парсинг-ошибка НЕ кешируется — временная недоступность
		// Configuration Redis не должна навсегда "запечь" отрицательный
		// результат для оператора, чья конфигурация на самом деле есть.
		return "", false, err
	}

	r.mu.Lock()
	r.cache[operatorID] = cacheEntry{credentialRef: ref, found: found}
	r.mu.Unlock()
	return ref, found, nil
}

// Invalidate — hook для будущего config.changes-консьюмера (не реализован
// в этом проходе, см. package doc), тот же паттерн, что
// TariffCache.invalidate.
func (r *Reader) Invalidate(operatorID string) {
	r.mu.Lock()
	delete(r.cache, operatorID)
	r.mu.Unlock()
}

func (r *Reader) loadFromRedis(ctx context.Context, operatorID string) (string, bool, error) {
	version, err := r.rdb.Get(ctx, currentKey(operatorID)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("opconfig: GET %s: %w", currentKey(operatorID), err)
	}

	payload, err := r.rdb.Get(ctx, versionKey(operatorID, version)).Bytes()
	if err == redis.Nil {
		return "", false, fmt.Errorf("opconfig: config:current указывает на версию %s, но %s отсутствует", version, versionKey(operatorID, version))
	}
	if err != nil {
		return "", false, fmt.Errorf("opconfig: GET %s: %w", versionKey(operatorID, version), err)
	}

	var doc operatorDocument
	if err := json.Unmarshal(payload, &doc); err != nil {
		return "", false, fmt.Errorf("opconfig: разбор payload оператора %s версии %s: %w", operatorID, version, err)
	}
	if doc.HTTPProfile == nil || doc.HTTPProfile.WebhookAuth.CredentialRef == "" {
		return "", false, nil
	}
	return doc.HTTPProfile.WebhookAuth.CredentialRef, true, nil
}
