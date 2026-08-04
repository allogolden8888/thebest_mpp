// Package projector — Consent Cache Projector (data_infrastructure_spec.md
// §1.9c, симметричен Config Cache Projector, но проецирует в Runtime Redis,
// не Configuration Redis — Policy читает consent-данные на каждое
// сообщение, hot path, не bootstrap):
//
//	consent:category_blacklist:{msisdn}  SET  заблокированные категории
//	consent:sender_blacklist:{msisdn}    SET  заблокированные sender_id
//
// entity_type=subscriber_consent — append/delete таблица по PRIMARY KEY
// (msisdn, scope_type, scope_value, channel), не version-based
// (config_schemas/subscriber_consent.schema.json) — ConfigChangeEvent.status
// здесь означает "active"=добавить в блэклист, "archived"=убрать
// (opt-out отозван).
package projector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// ConsentPayload — форма payload_json, config_schemas/subscriber_consent.schema.json.
type ConsentPayload struct {
	MSISDN     string `json:"msisdn"`
	ScopeType  string `json:"scope_type"` // "CATEGORY" | "SENDER"
	ScopeValue string `json:"scope_value"`
	Channel    string `json:"channel"`
}

// ParsePayload — чистая функция разбора payload_json.
func ParsePayload(payloadJSON []byte) (ConsentPayload, error) {
	var p ConsentPayload
	if err := json.Unmarshal(payloadJSON, &p); err != nil {
		return ConsentPayload{}, fmt.Errorf("unmarshal subscriber_consent payload: %w", err)
	}
	if p.MSISDN == "" {
		return ConsentPayload{}, fmt.Errorf("subscriber_consent payload без msisdn")
	}
	if p.ScopeType != "CATEGORY" && p.ScopeType != "SENDER" {
		return ConsentPayload{}, fmt.Errorf("неизвестный scope_type %q", p.ScopeType)
	}
	return p, nil
}

func blacklistKey(p ConsentPayload) string {
	if p.ScopeType == "CATEGORY" {
		return "consent:category_blacklist:" + p.MSISDN
	}
	return "consent:sender_blacklist:" + p.MSISDN
}

type Client struct {
	rdb *redis.Client
}

func NewClient(addr, password string) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password})}
}

func NewClientFromRedis(rdb *redis.Client) *Client {
	return &Client{rdb: rdb}
}

func (c *Client) Close() error { return c.rdb.Close() }

// Ping — /readyz dependency check (CODE_REVIEW.md: "/readyz never
// reflects real downstream health after startup").
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// ApplyConsentChange — on_config_change для entity_type=subscriber_consent:
// status="active" -> SADD (opt-out зарегистрирован); status="archived" ->
// SREM (opt-out отозван).
//
// CODE_REVIEW.md finding #2: раньше любой status, отличный от ровно
// "archived", трактовался как "active" (SADD) без позитивной проверки,
// что status ∈ {"active","archived"} — непоследовательно с более строгой
// валидацией ParsePayload для scope_type в этой же функции. Теперь
// неизвестный status явно отклоняется вместо того, чтобы молча добавлять
// запись в блэклист по default-ветке. Это не более "безопасно по
// умолчанию", чем раньше (fail-closed для блэклиста всё ещё означало бы
// "добавить"), но неизвестный status почти наверняка означает баг
// where-то выше по пайплайну (например, ResolveStatus в
// config-event-publisher вернул что-то неожиданное) — тихо трактовать это
// как "добавить в блэклист" прячет реальную проблему конфигурации вместо
// того, чтобы её проявить.
func (c *Client) ApplyConsentChange(ctx context.Context, event *eventsv1.ConfigChangeEvent) error {
	payload, err := ParsePayload(event.GetPayloadJson())
	if err != nil {
		return err
	}
	status := event.GetStatus()
	if status != "active" && status != "archived" {
		return fmt.Errorf("subscriber_consent entity_id=%q: неизвестный status %q, ожидали active/archived", event.GetEntityId(), status)
	}
	key := blacklistKey(payload)

	if status == "archived" {
		return c.rdb.SRem(ctx, key, payload.ScopeValue).Err()
	}
	return c.rdb.SAdd(ctx, key, payload.ScopeValue).Err()
}

// IsBlacklisted — read-путь для тестов (то же, что Policy::check_category_blacklist/
// check_sender_blacklist делали бы).
func (c *Client) IsBlacklisted(ctx context.Context, scopeType, msisdn, scopeValue string) (bool, error) {
	key := blacklistKey(ConsentPayload{ScopeType: scopeType, MSISDN: msisdn})
	return c.rdb.SIsMember(ctx, key, scopeValue).Result()
}

// ResyncFromPostgres — data_infrastructure_spec.md §1.9c: "consent — не
// хранится только в Redis... при полной потере этой части Runtime Redis
// требуется принудительный full resync из PostgreSQL, а не просто
// ожидание естественного пополнения — это должно быть зафиксировано как
// явная runbook-процедура". CODE_REVIEW.md finding #4 (было
// self-disclosed в README как нереализованное) — реализовано здесь.
//
// Читает ВСЕ строки policy.subscriber_consent (единственный source of
// truth, migrations/V014), перестраивает каждый затронутый
// consent:{category,sender}_blacklist:{msisdn} ключ атомарно
// (write-to-temp-SET-then-RENAME — читатели Policy никогда не видят
// пустой/частично заполненный набор в процессе ресинка, RENAME в Redis
// атомарен), и удаляет любой consent:*_blacklist:* ключ, для которого в
// PostgreSQL больше нет ни одной строки (полностью отозванный набор
// opt-out для этого msisdn — иначе он остался бы фантомом в Redis
// навсегда, ресинк только по существующим строкам его бы не тронул).
func (c *Client) ResyncFromPostgres(ctx context.Context, pool *pgxpool.Pool) (keysWritten, keysDeleted int, err error) {
	rows, err := pool.Query(ctx, `SELECT msisdn, scope_type, scope_value, channel FROM policy.subscriber_consent`)
	if err != nil {
		return 0, 0, fmt.Errorf("resync: query policy.subscriber_consent: %w", err)
	}

	grouped := map[string][]string{}
	for rows.Next() {
		var msisdn, scopeType, scopeValue, channel string
		if scanErr := rows.Scan(&msisdn, &scopeType, &scopeValue, &channel); scanErr != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("resync: scan row: %w", scanErr)
		}
		key := blacklistKey(ConsentPayload{MSISDN: msisdn, ScopeType: scopeType})
		grouped[key] = append(grouped[key], scopeValue)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return 0, 0, fmt.Errorf("resync: rows: %w", rowsErr)
	}

	seen := make(map[string]bool, len(grouped))
	for key, values := range grouped {
		seen[key] = true
		tmpKey := key + ":resync_tmp"
		args := make([]interface{}, len(values))
		for i, v := range values {
			args[i] = v
		}
		_, txErr := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, tmpKey)
			pipe.SAdd(ctx, tmpKey, args...)
			pipe.Rename(ctx, tmpKey, key)
			return nil
		})
		if txErr != nil {
			return keysWritten, keysDeleted, fmt.Errorf("resync: rebuild key %s: %w", key, txErr)
		}
		keysWritten++
	}

	var cursor uint64
	for {
		var keys []string
		var scanErr error
		keys, cursor, scanErr = c.rdb.Scan(ctx, cursor, "consent:*_blacklist:*", 200).Result()
		if scanErr != nil {
			return keysWritten, keysDeleted, fmt.Errorf("resync: scan existing keys: %w", scanErr)
		}
		for _, k := range keys {
			if seen[k] {
				continue
			}
			if delErr := c.rdb.Del(ctx, k).Err(); delErr != nil {
				return keysWritten, keysDeleted, fmt.Errorf("resync: delete stale key %s: %w", k, delErr)
			}
			keysDeleted++
		}
		if cursor == 0 {
			break
		}
	}

	return keysWritten, keysDeleted, nil
}
