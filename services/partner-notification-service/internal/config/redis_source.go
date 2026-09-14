package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisSource — bootstrap + on-demand read path against Configuration
// Redis, the same store config-cache-projector's WriteProjection
// (services/config-cache-projector/internal/projector/projector.go)
// writes to via Redis MULTI/EXEC:
//
//	config:current:partner:{partner_id}            STRING  active version number
//	config:version:partner:{partner_id}:{version}  STRING  partner.schema.json payload
//
// This is the bootstrap source for partner config. Live changes are applied
// from the immutable config.changes payload itself: the projector is an
// independent consumer and must not be raced by a per-event Redis re-read.
type RedisSource struct {
	client *redis.Client
}

func NewRedisSource(redisURL string) (*RedisSource, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("redis.ParseURL: %w", err)
	}
	return &RedisSource{client: redis.NewClient(opts)}, nil
}

// NewRedisSourceFromClient — test seam (mirrors
// config-cache-projector/internal/projector.NewClientFromRedis) so unit
// tests can point at miniredis without going through a URL.
func NewRedisSourceFromClient(rdb *redis.Client) *RedisSource {
	return &RedisSource{client: rdb}
}

func (s *RedisSource) Close() error { return s.client.Close() }

// Ping — /readyz dependency check, same convention as
// config-cache-projector's projector.Client.Ping.
func (s *RedisSource) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

const entityTypePartner = "partner"

func currentKey(partnerID string) string {
	return fmt.Sprintf("config:current:%s:%s", entityTypePartner, partnerID)
}

func versionKey(partnerID string, version int64) string {
	return fmt.Sprintf("config:version:%s:%s:%d", entityTypePartner, partnerID, version)
}

// FetchPartner reads one exact current version for LoadAll bootstrap.
//
// found=false means partner_id has no config:current:partner:* entry at
// all (never projected, or config-cache-projector hasn't caught up yet) —
// callers should treat this the same as an archived partner: not eligible
// to be resolved for live traffic.
func (s *RedisSource) FetchPartner(ctx context.Context, partnerID string) (Partner, bool, error) {
	version, err := s.client.Get(ctx, currentKey(partnerID)).Int64()
	if errors.Is(err, redis.Nil) {
		return Partner{}, false, nil
	}
	if err != nil {
		return Partner{}, false, fmt.Errorf("GET %s: %w", currentKey(partnerID), err)
	}

	payload, err := s.client.Get(ctx, versionKey(partnerID, version)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Partner{}, false, fmt.Errorf("config:current:partner:%s указывает на версию %d, но %s отсутствует в Configuration Redis (несогласованность projector'а)", partnerID, version, versionKey(partnerID, version))
	}
	if err != nil {
		return Partner{}, false, fmt.Errorf("GET %s: %w", versionKey(partnerID, version), err)
	}

	var partner Partner
	if err := json.Unmarshal(payload, &partner); err != nil {
		return Partner{}, false, fmt.Errorf("partner.schema.json unmarshal (partner_id=%s version=%d): %w", partnerID, version, err)
	}
	if partner.PartnerID != partnerID {
		return Partner{}, false, fmt.Errorf("partner payload partner_id=%q не совпадает с Redis key partner_id=%q", partner.PartnerID, partnerID)
	}
	if int64(partner.Version) != version {
		return Partner{}, false, fmt.Errorf("partner payload version=%d не совпадает с Redis current version=%d для partner_id=%s", partner.Version, version, partnerID)
	}
	if partner.Status != "active" && partner.Status != "suspended" && partner.Status != "archived" {
		return Partner{}, false, fmt.Errorf("config:current указывает на partner_id=%s с неизвестным payload status=%q", partnerID, partner.Status)
	}
	return partner, true, nil
}

// LoadAll — bootstrap read of every partner currently projected into
// Configuration Redis, replacing the old load-once-from-static-file
// snapshot. SCANs config:current:partner:* (cursor-based, non-blocking —
// this key space is small (one entry per partner) but SCAN over KEYS is
// the same convention used elsewhere in this codebase for Redis
// enumeration) and re-fetches each via FetchPartner.
//
// Archived partners are intentionally excluded from the bootstrap
// snapshot for the same reason config.changes-driven updates remove them
// live (see Partner.IsArchived / internal/kafkaio/configchanges.go) —
// an archived partner should not resolve to a delivery channel, whether
// that's discovered at startup or via a later event.
func (s *RedisSource) LoadAll(ctx context.Context) (Snapshot, error) {
	prefix := fmt.Sprintf("config:current:%s:", entityTypePartner)
	pattern := prefix + "*"

	var partners []Partner
	iter := s.client.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		partnerID := strings.TrimPrefix(iter.Val(), prefix)
		if partnerID == "" {
			continue
		}
		partner, found, err := s.FetchPartner(ctx, partnerID)
		if err != nil {
			return Snapshot{}, fmt.Errorf("bootstrap: не удалось прочитать partner_id=%s из Configuration Redis: %w", partnerID, err)
		}
		if !found || partner.IsArchived() {
			continue
		}
		partners = append(partners, partner)
	}
	if err := iter.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("SCAN %s: %w", pattern, err)
	}
	return NewSnapshot(partners), nil
}
