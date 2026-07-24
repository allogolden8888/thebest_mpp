// Package registry — lookup_gateway_instance (service_internal_methods.md
// §7.1): `smpp:partner_session:{partner_id}:{system_id}` HASH в Runtime
// Redis (data_infrastructure_spec.md §286): session_id,
// gateway_instance_id, endpoint, session_epoch, heartbeat. Владелец записи
// — Partner SMPP Gateway (не реализован ни в одном репозитории на момент
// написания, владелец — Субагент 1) — запись здесь никогда не появится
// живьём, live не проверено.
//
// `system_id` — найдено при реализации: registry ключ требует system_id,
// но partner.schema.json не имеет отдельного поля "SMPP system_id",
// только `application_id`. Разумное, задокументированное допущение:
// system_id = application_id (один бинд на приложение) — тот же класс
// решения, что уже принят в billing-service/policy-service для похожих
// неоднозначностей (см. их README).
package registry

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type GatewayEndpoint struct {
	SessionID         string
	GatewayInstanceID string
	Endpoint          string
	SessionEpoch      int64
}

type Store struct {
	client *redis.Client
}

func NewStore(redisURL string) (*Store, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("redis.ParseURL: %w", err)
	}
	return &Store{client: redis.NewClient(opts)}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Lookup(ctx context.Context, partnerID, systemID string) (GatewayEndpoint, bool, error) {
	key := fmt.Sprintf("smpp:partner_session:%s:%s", partnerID, systemID)
	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return GatewayEndpoint{}, false, fmt.Errorf("HGETALL %s: %w", key, err)
	}
	if len(fields) == 0 {
		return GatewayEndpoint{}, false, nil
	}
	var epoch int64
	_, _ = fmt.Sscanf(fields["session_epoch"], "%d", &epoch)
	return GatewayEndpoint{
		SessionID:         fields["session_id"],
		GatewayInstanceID: fields["gateway_instance_id"],
		Endpoint:          fields["endpoint"],
		SessionEpoch:      epoch,
	}, true, nil
}
