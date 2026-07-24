// Package msgctx — реальный Lettuce/go-redis клиент против `msgctx:{message_id}`
// (data_infrastructure_spec.md §284), нужен только для `partner_id`:
// `message.lifecycle`/`MessageLifecycleEvent` не несёт ни `partner_id`,
// ни `application_id` — этот сервис единственный на всём пути, кому
// вообще нужен `partner_id` после того, как сообщение попало в pipeline,
// поэтому единственный, кто натыкается на этот пробел.
package msgctx

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

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

// PartnerID — пустая строка + false, если msgctx не найден (истёк TTL —
// message_ttl + safety margin, service_internal_methods.md §0 — не должно
// происходить для terminal lifecycle событий в пределах message_ttl, но
// не гарантировано при очень позднем retry, см. README).
func (s *Store) PartnerID(ctx context.Context, messageID string) (string, bool, error) {
	partnerID, err := s.client.HGet(ctx, "msgctx:"+messageID, "partner_id").Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("HGET msgctx:%s partner_id: %w", messageID, err)
	}
	if partnerID == "" {
		return "", false, nil
	}
	return partnerID, true, nil
}
