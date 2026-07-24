// Package pending — кэш "ожидающих доставки" `MessageLifecycleEvent` в
// Runtime Redis, keyed по `event_id`.
//
// Зачем — тот же реальный пробел контракта, что уже закрыт для
// `dlr-manager` (см. его README), только на другой паре топиков:
// `NotificationRetryTask` (`scheduler_events.proto`) несёт только
// `lifecycle_event_id` — `message_id` в нём НЕ заполняется (уже
// задокументировано как открытый вопрос в
// `scheduler-background-lane/topology/DispatchBuilder.java`,
// `dispatch_notification_retry`: "Partner Notification Service должен
// уметь резолвить message_id из lifecycle_event_id самостоятельно").
// Этот сервис — тот самый Partner Notification Service, который должен
// был это уметь; кэш здесь и есть решение: исходный `MessageLifecycleEvent`
// (несущий message_id) сохраняется под своим `event_id` при первой
// неудачной попытке, извлекается обратно по `lifecycle_event_id` из
// wake-up.
package pending

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
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

func key(eventID string) string {
	return "notification:pending:" + eventID
}

func (s *Store) Save(ctx context.Context, eventID string, event *eventsv1.MessageLifecycleEvent, ttl time.Duration) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal MessageLifecycleEvent: %w", err)
	}
	if err := s.client.Set(ctx, key(eventID), payload, ttl).Err(); err != nil {
		return fmt.Errorf("SET %s: %w", key(eventID), err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, eventID string) (*eventsv1.MessageLifecycleEvent, bool, error) {
	payload, err := s.client.Get(ctx, key(eventID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("GET %s: %w", key(eventID), err)
	}
	var event eventsv1.MessageLifecycleEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, false, fmt.Errorf("unmarshal cached MessageLifecycleEvent: %w", err)
	}
	return &event, true, nil
}

func (s *Store) Delete(ctx context.Context, eventID string) error {
	if err := s.client.Del(ctx, key(eventID)).Err(); err != nil {
		return fmt.Errorf("DEL %s: %w", key(eventID), err)
	}
	return nil
}
