// Package pending — кэш "ожидающих корреляции" сырых DLR в Runtime Redis,
// keyed по event_id, который DLR Manager генерирует сам при первом
// получении operator.dlr.
//
// Зачем это вообще нужно — реальный, не гипотетический пробел контракта:
// `SchedulerBackgroundTask` (`scheduler_events.proto`) несёт только
// `source_event_id`, не сам payload. Сгенерированный Scheduler Background
// Lane'ом (Субагент 1, `topology/DispatchBuilder.buildDlrRetryWakeup`,
// `services/scheduler-background-lane` — уже реализовано и
// закоммичено) republish на `operator.dlr.unresolved` — это САМА
// `SchedulerBackgroundTask` с `attempt+1`, не `OperatorDlr` (их README,
// "Открытые вопросы" п.1, явно помечает это как непроверенное с этой
// стороны предположение: "Не подтверждено кодом DLR Manager"). Раз
// DLR Manager получает обратно только `source_event_id`, а не тело
// исходного DLR — он обязан сам держать этот payload где-то доступном по
// тому же ключу, иначе retry физически нечего повторно коррелировать.
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
	return "dlr:pending:" + eventID
}

func (s *Store) Save(ctx context.Context, eventID string, dlr *eventsv1.OperatorDlr, ttl time.Duration) error {
	payload, err := proto.Marshal(dlr)
	if err != nil {
		return fmt.Errorf("marshal OperatorDlr: %w", err)
	}
	if err := s.client.Set(ctx, key(eventID), payload, ttl).Err(); err != nil {
		return fmt.Errorf("SET %s: %w", key(eventID), err)
	}
	return nil
}

// Get — `nil, false, nil`, если ключ не найден (истёк TTL, либо
// source_event_id никогда не был у нас — искажённая/чужая задача) — не
// ошибка, вызывающая сторона логирует и молча пропускает (то же
// "известное ограничение", что уже задокументировано в поведении
// autocommit-независимых DLQ-путей других сервисов этой сессии).
func (s *Store) Get(ctx context.Context, eventID string) (*eventsv1.OperatorDlr, bool, error) {
	payload, err := s.client.Get(ctx, key(eventID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("GET %s: %w", key(eventID), err)
	}
	var dlr eventsv1.OperatorDlr
	if err := proto.Unmarshal(payload, &dlr); err != nil {
		return nil, false, fmt.Errorf("unmarshal cached OperatorDlr: %w", err)
	}
	return &dlr, true, nil
}

func (s *Store) Delete(ctx context.Context, eventID string) error {
	if err := s.client.Del(ctx, key(eventID)).Err(); err != nil {
		return fmt.Errorf("DEL %s: %w", key(eventID), err)
	}
	return nil
}
