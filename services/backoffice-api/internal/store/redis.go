// Package store — Redis.ScanOperatorRoutes читает те же ключи Runtime
// Redis, что operator-smpp-session-manager (registry/OperatorRouteRegistry.
// java) и operator-http-gateway (internal/registry/registry.go) пишут:
//
//	operator_route:{operator_id}:{route_id}  HASH
//	  protocol            "SMPP" | "HTTP"
//	  owning_instance_id  hostname инстанса, который сейчас держит route
//	  endpoint            строка эндпоинта
//	  route_epoch         строковый int64
//	  heartbeat           строковый unix millis (System.currentTimeMillis())
//
// Ключ живёт под TTL (обновляется вместе с heartbeat, 3x heartbeat interval,
// см. OperatorRouteRegistry.java) — "мёртвый" route не имеет отдельного
// status-поля, это просто ключ, у которого истёк TTL и он исчез из Redis
// (SCAN его больше не увидит). Только чтение — backoffice-api никогда не
// пишет в эти ключи (владение записью — исключительно у самих
// operator-*-session-manager/gateway инстансов через conditional
// Lua-скрипты HEARTBEAT_IF_OWNER/UNREGISTER_IF_OWNER).
//
// Нет источника "список всех operator_id" (backoffice-api's ConfigClient —
// gRPC CreateVersion/GetActiveVersion/ArchiveVersion, нет "list all
// entities" RPC) — SCAN по всему operator_route:* обязателен, тот же
// cursor-loop паттерн, что уже используется в consent-cache-projector's
// resync (internal/projector/projector.go).
package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	rdb *redis.Client
}

func NewRedis(addr, password string) *Redis {
	return &Redis{rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password})}
}

func (r *Redis) Close() error { return r.rdb.Close() }

func (r *Redis) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

// OperatorRoute — один operator_route:{operator_id}:{route_id} снимок.
type OperatorRoute struct {
	OperatorID       string
	RouteID          string
	Protocol         string
	OwningInstanceID string
	Endpoint         string
	RouteEpoch       int64
	Heartbeat        time.Time
	TTLSeconds       int64
}

const operatorRouteKeyPrefix = "operator_route:"

func (r *Redis) ScanOperatorRoutes(ctx context.Context) ([]OperatorRoute, error) {
	var routes []OperatorRoute
	var cursor uint64
	for {
		keys, nextCursor, err := r.rdb.Scan(ctx, cursor, operatorRouteKeyPrefix+"*", 200).Result()
		if err != nil {
			return nil, fmt.Errorf("scan %s*: %w", operatorRouteKeyPrefix, err)
		}
		for _, key := range keys {
			route, err := r.readOperatorRoute(ctx, key)
			if err != nil {
				// Ключ мог истечь (TTL) между SCAN и чтением — штатная
				// гонка живого heartbeat-состояния, не ошибка ответа
				// целиком; пропускаем этот один ключ.
				continue
			}
			routes = append(routes, route)
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return routes, nil
}

func (r *Redis) readOperatorRoute(ctx context.Context, key string) (OperatorRoute, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, operatorRouteKeyPrefix), ":", 2)
	if len(parts) != 2 {
		return OperatorRoute{}, fmt.Errorf("unexpected key shape: %s", key)
	}

	fields, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return OperatorRoute{}, err
	}
	if len(fields) == 0 {
		return OperatorRoute{}, fmt.Errorf("key expired: %s", key)
	}

	ttl, err := r.rdb.TTL(ctx, key).Result()
	if err != nil {
		return OperatorRoute{}, err
	}

	routeEpoch, _ := strconv.ParseInt(fields["route_epoch"], 10, 64)
	heartbeatMillis, _ := strconv.ParseInt(fields["heartbeat"], 10, 64)

	return OperatorRoute{
		OperatorID:       parts[0],
		RouteID:          parts[1],
		Protocol:         fields["protocol"],
		OwningInstanceID: fields["owning_instance_id"],
		Endpoint:         fields["endpoint"],
		RouteEpoch:       routeEpoch,
		Heartbeat:        time.UnixMilli(heartbeatMillis).UTC(),
		TTLSeconds:       int64(ttl.Seconds()),
	}, nil
}
