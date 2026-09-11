package kafkaio

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/partner-notification-service/internal/config"
)

func TestDecodeConfigChangeEventSkipsNonPartnerEntityTypes(t *testing.T) {
	payload, err := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF,
		EntityId:   "some-tariff",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	event, err := DecodeConfigChangeEvent(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Fatal("ожидали nil для entity_type != PARTNER — не наш путь")
	}
}

func TestDecodeConfigChangeEventRejectsMissingEntityId(t *testing.T) {
	payload, err := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := DecodeConfigChangeEvent(payload); err == nil {
		t.Fatal("ожидали ошибку для PARTNER-события без entity_id")
	}
}

func TestDecodeConfigChangeEventRejectsGarbageBytes(t *testing.T) {
	if _, err := DecodeConfigChangeEvent([]byte("not a protobuf message, definitely")); err == nil {
		t.Fatal("ожидали ошибку разбора мусорных байт")
	}
}

func TestDecodeConfigChangeEventAcceptsPartner(t *testing.T) {
	payload, err := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    2,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	event, err := DecodeConfigChangeEvent(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil || event.GetEntityId() != "acme" {
		t.Fatalf("event = %+v, want entity_id=acme", event)
	}
}

func newTestRedisSource(t *testing.T) (*config.RedisSource, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return config.NewRedisSourceFromClient(rdb), rdb
}

func recordFor(t *testing.T, event *eventsv1.ConfigChangeEvent) *kgo.Record {
	t.Helper()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &kgo.Record{Topic: TopicConfigChanges, Value: payload}
}

func TestHandleConfigChangeRecordUpsertsActivePartner(t *testing.T) {
	source, rdb := newTestRedisSource(t)
	ctx := context.Background()
	if err := rdb.Set(ctx, "config:version:partner:acme:1", `{"partner_id":"acme","version":1,"status":"active","applications":[{"application_id":"acme_app"}]}`, 0).Err(); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := rdb.Set(ctx, "config:current:partner:acme", 1, 0).Err(); err != nil {
		t.Fatalf("seed current: %v", err)
	}

	store := config.NewStore(config.NewSnapshot(nil))
	record := recordFor(t, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    1,
	})

	if err := HandleConfigChangeRecord(ctx, store, source, record); err != nil {
		t.Fatalf("HandleConfigChangeRecord failed: %v", err)
	}

	if _, _, found := store.Application("acme", "acme_app"); !found {
		t.Fatal("ожидали, что acme появится в живом Store после config.changes события")
	}
}

func TestHandleConfigChangeRecordRemovesArchivedPartner(t *testing.T) {
	source, rdb := newTestRedisSource(t)
	ctx := context.Background()
	if err := rdb.Set(ctx, "config:version:partner:acme:2", `{"partner_id":"acme","version":2,"status":"archived","applications":[]}`, 0).Err(); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := rdb.Set(ctx, "config:current:partner:acme", 2, 0).Err(); err != nil {
		t.Fatalf("seed current: %v", err)
	}

	initial := config.NewSnapshot([]config.Partner{{
		PartnerID: "acme", Version: 1, Status: "active",
		Applications: []config.Application{{ApplicationID: "acme_app"}},
	}})
	store := config.NewStore(initial)

	record := recordFor(t, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    2,
	})

	if err := HandleConfigChangeRecord(ctx, store, source, record); err != nil {
		t.Fatalf("HandleConfigChangeRecord failed: %v", err)
	}

	if _, _, found := store.Application("acme", "acme_app"); found {
		t.Fatal("archived partner должен быть удалён из живого Store, не остаться со старой активной версией")
	}
}

func TestHandleConfigChangeRecordRemovesPartnerMissingFromConfigurationRedis(t *testing.T) {
	source, _ := newTestRedisSource(t) // ничего не засеяно — config:current:partner:acme отсутствует

	initial := config.NewSnapshot([]config.Partner{{
		PartnerID: "acme", Version: 1, Status: "active",
		Applications: []config.Application{{ApplicationID: "acme_app"}},
	}})
	store := config.NewStore(initial)

	record := recordFor(t, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    1,
	})

	if err := HandleConfigChangeRecord(context.Background(), store, source, record); err != nil {
		t.Fatalf("HandleConfigChangeRecord failed: %v", err)
	}

	if _, _, found := store.Application("acme", "acme_app"); found {
		t.Fatal("partner отсутствующий в Configuration Redis должен быть удалён из живого Store")
	}
}

func TestHandleConfigChangeRecordSkipsNonPartnerEventsWithoutError(t *testing.T) {
	source, _ := newTestRedisSource(t)
	store := config.NewStore(config.NewSnapshot(nil))
	record := recordFor(t, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF,
		EntityId:   "some-tariff",
	})

	if err := HandleConfigChangeRecord(context.Background(), store, source, record); err != nil {
		t.Fatalf("unexpected error for non-partner entity_type: %v", err)
	}
}

func TestHandleConfigChangeRecordSkipsPoisonMessageWithoutError(t *testing.T) {
	source, _ := newTestRedisSource(t)
	store := config.NewStore(config.NewSnapshot(nil))
	record := &kgo.Record{Topic: TopicConfigChanges, Value: []byte("garbage")}

	if err := HandleConfigChangeRecord(context.Background(), store, source, record); err != nil {
		t.Fatalf("poison message should be skipped (commit), not retried: %v", err)
	}
}

func TestHandleConfigChangeRecordPropagatesRedisErrorAsRetriable(t *testing.T) {
	source, rdb := newTestRedisSource(t)
	ctx := context.Background()
	// config:current указывает на версию, для которой нет config:version —
	// это FetchPartner-ошибка, должна дойти до вызывающей стороны как
	// retriable (не коммитить оффсет).
	if err := rdb.Set(ctx, "config:current:partner:acme", 9, 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store := config.NewStore(config.NewSnapshot(nil))
	record := recordFor(t, &eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    9,
	})

	if err := HandleConfigChangeRecord(ctx, store, source, record); err == nil {
		t.Fatal("ожидали ошибку — несогласованная проекция должна быть retriable, не тихо проигнорирована")
	}
}
