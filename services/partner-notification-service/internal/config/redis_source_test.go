package config

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestSource — real Redis protocol round-trip against miniredis, same
// convention as config-cache-projector/internal/projector/projector_test.go's
// newTestClient — this exercises RedisSource against the actual keys
// config-cache-projector's WriteProjection writes
// (config:current:partner:{id} / config:version:partner:{id}:{version}),
// not a mock of our own invention.
func newTestSource(t *testing.T) (*RedisSource, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewRedisSourceFromClient(rdb), rdb
}

func seedPartner(t *testing.T, rdb *redis.Client, partnerID string, version int64, payload string) {
	t.Helper()
	ctx := context.Background()
	if err := rdb.Set(ctx, versionKey(partnerID, version), payload, 0).Err(); err != nil {
		t.Fatalf("seed version key: %v", err)
	}
	if err := rdb.Set(ctx, currentKey(partnerID), version, 0).Err(); err != nil {
		t.Fatalf("seed current key: %v", err)
	}
}

func TestFetchPartnerReadsCurrentVersionPayload(t *testing.T) {
	source, rdb := newTestSource(t)
	seedPartner(t, rdb, "acme", 3, `{"partner_id":"acme","version":3,"status":"active","applications":[]}`)

	partner, found, err := source.FetchPartner(context.Background(), "acme")
	if err != nil {
		t.Fatalf("FetchPartner failed: %v", err)
	}
	if !found {
		t.Fatal("ожидали found=true")
	}
	if partner.PartnerID != "acme" || partner.Version != 3 {
		t.Fatalf("partner = %+v, want partner_id=acme version=3", partner)
	}
}

func TestFetchPartnerAcceptsSuspendedEntityInsideActiveConfigVersion(t *testing.T) {
	source, rdb := newTestSource(t)
	seedPartner(t, rdb, "acme", 4, `{"partner_id":"acme","version":4,"status":"suspended","applications":[{"application_id":"acme_app"}]}`)

	partner, found, err := source.FetchPartner(context.Background(), "acme")
	if err != nil || !found {
		t.Fatalf("FetchPartner suspended: found=%v err=%v", found, err)
	}
	if partner.Status != "suspended" {
		t.Fatalf("status=%q, want suspended", partner.Status)
	}
}

func TestFetchPartnerNotFoundWhenNoCurrentPointer(t *testing.T) {
	source, _ := newTestSource(t)

	_, found, err := source.FetchPartner(context.Background(), "ghost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("ожидали found=false — config:current:partner:ghost не существует")
	}
}

func TestFetchPartnerErrorsOnInconsistentProjection(t *testing.T) {
	source, rdb := newTestSource(t)
	ctx := context.Background()
	// config:current указывает на версию 5, но config:version:...:5 не
	// записан — несогласованность projector'а, не должно тихо
	// возвращать found=false.
	if err := rdb.Set(ctx, currentKey("acme"), 5, 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, _, err := source.FetchPartner(ctx, "acme")
	if err == nil {
		t.Fatal("ожидали ошибку при отсутствующем config:version, на который указывает config:current")
	}
}

func TestLoadAllReturnsSnapshotOfAllActivePartners(t *testing.T) {
	source, rdb := newTestSource(t)
	seedPartner(t, rdb, "acme", 1, `{"partner_id":"acme","version":1,"status":"active","applications":[{"application_id":"acme_app"}]}`)
	seedPartner(t, rdb, "beta", 1, `{"partner_id":"beta","version":1,"status":"active","applications":[{"application_id":"beta_app"}]}`)

	snapshot, err := source.LoadAll(context.Background())
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if snapshot.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", snapshot.Len())
	}
	if _, _, found := snapshot.Application("acme", "acme_app"); !found {
		t.Fatal("ожидали найденное acme_app")
	}
	if _, _, found := snapshot.Application("beta", "beta_app"); !found {
		t.Fatal("ожидали найденное beta_app")
	}
}

func TestLoadAllExcludesArchivedPartners(t *testing.T) {
	source, rdb := newTestSource(t)
	seedPartner(t, rdb, "acme", 1, `{"partner_id":"acme","version":1,"status":"active","applications":[{"application_id":"acme_app"}]}`)
	seedPartner(t, rdb, "gone", 2, `{"partner_id":"gone","version":2,"status":"archived","applications":[]}`)

	snapshot, err := source.LoadAll(context.Background())
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if snapshot.Len() != 1 {
		t.Fatalf("Len() = %d, want 1 (archived partner должен быть исключён из bootstrap-снапшота)", snapshot.Len())
	}
	if _, _, found := snapshot.Application("gone", "anything"); found {
		t.Fatal("archived partner не должен резолвиться")
	}
}

func TestLoadAllEmptyConfigurationRedisReturnsEmptySnapshot(t *testing.T) {
	source, _ := newTestSource(t)

	snapshot, err := source.LoadAll(context.Background())
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if snapshot.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", snapshot.Len())
	}
}
