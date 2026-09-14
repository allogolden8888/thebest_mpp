package config

import (
	"strconv"
	"sync"
	"testing"
)

func testPartner(id string) Partner {
	return Partner{
		PartnerID: id,
		Version:   1,
		Status:    "active",
		Applications: []Application{
			{ApplicationID: id + "_app", Auth: AuthConfig{Type: "API_KEY"}, NotificationCallbackURL: "https://example.com/" + id},
		},
	}
}

func TestStoreGetReturnsInitialSnapshot(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	_, _, found := store.Application("acme", "acme_app")
	if !found {
		t.Fatal("ожидали найденное приложение из bootstrap-снапшота")
	}
}

func TestStoreUpsertAddsNewPartnerWithoutAffectingOldSnapshot(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	old := store.Get()

	store.Upsert(testPartner("beta"))

	if _, _, found := old.Application("beta", "beta_app"); found {
		t.Fatal("старый снапшот, полученный ДО Upsert, не должен видеть новый partner_id — Snapshot обязан быть неизменяемым")
	}
	if _, _, found := store.Application("beta", "beta_app"); !found {
		t.Fatal("Store.Get() после Upsert должен видеть новый partner_id")
	}
	if _, _, found := store.Application("acme", "acme_app"); !found {
		t.Fatal("Upsert нового партнёра не должен терять существующих")
	}
}

func TestStoreUpsertReplacesExistingPartnerVersion(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))

	updated := testPartner("acme")
	updated.Version = 2
	updated.Applications[0].NotificationCallbackURL = "https://example.com/acme-v2"
	store.Upsert(updated)

	partner, app, found := store.Application("acme", "acme_app")
	if !found {
		t.Fatal("ожидали найденное приложение после Upsert")
	}
	if partner.Version != 2 {
		t.Fatalf("Version = %d, want 2 (Upsert должен заменить, не дублировать)", partner.Version)
	}
	if app.NotificationCallbackURL != "https://example.com/acme-v2" {
		t.Fatalf("NotificationCallbackURL не обновился: %s", app.NotificationCallbackURL)
	}
}

func TestStoreRemoveDropsPartnerFromLiveMap(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme"), testPartner("beta")}))

	store.Remove("acme")

	if _, _, found := store.Application("acme", "acme_app"); found {
		t.Fatal("acme должен быть удалён из живого снапшота")
	}
	if _, _, found := store.Application("beta", "beta_app"); !found {
		t.Fatal("Remove одного партнёра не должен затрагивать остальных")
	}
}

func TestStoreRemoveUnknownPartnerIsNoop(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	store.Remove("unknown") // не должно паниковать/зависать
	if _, _, found := store.Application("acme", "acme_app"); !found {
		t.Fatal("Remove неизвестного partner_id не должен трогать существующие записи")
	}
}

// TestStoreConcurrentUpsertsDoNotLoseUpdates — Store.Upsert используется
// из одного consumer-цикла (config.changes партиции обрабатываются
// последовательно в HandleConfigChangeRecord), но CompareAndSwap-цикл
// обязан быть корректен и под настоящей конкуренцией — это не гипотетика,
// это ровно тот примитив (atomic.Pointer + CAS retry), который защищает
// от гонки читателей (HandleRecord на каждое message.lifecycle) с
// писателем (config.changes consumer) при активной живой доставке.
func TestStoreConcurrentUpsertsDoNotLoseUpdates(t *testing.T) {
	store := NewStore(NewSnapshot(nil))
	var wg sync.WaitGroup
	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.Upsert(testPartner("partner_" + strconv.Itoa(i)))
		}(i)
	}
	wg.Wait()

	snapshot := store.Get()
	if snapshot.Len() != n {
		t.Fatalf("Len() = %d, want %d — конкурентные Upsert потеряли обновления", snapshot.Len(), n)
	}
}
