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
	if _, _, found := store.Application("acme", "acme_app"); !found {
		t.Fatal("expected application from bootstrap snapshot")
	}
}

func TestApplyPartnerPreservesPublishedSnapshotAndOtherPartners(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	old := store.Get()

	beta := testPartner("beta")
	if !store.ApplyPartner(1, beta) {
		t.Fatal("new partner version was not applied")
	}
	if _, _, found := old.Application("beta", "beta_app"); found {
		t.Fatal("an already published snapshot must remain immutable")
	}
	if _, _, found := store.Application("beta", "beta_app"); !found {
		t.Fatal("live store does not contain the new partner")
	}
	if _, _, found := store.Application("acme", "acme_app"); !found {
		t.Fatal("applying another partner lost existing state")
	}
}

func TestApplyPartnerReplacesOnlyWithNewerVersion(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	updated := testPartner("acme")
	updated.Version = 2
	updated.Applications[0].NotificationCallbackURL = "https://example.com/acme-v2"
	if !store.ApplyPartner(2, updated) {
		t.Fatal("newer version was not applied")
	}

	stale := testPartner("acme")
	stale.Applications[0].NotificationCallbackURL = "https://stale.example.com"
	if store.ApplyPartner(1, stale) {
		t.Fatal("stale replay must be ignored")
	}
	partner, app, found := store.Application("acme", "acme_app")
	if !found || partner.Version != 2 || app.NotificationCallbackURL != "https://example.com/acme-v2" {
		t.Fatalf("unexpected current partner: found=%v partner=%+v app=%+v", found, partner, app)
	}
}

func TestApplyArchiveAcceptsSameVersionTerminalTransition(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	if !store.ApplyArchive(1, "acme") {
		t.Fatal("active(1) -> archived(1) must be accepted")
	}
	if _, _, found := store.Application("acme", "acme_app"); found {
		t.Fatal("archived partner remained in live snapshot")
	}
	if store.ApplyArchive(1, "acme") {
		t.Fatal("duplicate archive must be ignored")
	}
}

func TestArchiveTombstonePreventsReplayRevival(t *testing.T) {
	store := NewStore(NewSnapshot([]Partner{testPartner("acme")}))
	if !store.ApplyArchive(2, "acme") {
		t.Fatal("archive was not applied")
	}
	activeV2 := testPartner("acme")
	activeV2.Version = 2
	if store.ApplyPartner(2, activeV2) {
		t.Fatal("same-version active replay revived an archived partner")
	}
	if store.ApplyPartner(1, testPartner("acme")) {
		t.Fatal("older active replay revived an archived partner")
	}
	if _, _, found := store.Application("acme", "acme_app"); found {
		t.Fatal("archive tombstone was lost")
	}
}

func TestConcurrentApplyPartnerDoesNotLoseUpdates(t *testing.T) {
	store := NewStore(NewSnapshot(nil))
	var wg sync.WaitGroup
	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			partner := testPartner("partner_" + strconv.Itoa(i))
			store.ApplyPartner(1, partner)
		}(i)
	}
	wg.Wait()
	if got := store.Get().Len(); got != n {
		t.Fatalf("Len() = %d, want %d", got, n)
	}
}
