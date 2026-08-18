package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func loadRealPartner(t *testing.T) Partner {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "config_schemas", "examples", "partner.valid.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("не удалось прочитать %s: %v", path, err)
	}
	var partner Partner
	if err := json.Unmarshal(data, &partner); err != nil {
		t.Fatalf("partner.schema.json форма: %v", err)
	}
	return partner
}

func TestLoadsRealPartnerFixtureWithNotificationCallbackURL(t *testing.T) {
	partner := loadRealPartner(t)
	if partner.PartnerID != "click_uz" {
		t.Fatalf("PartnerID = %q, want click_uz", partner.PartnerID)
	}
	if !partner.IsActive() {
		t.Fatal("ожидали активного партнёра")
	}
	if len(partner.Applications) != 2 {
		t.Fatalf("ожидали 2 приложения, получили %d", len(partner.Applications))
	}
	if partner.Applications[0].NotificationCallbackURL == "" {
		t.Fatal("ожидали непустой notification_callback_url в реальном фикстуре")
	}
}

func TestSnapshotApplicationLooksUpByBothIds(t *testing.T) {
	snapshot := NewSnapshot([]Partner{loadRealPartner(t)})
	_, app, found := snapshot.Application("click_uz", "click_uz_main")
	if !found {
		t.Fatal("ожидали найденное приложение")
	}
	// 2000, не 300 — фикстура поднята веткой main (1500 TPS load-test push)
	// для нагрузочного тестирования.
	if app.RateLimitTPS != 2000 {
		t.Errorf("RateLimitTPS = %d, want 2000", app.RateLimitTPS)
	}
}

func TestSnapshotApplicationUnknownReturnsFalse(t *testing.T) {
	snapshot := NewSnapshot([]Partner{loadRealPartner(t)})
	if _, _, found := snapshot.Application("unknown", "click_uz_main"); found {
		t.Fatal("неизвестный партнёр не должен находиться")
	}
	if _, _, found := snapshot.Application("click_uz", "unknown_app"); found {
		t.Fatal("неизвестное приложение не должно находиться")
	}
}

func TestFirstApplicationReturnsFirstConfigured(t *testing.T) {
	snapshot := NewSnapshot([]Partner{loadRealPartner(t)})
	_, app, found := snapshot.FirstApplication("click_uz")
	if !found {
		t.Fatal("ожидали найденное приложение")
	}
	if app.ApplicationID != "click_uz_main" {
		t.Errorf("ApplicationID = %q, want click_uz_main (первое в списке)", app.ApplicationID)
	}
}

func TestResolveDeliveryChannelSmppBindDoesNotNeedCallbackUrl(t *testing.T) {
	app := Application{ApplicationID: "a1", Auth: AuthConfig{Type: "SMPP_BIND"}}
	channel, err := ResolveDeliveryChannel(app)
	if err != nil {
		t.Fatalf("SMPP_BIND не должен требовать notification_callback_url: %v", err)
	}
	if channel != ChannelSMPP {
		t.Errorf("channel = %v, want ChannelSMPP", channel)
	}
}

func TestResolveDeliveryChannelApiKeyWithCallbackUrlIsRest(t *testing.T) {
	app := Application{ApplicationID: "a1", Auth: AuthConfig{Type: "API_KEY"}, NotificationCallbackURL: "https://example.com/hook"}
	channel, err := ResolveDeliveryChannel(app)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if channel != ChannelREST {
		t.Errorf("channel = %v, want ChannelREST", channel)
	}
}

func TestResolveDeliveryChannelApiKeyWithoutCallbackUrlErrors(t *testing.T) {
	app := Application{ApplicationID: "a1", Auth: AuthConfig{Type: "API_KEY"}}
	_, err := ResolveDeliveryChannel(app)
	if err == nil {
		t.Fatal("API_KEY без notification_callback_url должен вернуть ошибку — некуда доставлять")
	}
}
