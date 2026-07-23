package projector

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func newTestClient(t *testing.T) *Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewClientFromRedis(rdb)
}

func TestParsePayloadValid(t *testing.T) {
	p, err := ParsePayload([]byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`))
	if err != nil {
		t.Fatalf("ParsePayload failed: %v", err)
	}
	if p.MSISDN != "998901234567" || p.ScopeType != "CATEGORY" || p.ScopeValue != "ADVERTISING" {
		t.Fatalf("неверный разбор: %+v", p)
	}
}

func TestParsePayloadRejectsMissingMsisdn(t *testing.T) {
	_, err := ParsePayload([]byte(`{"scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`))
	if err == nil {
		t.Fatalf("ожидали ошибку без msisdn")
	}
}

func TestParsePayloadRejectsUnknownScopeType(t *testing.T) {
	_, err := ParsePayload([]byte(`{"msisdn":"998901234567","scope_type":"BOGUS","scope_value":"x","channel":"SMS"}`))
	if err == nil {
		t.Fatalf("ожидали ошибку для неизвестного scope_type")
	}
}

func TestApplyConsentChangeAddsToCategertyBlacklistOnActive(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
		Status:      "active",
	}
	if err := c.ApplyConsentChange(ctx, event); err != nil {
		t.Fatalf("ApplyConsentChange failed: %v", err)
	}

	blocked, err := c.IsBlacklisted(ctx, "CATEGORY", "998901234567", "ADVERTISING")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if !blocked {
		t.Fatalf("ожидали, что ADVERTISING заблокирован для этого msisdn")
	}
}

func TestApplyConsentChangeRemovesFromBlacklistOnArchived(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	add := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"SENDER","scope_value":"click_uz_main","channel":"SMS"}`),
		Status:      "active",
	}
	if err := c.ApplyConsentChange(ctx, add); err != nil {
		t.Fatalf("ApplyConsentChange (add) failed: %v", err)
	}

	remove := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"SENDER","scope_value":"click_uz_main","channel":"SMS"}`),
		Status:      "archived",
	}
	if err := c.ApplyConsentChange(ctx, remove); err != nil {
		t.Fatalf("ApplyConsentChange (remove) failed: %v", err)
	}

	blocked, err := c.IsBlacklisted(ctx, "SENDER", "998901234567", "click_uz_main")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if blocked {
		t.Fatalf("ожидали, что click_uz_main разблокирован после archived (opt-out отозван)")
	}
}

func TestCategoryAndSenderBlacklistsAreIndependent(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	category := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
		Status:      "active",
	}
	if err := c.ApplyConsentChange(ctx, category); err != nil {
		t.Fatalf("ApplyConsentChange failed: %v", err)
	}

	senderBlocked, err := c.IsBlacklisted(ctx, "SENDER", "998901234567", "ADVERTISING")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if senderBlocked {
		t.Fatalf("CATEGORY-блэклист не должен влиять на SENDER-блэклист")
	}
}
