package opconfig

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestReader(t *testing.T) (*Reader, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewReaderFromRedis(rdb), mr
}

func seedOperator(t *testing.T, mr *miniredis.Miniredis, operatorID string, version int, payload string) {
	t.Helper()
	mr.Set("config:current:operator:"+operatorID, itoa(version))
	mr.Set("config:version:operator:"+operatorID+":"+itoa(version), payload)
}

func itoa(v int) string {
	digits := "0123456789"
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{digits[v%10]}, b...)
		v /= 10
	}
	return string(b)
}

func TestWebhookCredentialRefFoundForBearerToken(t *testing.T) {
	r, mr := newTestReader(t)
	seedOperator(t, mr, "beeline_uz", 1, `{"operator_id":"beeline_uz","version":1,"status":"active","http_profile":{"endpoint_url":"https://x","tps_limit":1,"webhook_auth":{"type":"BEARER_TOKEN","credential_ref":"vault://operators/beeline_uz/webhook_bearer"},"retry_policy":{"max_attempts":1,"backoff_ms":1},"reconnect_policy":{"initial_backoff_ms":1,"max_backoff_ms":1,"multiplier":1.1}}}`)

	ref, found, err := r.WebhookCredentialRef(context.Background(), "beeline_uz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if ref != "vault://operators/beeline_uz/webhook_bearer" {
		t.Fatalf("unexpected credential_ref: %q", ref)
	}
}

func TestWebhookCredentialRefNotFoundForUnknownOperator(t *testing.T) {
	r, _ := newTestReader(t)
	ref, found, err := r.WebhookCredentialRef(context.Background(), "unknown_operator")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatalf("expected found=false, got credential_ref=%q", ref)
	}
}

func TestWebhookCredentialRefNotFoundWhenNoHTTPProfile(t *testing.T) {
	r, mr := newTestReader(t)
	// SMPP-only operator — anyOf в схеме допускает отсутствие http_profile.
	seedOperator(t, mr, "smpp_only_uz", 1, `{"operator_id":"smpp_only_uz","version":1,"status":"active","smpp_profile":{"max_window":1,"tps_limit":1,"bind_count":1,"reconnect_policy":{"initial_backoff_ms":1,"max_backoff_ms":1,"multiplier":1.1},"enquire_link_interval_sec":1,"dlr_supported":true,"dlr_reliable":true,"query_sm_enabled":false}}`)

	_, found, err := r.WebhookCredentialRef(context.Background(), "smpp_only_uz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected found=false for operator without http_profile")
	}
}

func TestWebhookCredentialRefResultIsCachedNotReReadFromRedis(t *testing.T) {
	r, mr := newTestReader(t)
	seedOperator(t, mr, "ucell_uz", 1, `{"http_profile":{"webhook_auth":{"type":"HMAC_SIGNATURE","credential_ref":"vault://operators/ucell_uz/webhook_hmac"}}}`)

	ref1, found1, err := r.WebhookCredentialRef(context.Background(), "ucell_uz")
	if err != nil || !found1 || ref1 != "vault://operators/ucell_uz/webhook_hmac" {
		t.Fatalf("первый вызов: ref=%q found=%v err=%v", ref1, found1, err)
	}

	// Меняем данные в Redis напрямую, мимо Reader — если бы кеша не было,
	// второй вызов увидел бы новое значение.
	mr.Set("config:version:operator:ucell_uz:1", `{"http_profile":{"webhook_auth":{"type":"HMAC_SIGNATURE","credential_ref":"vault://operators/ucell_uz/CHANGED"}}}`)

	ref2, found2, err := r.WebhookCredentialRef(context.Background(), "ucell_uz")
	if err != nil || !found2 || ref2 != "vault://operators/ucell_uz/webhook_hmac" {
		t.Fatalf("второй вызов должен был использовать кеш, получили ref=%q found=%v err=%v", ref2, found2, err)
	}
}

func TestInvalidateForcesReReadFromRedis(t *testing.T) {
	r, mr := newTestReader(t)
	seedOperator(t, mr, "ucell_uz", 1, `{"http_profile":{"webhook_auth":{"type":"HMAC_SIGNATURE","credential_ref":"vault://operators/ucell_uz/webhook_hmac"}}}`)

	if _, _, err := r.WebhookCredentialRef(context.Background(), "ucell_uz"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mr.Set("config:version:operator:ucell_uz:1", `{"http_profile":{"webhook_auth":{"type":"HMAC_SIGNATURE","credential_ref":"vault://operators/ucell_uz/CHANGED"}}}`)
	r.Invalidate("ucell_uz")

	ref, _, err := r.WebhookCredentialRef(context.Background(), "ucell_uz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref != "vault://operators/ucell_uz/CHANGED" {
		t.Fatalf("после Invalidate ожидали свежее значение, получили %q", ref)
	}
}

func TestWebhookCredentialRefErrorWhenCurrentPointsToMissingVersion(t *testing.T) {
	r, mr := newTestReader(t)
	mr.Set("config:current:operator:broken_uz", "1")
	// config:version:operator:broken_uz:1 намеренно не создан.

	if _, _, err := r.WebhookCredentialRef(context.Background(), "broken_uz"); err == nil {
		t.Fatal("ожидали ошибку, когда config:current указывает на отсутствующую версию")
	}
}
