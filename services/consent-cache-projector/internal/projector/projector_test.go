package projector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
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

// CODE_REVIEW.md finding #2: ApplyConsentChange treats any status other
// than exactly "archived" as "add to blacklist" with no positive
// validation that status ∈ {"active","archived"}. This is the direct
// regression test — an unrecognized status (e.g. a bug upstream in
// config-event-publisher.ResolveStatus, or a future entity revision) must
// be rejected loudly, not silently added to the blacklist.
func TestApplyConsentChangeRejectsUnknownStatus(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
		Status:      "pending", // не active/archived
	}
	if err := c.ApplyConsentChange(ctx, event); err == nil {
		t.Fatalf("ожидали ошибку для неизвестного status=%q", event.GetStatus())
	}

	blocked, err := c.IsBlacklisted(ctx, "CATEGORY", "998901234567", "ADVERTISING")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if blocked {
		t.Fatalf("неизвестный status не должен был привести к добавлению в блэклист")
	}
}

func TestApplyConsentChangeRejectsEmptyStatus(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	event := &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
	}
	if err := c.ApplyConsentChange(ctx, event); err == nil {
		t.Fatalf("ожидали ошибку для пустого status")
	}
}

// CODE_REVIEW.md finding #4 (self-disclosed as not implemented): "Full
// Postgres resync on Redis loss not implemented". These tests exercise
// ResyncFromPostgres against a real local Postgres (skips cleanly if
// unavailable, matching config-event-publisher's convention).

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CONSENT_CACHE_PROJECTOR_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	return pool
}

func uniqueMsisdn(t *testing.T) string {
	t.Helper()
	// 998 + 9 цифр (config_schemas/subscriber_consent.schema.json pattern);
	// используем часть unix-нано, чтобы не пересекаться между тестами.
	return fmt.Sprintf("998%09d", time.Now().UnixNano()%1_000_000_000)
}

func insertConsentRow(t *testing.T, pool *pgxpool.Pool, msisdn, scopeType, scopeValue, channel string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO policy.subscriber_consent (msisdn, scope_type, scope_value, channel)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING
	`, msisdn, scopeType, scopeValue, channel)
	if err != nil {
		t.Fatalf("insert test subscriber_consent row failed: %v", err)
	}
}

func deleteConsentRow(t *testing.T, pool *pgxpool.Pool, msisdn, scopeType, scopeValue, channel string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		DELETE FROM policy.subscriber_consent
		WHERE msisdn = $1 AND scope_type = $2 AND scope_value = $3 AND channel = $4
	`, msisdn, scopeType, scopeValue, channel)
	if err != nil {
		t.Fatalf("cleanup delete failed: %v", err)
	}
}

func TestResyncFromPostgresRebuildsBlacklistFromSourceOfTruth(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	c := newTestClient(t)
	ctx := context.Background()

	msisdn := uniqueMsisdn(t)
	insertConsentRow(t, pool, msisdn, "CATEGORY", "ADVERTISING", "SMS")
	defer deleteConsentRow(t, pool, msisdn, "CATEGORY", "ADVERTISING", "SMS")

	// Redis начинает пустым — симулирует "полную потерю Runtime Redis".
	written, deleted, err := c.ResyncFromPostgres(ctx, pool)
	if err != nil {
		t.Fatalf("ResyncFromPostgres failed: %v", err)
	}
	if written < 1 {
		t.Fatalf("ожидали как минимум 1 перезаписанный ключ, получили %d", written)
	}
	if deleted != 0 {
		t.Fatalf("на пустом Redis не должно быть удалённых ключей, получили %d", deleted)
	}

	blocked, err := c.IsBlacklisted(ctx, "CATEGORY", msisdn, "ADVERTISING")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if !blocked {
		t.Fatalf("ожидали, что ресинк восстановил consent из PostgreSQL")
	}
}

// TestResyncFromPostgresDeletesStaleKeyNoLongerInPostgres — если msisdn
// полностью отозвал opt-out (строка удалена из policy.subscriber_consent),
// но старый Redis-набор остался (например, событие archived было
// потеряно ДО этого сессионного фикса cross-cutting автокоммит-бага) —
// full resync должен убрать фантомный ключ, а не просто игнорировать его,
// раз в PostgreSQL по нему больше нет строк.
func TestResyncFromPostgresDeletesStaleKeyNoLongerInPostgres(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	c := newTestClient(t)
	ctx := context.Background()

	msisdn := uniqueMsisdn(t)
	// Redis содержит фантомную запись, которой уже нет в PostgreSQL.
	if err := c.ApplyConsentChange(ctx, &eventsv1.ConfigChangeEvent{
		PayloadJson: []byte(fmt.Sprintf(`{"msisdn":%q,"scope_type":"SENDER","scope_value":"stale_sender","channel":"SMS"}`, msisdn)),
		Status:      "active",
	}); err != nil {
		t.Fatalf("seed ApplyConsentChange failed: %v", err)
	}

	_, deleted, err := c.ResyncFromPostgres(ctx, pool)
	if err != nil {
		t.Fatalf("ResyncFromPostgres failed: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("ожидали как минимум 1 удалённый фантомный ключ, получили %d", deleted)
	}

	blocked, err := c.IsBlacklisted(ctx, "SENDER", msisdn, "stale_sender")
	if err != nil {
		t.Fatalf("IsBlacklisted failed: %v", err)
	}
	if blocked {
		t.Fatalf("фантомная запись, отсутствующая в PostgreSQL, должна была быть удалена ресинком")
	}
}
