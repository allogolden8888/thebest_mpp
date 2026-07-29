// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware + JWT (сгенерированный тестовый ключ) + реальный
// PostgreSQL/ClickHouse, если доступны локально (иначе t.Skip, тот же
// принцип, что и в internal/store).
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/store"
)

const (
	testAudience = "partner-api"
	testIssuer   = "https://keycloak.mpp.svc/realms/mpp"
)

func newTestValidator(pubKey *rsa.PublicKey) *auth.Validator {
	return auth.NewValidator(pubKey, testAudience, testIssuer)
}

func testToken(t *testing.T, key *rsa.PrivateKey, partnerID string) string {
	t.Helper()
	claims := auth.Claims{
		PartnerID: partnerID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			Audience:  jwt.ClaimStrings{testAudience},
			Issuer:    testIssuer,
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись тестового токена failed: %v", err)
	}
	return signed
}

func testPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PARTNER_API_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("Postgres недоступен (%v) — пропуск", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен (%v) — пропуск", err)
	}
	return pool
}

func TestHandleStatusQueryEndToEnd(t *testing.T) {
	pool := testPostgresPool(t)
	defer pool.Close()
	pg := store.NewPostgres(pool)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := newTestValidator(&key.PublicKey)
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())

	router := NewRouter(validator, pg, nil, tp)
	srv := httptest.NewServer(router)
	defer srv.Close()

	messageID := fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFFFF)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO messaging.message_read_model
			(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, 'acme', 'app1', $2, 'default', 'v1', 'DELIVERED', true)
	`, messageID, fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFF))
	if err != nil {
		t.Fatalf("insert test row failed: %v", err)
	}

	token := testToken(t, key, "acme")

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/status?message_id="+messageID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var body messageStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if body.CurrentStatus != "DELIVERED" {
		t.Fatalf("current_status = %q, want DELIVERED", body.CurrentStatus)
	}
}

func TestHandleStatusQueryWithoutAuthReturns401(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := newTestValidator(&key.PublicKey)
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())

	router := NewRouter(validator, store.NewPostgres(nil), nil, tp)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/messages/status?message_id=abc")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ожидали 401, получили %d", resp.StatusCode)
	}
}

func TestHandleStatusQueryMissingParamsReturns400(t *testing.T) {
	pool := testPostgresPool(t)
	defer pool.Close()
	pg := store.NewPostgres(pool)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := newTestValidator(&key.PublicKey)
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())

	router := NewRouter(validator, pg, nil, tp)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "acme")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("ожидали 400, получили %d", resp.StatusCode)
	}
}

func TestHandleSearchQueryEndToEnd(t *testing.T) {
	pool := testPostgresPool(t)
	defer pool.Close()
	pg := store.NewPostgres(pool)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := newTestValidator(&key.PublicKey)
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())

	router := NewRouter(validator, pg, nil, tp)
	srv := httptest.NewServer(router)
	defer srv.Close()

	appID := fmt.Sprintf("app-%d", time.Now().UnixNano())
	messageID := fmt.Sprintf("%08x-0000-0000-0000-000000000001", time.Now().UnixNano()&0xFFFFFFFF)
	_, err = pool.Exec(context.Background(), `
		INSERT INTO messaging.message_read_model
			(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, 'acme', $2, $3, 'default', 'v1', 'FAILED', true)
	`, messageID, appID, fmt.Sprintf("%08x-0000-0000-0000-000000000002", time.Now().UnixNano()&0xFFFFFF))
	if err != nil {
		t.Fatalf("insert test row failed: %v", err)
	}

	token := testToken(t, key, "acme")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/search?application_id="+appID+"&status=FAILED", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var body searchResults
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if len(body.Results) != 1 || body.Results[0].MessageID != messageID {
		t.Fatalf("неверный результат поиска: %+v", body.Results)
	}
}
