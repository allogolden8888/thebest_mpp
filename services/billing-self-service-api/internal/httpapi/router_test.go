// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware (JWT, тестовый ключ) + реальный локальный Postgres
// (billing.billing_ledger) + реальный gRPC-клиент поверх bufconn с фейковым
// ConfigServiceServer — тот же паттерн, что во всех остальных сервисах этой
// сессии (compliance-api/consent_test.go, partner-self-service-api/router_test.go).
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/billing-self-service-api/internal/auth"
	"mpp/billing-self-service-api/internal/store"
)

const (
	testAudience = "billing-self-service-api"
	testIssuer   = "https://keycloak.mpp.svc/realms/mpp"
)

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
	dsn := os.Getenv("BILLING_SELF_SERVICE_API_TEST_DSN")
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

func uniquePartnerID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-partner-%d", time.Now().UnixNano())
}

// fakeConfigServer — держит один payload на entity_id (совмещённый ключ по
// entity_type в имени, т.к. BILLING_TARIFF и PARTNER читаются под разными
// entity_id для одного теста без коллизий — партнёр сам себе и entity_id
// в обоих случаях).
type fakeConfigServer struct {
	grpcv1.UnimplementedConfigServiceServer
	mu       sync.Mutex
	tariffs  map[string][]byte
	partners map[string][]byte
}

func newFakeConfigServer() *fakeConfigServer {
	return &fakeConfigServer{tariffs: map[string][]byte{}, partners: map[string][]byte{}}
}

func (f *fakeConfigServer) seedTariff(partnerID string, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tariffs[partnerID] = payload
}

func (f *fakeConfigServer) seedPartner(partnerID string, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partners[partnerID] = payload
}

func (f *fakeConfigServer) GetActiveVersion(ctx context.Context, req *grpcv1.GetActiveVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var payload []byte
	var ok bool
	switch req.GetEntityType() {
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF:
		payload, ok = f.tariffs[req.GetEntityId()]
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER:
		payload, ok = f.partners[req.GetEntityId()]
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "не найдено: %s/%s", req.GetEntityType(), req.GetEntityId())
	}
	return &grpcv1.ConfigVersionResponse{
		EntityType: req.GetEntityType(), EntityId: req.GetEntityId(), Version: 1, Status: "active", PayloadJson: payload,
	}, nil
}

func dialBufconn(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type testFixture struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	config *fakeConfigServer
	pool   *pgxpool.Pool
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	pool := testPostgresPool(t)
	t.Cleanup(pool.Close)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := auth.NewValidator(&key.PublicKey, testAudience, testIssuer)

	configSrv := newFakeConfigServer()
	configConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, configSrv) })

	router := NewRouter(Deps{
		Validator:      validator,
		ConfigClient:   grpcv1.NewConfigServiceClient(configConn),
		Store:          store.NewPostgres(pool),
		TracerProvider: sdktrace.NewTracerProvider(),
	})

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	return &testFixture{server: server, key: key, config: configSrv, pool: pool}
}

func doRequest(t *testing.T, f *testFixture, method, path, token string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, f.server.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	return resp.StatusCode, body
}

func insertLedgerRow(t *testing.T, pool *pgxpool.Pool, partnerID, accountID string, amount string, entryType string, sourceChargeID *string, createdAt time.Time) {
	t.Helper()
	chargeID := uuid.New().String()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO billing.billing_ledger (charge_id, account_id, partner_id, amount, currency, entry_type, source_charge_id, created_at)
		VALUES ($1, $2, $3, $4, 'UZS', $5, $6, $7)
	`, chargeID, accountID, partnerID, amount, entryType, sourceChargeID, createdAt)
	if err != nil {
		t.Fatalf("insert ledger row failed: %v", err)
	}
}

func TestLedgerRequiresAuth(t *testing.T) {
	f := newTestFixture(t)
	status, _ := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/ledger", "")
	if status != http.StatusUnauthorized {
		t.Fatalf("ожидали 401, получили %d", status)
	}
}

func TestLedgerReturnsOnlyOwnPartnerRows(t *testing.T) {
	f := newTestFixture(t)
	partnerA := uniquePartnerID(t)
	partnerB := uniquePartnerID(t)

	insertLedgerRow(t, f.pool, partnerA, partnerA, "100.0000", "charge", nil, time.Now())
	insertLedgerRow(t, f.pool, partnerB, partnerB, "200.0000", "charge", nil, time.Now())

	token := testToken(t, f.key, partnerA)
	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/ledger", token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}

	var entries []store.LedgerEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(entries) != 1 || entries[0].PartnerID != partnerA {
		t.Fatalf("ожидали ровно 1 запись partnerA, не чужую: %+v", entries)
	}
	if entries[0].Amount != "100.0000" {
		t.Fatalf("amount не пробросился как строка без потери точности: %q", entries[0].Amount)
	}
}

func TestSpendSummaryNetsChargeAndCompensating(t *testing.T) {
	f := newTestFixture(t)
	partnerID := uniquePartnerID(t)
	from := time.Now().Add(-time.Hour)

	insertLedgerRow(t, f.pool, partnerID, partnerID, "100.0000", "charge", nil, time.Now())
	// compensating требует непустой source_charge_id по CHECK-constraint'у
	// таблицы (V008__billing_ledger.sql), но CHECK не проверяет, что строка с
	// таким charge_id реально существует — синтетическое значение допустимо.
	insertLedgerRow(t, f.pool, partnerID, partnerID, "30.0000", "compensating", strPtr("00000000-0000-0000-0000-000000000001"), time.Now())

	to := time.Now().Add(time.Hour)
	token := testToken(t, f.key, partnerID)
	query := url.Values{"from": {from.Format(time.RFC3339)}, "to": {to.Format(time.RFC3339)}}
	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/summary?"+query.Encode(), token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}

	var summary []store.SpendSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(summary) != 1 || summary[0].Total != "70.0000" {
		t.Fatalf("ожидали net 100-30=70, получили %+v", summary)
	}
}

func strPtr(s string) *string { return &s }

func TestTariffReturnsCustomFalseWhenNotPublished(t *testing.T) {
	f := newTestFixture(t)
	partnerID := uniquePartnerID(t)
	token := testToken(t, f.key, partnerID)

	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/tariff", token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}
	var resp tariffResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.Custom {
		t.Fatalf("ожидали custom=false без опубликованного тарифа, получили %+v", resp)
	}
}

func TestTariffReturnsCustomTrueWhenPublished(t *testing.T) {
	f := newTestFixture(t)
	partnerID := uniquePartnerID(t)
	f.config.seedTariff(partnerID, []byte(`{"currency":"UZS","price_per_segment":{"BLOCKED":94,"TRANSACTION":50},"default_category":"TRANSACTION"}`))

	token := testToken(t, f.key, partnerID)
	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/tariff", token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}
	var resp tariffResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !resp.Custom || resp.Tariff == nil || resp.Tariff.Currency != "UZS" {
		t.Fatalf("ожидали custom=true с реальным тарифом, получили %+v", resp)
	}
}

func TestRecurringCombinesActiveSendersWithTariffFee(t *testing.T) {
	f := newTestFixture(t)
	partnerID := uniquePartnerID(t)
	f.config.seedPartner(partnerID, []byte(fmt.Sprintf(`{
		"partner_id": %q,
		"senders": [
			{"sender_id": "ACME", "type": "ALPHANAME", "status": "active"},
			{"sender_id": "OLD", "type": "ALPHANAME", "status": "archived"}
		]
	}`, partnerID)))
	f.config.seedTariff(partnerID, []byte(`{
		"currency":"UZS","price_per_segment":{"BLOCKED":94},"default_category":"TRANSACTION",
		"recurring_charges":{"alphaname_monthly_fee":4000000}
	}`))

	token := testToken(t, f.key, partnerID)
	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/recurring", token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}

	var previews []recurringChargePreview
	if err := json.Unmarshal(body, &previews); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(previews) != 1 || previews[0].SenderID != "ACME" {
		t.Fatalf("ожидали ровно 1 active sender (archived исключён), получили %+v", previews)
	}
	if !previews[0].FeeKnown || previews[0].Fee == nil || *previews[0].Fee != 4000000 {
		t.Fatalf("ожидали известную плату 4000000 из опубликованного тарифа, получили %+v", previews[0])
	}
}

func TestRecurringReportsFeeUnknownWithoutPublishedTariff(t *testing.T) {
	f := newTestFixture(t)
	partnerID := uniquePartnerID(t)
	f.config.seedPartner(partnerID, []byte(fmt.Sprintf(`{
		"partner_id": %q,
		"senders": [{"sender_id": "ACME", "type": "ALPHANAME", "status": "active"}]
	}`, partnerID)))
	// Тариф намеренно не засеян.

	token := testToken(t, f.key, partnerID)
	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/billing/recurring", token)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}

	var previews []recurringChargePreview
	if err := json.Unmarshal(body, &previews); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(previews) != 1 || previews[0].FeeKnown {
		t.Fatalf("без опубликованного тарифа fee_known обязан быть false, не угаданное значение: %+v", previews)
	}
}
