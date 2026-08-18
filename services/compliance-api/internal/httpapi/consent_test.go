// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware (JWT, тестовый ключ) + реальные gRPC-клиенты поверх
// bufconn (in-process transport, не мок интерфейса клиента) с фейковыми
// реализациями ConfigServiceServer/IamServiceServer + miniredis для
// consent-lookup — тот же паттерн, что services/backoffice-api/internal/httpapi/router_test.go.
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/compliance-api/internal/auth"
	"mpp/compliance-api/internal/redisio"
)

func testToken(t *testing.T, key *rsa.PrivateKey, subject string) string {
	t.Helper()
	claims := jwt.RegisteredClaims{Subject: subject, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись тестового токена failed: %v", err)
	}
	return signed
}

type fakeConfigServer struct {
	grpcv1.UnimplementedConfigServiceServer
	mu          sync.Mutex
	lastRequest *grpcv1.CreateVersionRequest
	nextVersion int64
}

func (f *fakeConfigServer) CreateVersion(ctx context.Context, req *grpcv1.CreateVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastRequest = req
	f.nextVersion++
	return &grpcv1.ConfigVersionResponse{
		EntityType: req.GetEntityType(),
		EntityId:   req.GetEntityId(),
		Version:    f.nextVersion,
		Status:     "active",
	}, nil
}

type fakeIamServer struct {
	grpcv1.UnimplementedIamServiceServer
	mu          sync.Mutex
	allowedPerm map[string]bool // external_id:permission -> allowed
}

func newFakeIamServer() *fakeIamServer {
	return &fakeIamServer{allowedPerm: map[string]bool{}}
}

func (f *fakeIamServer) allow(externalID, permission string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowedPerm[externalID+":"+permission] = true
}

func (f *fakeIamServer) CheckPermission(ctx context.Context, req *grpcv1.CheckPermissionRequest) (*grpcv1.CheckPermissionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	allowed := f.allowedPerm[req.GetExternalId()+":"+req.GetPermission()]
	return &grpcv1.CheckPermissionResponse{Allowed: allowed}, nil
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
	server    *httptest.Server
	key       *rsa.PrivateKey
	config    *fakeConfigServer
	iam       *fakeIamServer
	redis     *redisio.Client
	miniredis *miniredis.Miniredis
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := auth.NewValidator(&key.PublicKey)

	configSrv := &fakeConfigServer{}
	configConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, configSrv) })

	iamSrv := newFakeIamServer()
	iamConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterIamServiceServer(s, iamSrv) })

	mr := miniredis.RunT(t)
	redisClient := redisio.NewClient(mr.Addr(), "")

	router := NewRouter(Deps{
		Validator:      validator,
		Redis:          redisClient,
		ConfigClient:   grpcv1.NewConfigServiceClient(configConn),
		IamClient:      grpcv1.NewIamServiceClient(iamConn),
		TracerProvider: sdktrace.NewTracerProvider(),
	})

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	return &testFixture{server: server, key: key, config: configSrv, iam: iamSrv, redis: redisClient, miniredis: mr}
}

func doRequest(t *testing.T, f *testFixture, method, path, token, body string) (int, map[string]interface{}) {
	t.Helper()
	var reqBody *strings.Reader
	if body == "" {
		reqBody = strings.NewReader("")
	} else {
		reqBody = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, f.server.URL+path, reqBody)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var parsed map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&parsed) // может не быть JSON-тела (plain 401/500 текст) — ок, parsed останется nil
	return resp.StatusCode, parsed
}

func TestConsentLookupRequiresMsisdn(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")

	status, _ := doRequest(t, f, http.MethodGet, "/v1/compliance/consent", token, "")
	if status != http.StatusBadRequest {
		t.Fatalf("ожидали 400 без msisdn, получили %d", status)
	}
}

func TestConsentLookupReturnsEmptyListsWhenNothingBlocked(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")

	status, body := doRequest(t, f, http.MethodGet, "/v1/compliance/consent?msisdn=998901234567", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", status)
	}
	if cats, ok := body["blocked_categories"].([]interface{}); !ok || len(cats) != 0 {
		t.Fatalf("ожидали пустой blocked_categories, получили %v", body["blocked_categories"])
	}
	if senders, ok := body["blocked_senders"].([]interface{}); !ok || len(senders) != 0 {
		t.Fatalf("ожидали пустой blocked_senders, получили %v", body["blocked_senders"])
	}
}

func TestConsentLookupReflectsRealBlacklistState(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")

	// Пишем напрямую в miniredis теми же ключами, что consent-cache-projector
	// реально использует — читаем, что реально лежит, не подставное.
	f.miniredis.SetAdd("consent:category_blacklist:998901234567", "ADVERTISING")
	f.miniredis.SetAdd("consent:sender_blacklist:998901234567", "SPAMMER")

	status, body := doRequest(t, f, http.MethodGet, "/v1/compliance/consent?msisdn=998901234567", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", status)
	}
	cats := body["blocked_categories"].([]interface{})
	if len(cats) != 1 || cats[0] != "ADVERTISING" {
		t.Fatalf("ожидали [ADVERTISING], получили %v", cats)
	}
	senders := body["blocked_senders"].([]interface{})
	if len(senders) != 1 || senders[0] != "SPAMMER" {
		t.Fatalf("ожидали [SPAMMER], получили %v", senders)
	}
}

func TestConsentLookupDoesNotRequireComplianceWritePermission(t *testing.T) {
	// Чтение открыто любому валидному токену — только запись гейтится
	// compliance:write. iam-фейк здесь ничего не allow()'ит, но lookup
	// всё равно должен пройти (RequirePermission на GET не навешан).
	f := newTestFixture(t)
	token := testToken(t, f.key, "no-permissions-at-all")

	status, _ := doRequest(t, f, http.MethodGet, "/v1/compliance/consent?msisdn=998901234567", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200 (чтение не требует compliance:write), получили %d", status)
	}
}

func TestManualConsentRequiresAuthentication(t *testing.T) {
	f := newTestFixture(t)
	status, _ := doRequest(t, f, http.MethodPost, "/v1/compliance/consent", "", `{}`)
	if status != http.StatusUnauthorized {
		t.Fatalf("ожидали 401 без токена, получили %d", status)
	}
}

func TestManualConsentRejectsWithoutPermission(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-no-perm")
	// f.iam намеренно ничего не allow()'ит для этого subject.

	body := `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS","action":"block","reason":"test"}`
	status, _ := doRequest(t, f, http.MethodPost, "/v1/compliance/consent", token, body)
	if status != http.StatusForbidden {
		t.Fatalf("ожидали 403 без compliance:write, получили %d", status)
	}
}

func TestManualConsentValidatesRequiredFields(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")
	f.iam.allow("staff-1", "compliance:write")

	cases := []struct {
		name string
		body string
	}{
		{"missing msisdn", `{"scope_type":"CATEGORY","scope_value":"X","channel":"SMS","action":"block","reason":"r"}`},
		{"bad scope_type", `{"msisdn":"998901234567","scope_type":"WRONG","scope_value":"X","channel":"SMS","action":"block","reason":"r"}`},
		{"missing scope_value", `{"msisdn":"998901234567","scope_type":"CATEGORY","channel":"SMS","action":"block","reason":"r"}`},
		{"missing channel", `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"X","action":"block","reason":"r"}`},
		{"bad action", `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"X","channel":"SMS","action":"WRONG","reason":"r"}`},
		{"missing reason", `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"X","channel":"SMS","action":"block"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := doRequest(t, f, http.MethodPost, "/v1/compliance/consent", token, tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("%s: ожидали 400, получили %d", tc.name, status)
			}
		})
	}
}

func TestManualConsentBlockSendsActiveStatusInPayload(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")
	f.iam.allow("staff-1", "compliance:write")

	body := `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS","action":"block","reason":"жалоба абонента"}`
	status, respBody := doRequest(t, f, http.MethodPost, "/v1/compliance/consent", token, body)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d, body=%v", status, respBody)
	}

	f.config.mu.Lock()
	req := f.config.lastRequest
	f.config.mu.Unlock()
	if req == nil {
		t.Fatalf("CreateVersion не был вызван")
	}
	if req.GetRequestedBy() != "staff-1" {
		t.Fatalf("requested_by должен браться из JWT sub, получили %q", req.GetRequestedBy())
	}

	var payload map[string]string
	if err := json.Unmarshal(req.GetPayloadJson(), &payload); err != nil {
		t.Fatalf("payload_json не распарсился: %v", err)
	}
	if payload["status"] != "active" {
		t.Fatalf("ожидали status=active в payload для action=block, получили %q — без этого поля revocation/block структурно недостижимы через config.changes (см. коммит про subscriber_consent status gap)", payload["status"])
	}
	if payload["msisdn"] != "998901234567" || payload["scope_type"] != "CATEGORY" || payload["scope_value"] != "ADVERTISING" || payload["channel"] != "SMS" {
		t.Fatalf("payload не соответствует запросу: %+v", payload)
	}
}

func TestManualConsentUnblockSendsArchivedStatusInPayload(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")
	f.iam.allow("staff-1", "compliance:write")

	body := `{"msisdn":"998901234567","scope_type":"SENDER","scope_value":"SPAMMER","channel":"SMS","action":"unblock","reason":"отозван по требованию регулятора"}`
	status, _ := doRequest(t, f, http.MethodPost, "/v1/compliance/consent", token, body)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", status)
	}

	f.config.mu.Lock()
	req := f.config.lastRequest
	f.config.mu.Unlock()

	var payload map[string]string
	if err := json.Unmarshal(req.GetPayloadJson(), &payload); err != nil {
		t.Fatalf("payload_json не распарсился: %v", err)
	}
	if payload["status"] != "archived" {
		t.Fatalf("ожидали status=archived в payload для action=unblock, получили %q", payload["status"])
	}
}

func TestManualConsentEntityIDIsComposedFromScopeFields(t *testing.T) {
	f := newTestFixture(t)
	token := testToken(t, f.key, "staff-1")
	f.iam.allow("staff-1", "compliance:write")

	body := `{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS","action":"block","reason":"r"}`
	doRequest(t, f, http.MethodPost, "/v1/compliance/consent", token, body)

	f.config.mu.Lock()
	req := f.config.lastRequest
	f.config.mu.Unlock()

	wantEntityID := "998901234567:CATEGORY:ADVERTISING:SMS"
	if req.GetEntityId() != wantEntityID {
		t.Fatalf("ожидали entity_id=%q, получили %q", wantEntityID, req.GetEntityId())
	}
}
