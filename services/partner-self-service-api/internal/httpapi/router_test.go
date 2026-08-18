// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware (JWT, тестовый ключ) + реальные gRPC-клиенты поверх
// bufconn (in-process transport, не мок интерфейса клиента) с фейковыми
// реализациями ConfigServiceServer/CredentialIssuerServiceServer — тот же
// паттерн, что services/compliance-api/internal/httpapi/consent_test.go и
// services/backoffice-api/internal/httpapi/router_test.go.
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
)

const (
	testAudience = "partner-self-service-api"
	testIssuer   = "https://keycloak.mpp.svc/realms/mpp"
)

// testToken — auth.Claims несёт неэкспортируемый тип realmAccess, поэтому
// снаружи пакета auth собираем эквивалентный набор через jwt.MapClaims —
// на валидацию (aud/iss/exp/partner_id) и на разбор в auth.Claims это не
// влияет, jwt.ParseWithClaims работает по JSON, не по Go-типу подписчика.
func testToken(t *testing.T, key *rsa.PrivateKey, partnerID string, roles []string) string {
	t.Helper()
	claims := jwt.MapClaims{
		"partner_id": partnerID,
		"realm_access": map[string]interface{}{
			"roles": roles,
		},
		"aud": testAudience,
		"iss": testIssuer,
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)).Unix(),
		"sub": "user-" + partnerID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись тестового токена failed: %v", err)
	}
	return signed
}

// fakeConfigServer — держит по одному активному payload'у на entity_id,
// version инкрементируется на каждый CreateVersion (тот же контракт, что
// реальный configuration-service). GetActiveVersion возвращает NotFound,
// если для entity_id ещё ничего не создано.
type fakeConfigServer struct {
	grpcv1.UnimplementedConfigServiceServer
	mu       sync.Mutex
	versions map[string]int64
	payloads map[string][]byte
}

func newFakeConfigServer() *fakeConfigServer {
	return &fakeConfigServer{versions: map[string]int64{}, payloads: map[string][]byte{}}
}

func (f *fakeConfigServer) seed(entityID string, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versions[entityID] = 1
	f.payloads[entityID] = payload
}

func (f *fakeConfigServer) CreateVersion(ctx context.Context, req *grpcv1.CreateVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versions[req.GetEntityId()]++
	f.payloads[req.GetEntityId()] = req.GetPayloadJson()
	return &grpcv1.ConfigVersionResponse{
		EntityType:  req.GetEntityType(),
		EntityId:    req.GetEntityId(),
		Version:     f.versions[req.GetEntityId()],
		Status:      "active",
		PayloadJson: req.GetPayloadJson(),
	}, nil
}

func (f *fakeConfigServer) GetActiveVersion(ctx context.Context, req *grpcv1.GetActiveVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	payload, ok := f.payloads[req.GetEntityId()]
	if !ok {
		return nil, fmt.Errorf("entity_id %q не найден", req.GetEntityId())
	}
	return &grpcv1.ConfigVersionResponse{
		EntityType:  req.GetEntityType(),
		EntityId:    req.GetEntityId(),
		Version:     f.versions[req.GetEntityId()],
		Status:      "active",
		PayloadJson: payload,
	}, nil
}

type fakeCredentialServer struct {
	grpcv1.UnimplementedCredentialIssuerServiceServer
	mu            sync.Mutex
	rotateCalls   []*grpcv1.RotateCredentialRequest
	rotateErr     error
	issuedSecrets []*grpcv1.IssuedSecretSummary
}

func (f *fakeCredentialServer) RotateCredential(ctx context.Context, req *grpcv1.RotateCredentialRequest) (*grpcv1.RotateCredentialResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rotateCalls = append(f.rotateCalls, req)
	if f.rotateErr != nil {
		return nil, f.rotateErr
	}
	return &grpcv1.RotateCredentialResponse{
		CredentialRef:   "vault://partners/" + req.GetPartnerId() + "/" + req.GetApplicationId() + "/api_key",
		SecretVersion:   2,
		PlaintextSecret: "s3cr3t-plaintext-value",
	}, nil
}

func (f *fakeCredentialServer) ListIssuedSecrets(ctx context.Context, req *grpcv1.ListIssuedSecretsRequest) (*grpcv1.ListIssuedSecretsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*grpcv1.IssuedSecretSummary
	for _, s := range f.issuedSecrets {
		if s.GetPartnerId() == req.GetPartnerId() {
			out = append(out, s)
		}
	}
	return &grpcv1.ListIssuedSecretsResponse{Secrets: out}, nil
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
	cred   *fakeCredentialServer
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := auth.NewValidator(&key.PublicKey, testAudience, testIssuer)

	configSrv := newFakeConfigServer()
	configConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, configSrv) })

	credSrv := &fakeCredentialServer{}
	credConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterCredentialIssuerServiceServer(s, credSrv) })

	router := NewRouter(Deps{
		Validator:        validator,
		ConfigClient:     grpcv1.NewConfigServiceClient(configConn),
		CredentialClient: grpcv1.NewCredentialIssuerServiceClient(credConn),
		TracerProvider:   sdktrace.NewTracerProvider(),
	})

	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	return &testFixture{server: server, key: key, config: configSrv, cred: credSrv}
}

// TestListTemplatesForcesPartnerIDFromClaims — прямое доказательство находки
// в templates.go: template-management-service не несёт auth и принимает
// partner_id как обычный query-параметр — этот прокси обязан ПОЛНОСТЬЮ
// игнорировать partner_id, который мог бы прислать вызывающий, и всегда
// подставлять claims.PartnerID. Фейковый апстрим здесь просто эхом
// возвращает partner_id, который реально получил, чтобы тест мог это
// проверить.
func TestListTemplatesForcesPartnerIDFromClaims(t *testing.T) {
	var receivedPartnerID string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPartnerID = r.URL.Query().Get("partner_id")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"templates":[],"limit":50,"offset":0}`))
	}))
	defer upstream.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	validator := auth.NewValidator(&key.PublicKey, testAudience, testIssuer)
	router := NewRouter(Deps{
		Validator:           validator,
		ConfigClient:        grpcv1.NewConfigServiceClient(dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, newFakeConfigServer()) })),
		CredentialClient:    grpcv1.NewCredentialIssuerServiceClient(dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterCredentialIssuerServiceServer(s, &fakeCredentialServer{}) })),
		TemplatesServiceURL: upstream.URL,
		TracerProvider:      sdktrace.NewTracerProvider(),
	})
	server := httptest.NewServer(router)
	defer server.Close()

	claims := jwt.MapClaims{
		"partner_id": "acme", "aud": testAudience, "iss": testIssuer,
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)).Unix(), "sub": "user-acme",
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	if err != nil {
		t.Fatalf("подпись тестового токена failed: %v", err)
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/self-service/templates?partner_id=someone-else", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
	if receivedPartnerID != "acme" {
		t.Fatalf("upstream получил partner_id=%q — подмена query-параметром вызывающего не должна была сработать, ожидали claims.PartnerID=acme", receivedPartnerID)
	}
}

func doRequest(t *testing.T, f *testFixture, method, path, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, f.server.URL+path, strings.NewReader(body))
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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	return resp.StatusCode, respBody
}

const seedPartnerConfig = `{
	"partner_id": "acme",
	"version": 1,
	"status": "active",
	"applications": [
		{
			"application_id": "app1",
			"display_name": "Main",
			"auth": {"type": "API_KEY", "credential_ref": "vault://partners/acme/app1/api_key"},
			"ip_allowlist": ["10.0.0.0/8"],
			"rate_limit_tps": 100,
			"allowed_channels": ["SMS"]
		}
	],
	"senders": [
		{"sender_id": "ACME", "type": "ALPHANAME", "status": "active"}
	]
}`

func TestListApplicationsRequiresAuth(t *testing.T) {
	f := newTestFixture(t)
	status, _ := doRequest(t, f, http.MethodGet, "/v1/self-service/applications", "", "")
	if status != http.StatusUnauthorized {
		t.Fatalf("ожидали 401, получили %d", status)
	}
}

func TestListApplicationsReturnsSeededConfig(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", nil)

	status, body := doRequest(t, f, http.MethodGet, "/v1/self-service/applications", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, body)
	}
	var apps []application
	if err := json.Unmarshal(body, &apps); err != nil {
		t.Fatalf("decode failed: %v (%s)", err, body)
	}
	if len(apps) != 1 || apps[0].ApplicationID != "app1" {
		t.Fatalf("неверный список applications: %+v", apps)
	}
}

func TestCreateApplicationRequiresAdmin(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", nil) // без роли partner-admin

	body := `{"application_id":"app2","display_name":"Second","auth":{"type":"API_KEY","credential_ref":"vault://x"},"rate_limit_tps":10,"allowed_channels":["SMS"]}`
	status, _ := doRequest(t, f, http.MethodPost, "/v1/self-service/applications", token, body)
	if status != http.StatusForbidden {
		t.Fatalf("ожидали 403 без роли partner-admin, получили %d", status)
	}
}

func TestCreateApplicationAsAdminSucceedsAndPersists(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	body := `{"application_id":"app2","display_name":"Second","auth":{"type":"API_KEY","credential_ref":"vault://x"},"rate_limit_tps":10,"allowed_channels":["SMS"]}`
	status, respBody := doRequest(t, f, http.MethodPost, "/v1/self-service/applications", token, body)
	if status != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d: %s", status, respBody)
	}

	status, listBody := doRequest(t, f, http.MethodGet, "/v1/self-service/applications", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", status)
	}
	var apps []application
	if err := json.Unmarshal(listBody, &apps); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(apps) != 2 {
		t.Fatalf("ожидали 2 application после создания, получили %d: %+v", len(apps), apps)
	}
}

func TestCreateApplicationDuplicateIDReturnsConflict(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	body := `{"application_id":"app1","display_name":"Dup","auth":{"type":"API_KEY","credential_ref":"vault://x"},"rate_limit_tps":10,"allowed_channels":["SMS"]}`
	status, _ := doRequest(t, f, http.MethodPost, "/v1/self-service/applications", token, body)
	if status != http.StatusConflict {
		t.Fatalf("ожидали 409 на дубликат application_id, получили %d", status)
	}
}

func TestPartnerCannotSeeAnotherPartnersApplications(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	// beta не засеян вообще — GetActiveVersion должен вернуть ошибку (502),
	// а не случайно отдать конфиг acme.
	token := testToken(t, f.key, "beta", nil)

	status, _ := doRequest(t, f, http.MethodGet, "/v1/self-service/applications", token, "")
	if status != http.StatusBadGateway {
		t.Fatalf("ожидали 502 для партнёра без конфига, получили %d", status)
	}
}

func TestSenderLifecycleCreateThenArchive(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	createBody := `{"sender_id":"5252","type":"SHORT_NUMBER"}`
	status, respBody := doRequest(t, f, http.MethodPost, "/v1/self-service/senders", token, createBody)
	if status != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d: %s", status, respBody)
	}
	var created sender
	if err := json.Unmarshal(respBody, &created); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if created.Status != "active" {
		t.Fatalf("ожидали status=active на создании, получили %q", created.Status)
	}

	archiveBody := `{"status":"archived"}`
	status, respBody = doRequest(t, f, http.MethodPatch, "/v1/self-service/senders/5252", token, archiveBody)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200 на архивацию, получили %d: %s", status, respBody)
	}
	var archived sender
	if err := json.Unmarshal(respBody, &archived); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if archived.Status != "archived" {
		t.Fatalf("ожидали status=archived, получили %q", archived.Status)
	}
}

func TestUpdateSenderStatusUnknownSenderReturns404(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	status, _ := doRequest(t, f, http.MethodPatch, "/v1/self-service/senders/DOES_NOT_EXIST", token, `{"status":"archived"}`)
	if status != http.StatusNotFound {
		t.Fatalf("ожидали 404, получили %d", status)
	}
}

func TestWebhookPutThenGetRoundTrips(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	putBody := `{"notification_callback_url":"https://partner.example.com/notify"}`
	status, respBody := doRequest(t, f, http.MethodPut, "/v1/self-service/applications/app1/webhook", token, putBody)
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, respBody)
	}

	status, respBody = doRequest(t, f, http.MethodGet, "/v1/self-service/applications/app1/webhook", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, respBody)
	}
	var got struct {
		NotificationCallbackURL string `json:"notification_callback_url"`
	}
	if err := json.Unmarshal(respBody, &got); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.NotificationCallbackURL != "https://partner.example.com/notify" {
		t.Fatalf("webhook URL не сохранился: %q", got.NotificationCallbackURL)
	}
}

func TestWebhookPutRejectsNonHTTPScheme(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	token := testToken(t, f.key, "acme", []string{"partner-admin"})

	status, _ := doRequest(t, f, http.MethodPut, "/v1/self-service/applications/app1/webhook", token, `{"notification_callback_url":"file:///etc/passwd"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("ожидали 400 на не-http(s) схему, получили %d", status)
	}
}

func TestWebhookTestSendHitsConfiguredURL(t *testing.T) {
	f := newTestFixture(t)

	var receivedPath string
	echoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusTeapot)
	}))
	defer echoSrv.Close()

	seeded := strings.Replace(seedPartnerConfig, `"allowed_channels": ["SMS"]`,
		fmt.Sprintf(`"allowed_channels": ["SMS"], "notification_callback_url": %q`, echoSrv.URL), 1)
	f.config.seed("acme", []byte(seeded))
	token := testToken(t, f.key, "acme", nil)

	status, respBody := doRequest(t, f, http.MethodPost, "/v1/self-service/applications/app1/webhook/test", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, respBody)
	}
	var result struct {
		HTTPStatus int   `json:"http_status"`
		LatencyMs  int64 `json:"latency_ms"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.HTTPStatus != http.StatusTeapot {
		t.Fatalf("ожидали http_status=418 (эхо от тестового сервера), получили %d", result.HTTPStatus)
	}
	if receivedPath != "/" {
		t.Fatalf("test-send не дошёл до настроенного URL, receivedPath=%q", receivedPath)
	}
}

func TestListCredentialsScopedToPartner(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))
	f.cred.issuedSecrets = []*grpcv1.IssuedSecretSummary{
		{Id: 1, PartnerId: "acme", ApplicationId: "app1", CredentialRef: "vault://partners/acme/app1/api_key", Status: "active"},
		{Id: 2, PartnerId: "other", ApplicationId: "appX", CredentialRef: "vault://partners/other/appX/api_key", Status: "active"},
	}
	token := testToken(t, f.key, "acme", nil)

	status, respBody := doRequest(t, f, http.MethodGet, "/v1/self-service/credentials", token, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, respBody)
	}
	var secrets []*grpcv1.IssuedSecretSummary
	if err := json.Unmarshal(respBody, &secrets); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(secrets) != 1 || secrets[0].GetPartnerId() != "acme" {
		t.Fatalf("ожидали ровно 1 секрет партнёра acme, получили %+v", secrets)
	}
}

func TestRotateCredentialRequiresAdminAndReturnsPlaintextOnce(t *testing.T) {
	f := newTestFixture(t)
	f.config.seed("acme", []byte(seedPartnerConfig))

	viewerToken := testToken(t, f.key, "acme", nil)
	status, _ := doRequest(t, f, http.MethodPost, "/v1/self-service/applications/app1/credentials/rotate", viewerToken, "")
	if status != http.StatusForbidden {
		t.Fatalf("ожидали 403 без роли partner-admin, получили %d", status)
	}

	adminToken := testToken(t, f.key, "acme", []string{"partner-admin"})
	status, respBody := doRequest(t, f, http.MethodPost, "/v1/self-service/applications/app1/credentials/rotate", adminToken, "")
	if status != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d: %s", status, respBody)
	}
	var resp grpcv1.RotateCredentialResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if resp.GetPlaintextSecret() == "" {
		t.Fatalf("ожидали непустой plaintext_secret в ответе на rotate")
	}
	if len(f.cred.rotateCalls) != 1 || f.cred.rotateCalls[0].GetPartnerId() != "acme" {
		t.Fatalf("RotateCredential вызван с неверным partner_id: %+v", f.cred.rotateCalls)
	}
}
