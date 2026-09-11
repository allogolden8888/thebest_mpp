// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware (JWT, тестовый ключ) + реальные gRPC-клиенты поверх
// bufconn (in-process transport, не мок интерфейса клиента — тот же
// принцип, что InProcessServerBuilder в Java-сервисах этой сессии) с
// фейковыми реализациями ConfigServiceServer/ExecutionControlServiceServer/
// ReplayServiceServer/IamServiceServer.
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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jackc/pgx/v5/pgxpool"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
	"mpp/backoffice-api/internal/store"
)

func newTestRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация RSA-ключа failed: %v", err)
	}
	return key
}

// testToken — kid проставляется через auth.KeyID(&key.PublicKey), та же
// логика, что реальный auth.TokenIssuer.Issue (JWKS/kid rotation,
// internal/auth/keys.go) — auth.NewValidator(&key.PublicKey) в этом файле
// считает kid тем же способом, иначе токены отвергались бы с ErrMissingKid/
// ErrUnknownKid.
func testToken(t *testing.T, key *rsa.PrivateKey, subject string) string {
	t.Helper()
	claims := jwt.RegisteredClaims{Subject: subject, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = auth.KeyID(&key.PublicKey)
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("подпись тестового токена failed: %v", err)
	}
	return signed
}

// fakeConfigServer — фиксирует последний CreateVersionRequest.RequestedBy,
// чтобы проверить, что requested_by реально приходит из JWT, не из тела запроса.
type fakeConfigServer struct {
	grpcv1.UnimplementedConfigServiceServer
	lastRequestedBy string

	// validateValid/validateErrors — luminous-hugging-charm.md Ф10, controls
	// ValidateVersion's canned response for router-level tests.
	validateValid  bool
	validateErrors []string
	diffNotFound   bool
}

func (f *fakeConfigServer) CreateVersion(ctx context.Context, req *grpcv1.CreateVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	f.lastRequestedBy = req.GetRequestedBy()
	return &grpcv1.ConfigVersionResponse{
		EntityType: req.GetEntityType(),
		EntityId:   req.GetEntityId(),
		Version:    1,
		Status:     "ACTIVE",
	}, nil
}

func (f *fakeConfigServer) ValidateVersion(ctx context.Context, req *grpcv1.ValidateVersionRequest) (*grpcv1.ValidateVersionResponse, error) {
	return &grpcv1.ValidateVersionResponse{Valid: f.validateValid, Errors: f.validateErrors}, nil
}

func (f *fakeConfigServer) DiffVersions(ctx context.Context, req *grpcv1.DiffVersionsRequest) (*grpcv1.DiffVersionsResponse, error) {
	if f.diffNotFound {
		return nil, status.Error(codes.NotFound, "version not found")
	}
	return &grpcv1.DiffVersionsResponse{
		FromVersion: req.GetFromVersion(), FromPayloadJson: []byte(`{"n":1}`),
		ToVersion: req.GetToVersion(), ToPayloadJson: []byte(`{"n":2}`),
	}, nil
}

type fakeExecutionControlServer struct {
	grpcv1.UnimplementedExecutionControlServiceServer
}

func (f *fakeExecutionControlServer) ApplyOverride(ctx context.Context, req *grpcv1.ApplyOverrideRequest) (*grpcv1.ApplyOverrideResponse, error) {
	return &grpcv1.ApplyOverrideResponse{Version: 7}, nil
}

type fakeReplayServer struct {
	grpcv1.UnimplementedReplayServiceServer
}

func (f *fakeReplayServer) RequestReplay(ctx context.Context, req *grpcv1.RequestReplayRequest) (*grpcv1.RequestReplayResponse, error) {
	if req.GetStageExecutionId() == "reject-me" {
		return &grpcv1.RequestReplayResponse{Accepted: false, RejectionReason: "TTL_EXPIRED"}, nil
	}
	return &grpcv1.RequestReplayResponse{Accepted: true}, nil
}

// fakeIamServer — минимальная in-memory реализация grpcv1.IamServiceServer
// для роутер-тестов: CheckPermission решает по explicit grant-таблице
// (allow), заведённой тестом на конкретный (external_id, permission), а не
// разбором realm_access.roles — ровно то, что делает настоящий iam-service
// (services/iam-service/internal/store/store.go CheckPermission), только
// без реального Postgres. ListRoles/ListStaffAssignments/AssignStaffRole/
// RevokeStaffRole — упрощённый, но семантически верный in-memory CRUD
// (AlreadyExists/NotFound те же коды, что и настоящий сервер,
// iam-service/internal/grpcserver/server.go).
type fakeIamServer struct {
	grpcv1.UnimplementedIamServiceServer

	mu     sync.Mutex
	grants map[string]map[string]bool
	roles  []*grpcv1.Role

	assignments []*grpcv1.StaffAssignment
	nextID      int64

	// staffCredentials — luminous-hugging-charm.md Экран 33 "Admin users":
	// username -> {password, external_id}. Реальное bcrypt-сравнение живёт
	// в iam-service/internal/store (проверено там же отдельными Postgres-
	// тестами) — здесь фейк только эмулирует итог VerifyStaffCredentials,
	// не переизобретает хеширование.
	staffCredentials map[string]struct {
		password   string
		externalID string
	}
	staffAccounts []*grpcv1.StaffAccount

	// partnerPortalUsers — BACKOFFICE_ROADMAP.md Production Readiness
	// Review P0#5 — тот же класс in-memory CRUD, что staffAccounts выше, но
	// для iam.partner_portal_users (handleIamCreatePartnerPortalUser/
	// handleIamListPartnerPortalUsers/handleIamDeactivatePartnerPortalUser,
	// iam.go).
	partnerPortalUsers []*grpcv1.PartnerPortalUser
}

func (f *fakeIamServer) CreatePartnerPortalUser(ctx context.Context, req *grpcv1.CreatePartnerPortalUserRequest) (*grpcv1.CreatePartnerPortalUserResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.partnerPortalUsers {
		if u.GetUsername() == req.GetUsername() {
			return nil, status.Error(codes.AlreadyExists, "username уже занят")
		}
	}
	u := &grpcv1.PartnerPortalUser{
		ExternalId:  req.GetUsername(),
		Username:    req.GetUsername(),
		PartnerId:   req.GetPartnerId(),
		DisplayName: req.GetDisplayName(),
		Active:      true,
	}
	f.partnerPortalUsers = append(f.partnerPortalUsers, u)
	return &grpcv1.CreatePartnerPortalUserResponse{User: u}, nil
}

func (f *fakeIamServer) ListPartnerPortalUsers(ctx context.Context, req *grpcv1.ListPartnerPortalUsersRequest) (*grpcv1.ListPartnerPortalUsersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*grpcv1.PartnerPortalUser
	for _, u := range f.partnerPortalUsers {
		if req.GetPartnerId() == "" || u.GetPartnerId() == req.GetPartnerId() {
			out = append(out, u)
		}
	}
	return &grpcv1.ListPartnerPortalUsersResponse{Users: out}, nil
}

func (f *fakeIamServer) DeactivatePartnerPortalUser(ctx context.Context, req *grpcv1.DeactivatePartnerPortalUserRequest) (*grpcv1.DeactivatePartnerPortalUserResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.partnerPortalUsers {
		if u.GetExternalId() == req.GetExternalId() && u.GetActive() {
			u.Active = false
			return &grpcv1.DeactivatePartnerPortalUserResponse{Deactivated: true}, nil
		}
	}
	return &grpcv1.DeactivatePartnerPortalUserResponse{Deactivated: false}, nil
}

func (f *fakeIamServer) setStaffCredentials(username, password, externalID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staffCredentials == nil {
		f.staffCredentials = map[string]struct {
			password   string
			externalID string
		}{}
	}
	f.staffCredentials[username] = struct {
		password   string
		externalID string
	}{password, externalID}
}

func (f *fakeIamServer) VerifyStaffCredentials(ctx context.Context, req *grpcv1.VerifyStaffCredentialsRequest) (*grpcv1.VerifyStaffCredentialsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cred, ok := f.staffCredentials[req.GetUsername()]
	if !ok || cred.password != req.GetPassword() {
		return &grpcv1.VerifyStaffCredentialsResponse{Ok: false}, nil
	}
	return &grpcv1.VerifyStaffCredentialsResponse{Ok: true, ExternalId: cred.externalID}, nil
}

func (f *fakeIamServer) CreateStaffAccount(ctx context.Context, req *grpcv1.CreateStaffAccountRequest) (*grpcv1.CreateStaffAccountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := &grpcv1.StaffAccount{ExternalId: req.GetUsername(), Username: req.GetUsername(), DisplayName: req.GetDisplayName(), Active: true}
	f.staffAccounts = append(f.staffAccounts, a)
	return &grpcv1.CreateStaffAccountResponse{Account: a}, nil
}

func (f *fakeIamServer) ListStaffAccounts(ctx context.Context, req *grpcv1.ListStaffAccountsRequest) (*grpcv1.ListStaffAccountsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*grpcv1.StaffAccount
	for _, a := range f.staffAccounts {
		if !req.GetActiveOnly() || a.GetActive() {
			out = append(out, a)
		}
	}
	return &grpcv1.ListStaffAccountsResponse{Accounts: out}, nil
}

func (f *fakeIamServer) DeactivateStaffAccount(ctx context.Context, req *grpcv1.DeactivateStaffAccountRequest) (*grpcv1.DeactivateStaffAccountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.staffAccounts {
		if a.GetExternalId() == req.GetExternalId() && a.GetActive() {
			a.Active = false
			return &grpcv1.DeactivateStaffAccountResponse{Deactivated: true}, nil
		}
	}
	return &grpcv1.DeactivateStaffAccountResponse{Deactivated: false}, nil
}

func newFakeIamServer() *fakeIamServer {
	return &fakeIamServer{
		grants: map[string]map[string]bool{},
		roles: []*grpcv1.Role{
			{Id: 1, Name: "ops-viewer", Description: "только просмотр ops-видимости", Permissions: []string{"ops:read"}},
			{Id: 2, Name: "backoffice-admin", Description: "полный административный доступ", Permissions: []string{
				"config:write", "execution-control:write", "scheduler:force", "replay:request", "audit:read", "iam:manage",
			}},
		},
	}
}

// allow — тестовый helper: даёт external_id указанные права (эмулирует
// активное назначение роли, несущей эти права, без моделирования самих
// ролей — CheckPermission настоящего iam-service тоже в конечном счёте
// проверяет именно множество прав, не имя роли).
func (f *fakeIamServer) allow(externalID string, permissions ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.grants[externalID] == nil {
		f.grants[externalID] = map[string]bool{}
	}
	for _, p := range permissions {
		f.grants[externalID][p] = true
	}
}

func (f *fakeIamServer) CheckPermission(ctx context.Context, req *grpcv1.CheckPermissionRequest) (*grpcv1.CheckPermissionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	allowed := f.grants[req.GetExternalId()] != nil && f.grants[req.GetExternalId()][req.GetPermission()]
	var roles []string
	if allowed {
		roles = []string{"test-role"}
	}
	return &grpcv1.CheckPermissionResponse{Allowed: allowed, Roles: roles}, nil
}

func (f *fakeIamServer) ListRoles(ctx context.Context, _ *grpcv1.ListRolesRequest) (*grpcv1.ListRolesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &grpcv1.ListRolesResponse{Roles: f.roles}, nil
}

func (f *fakeIamServer) ListStaffAssignments(ctx context.Context, req *grpcv1.ListStaffAssignmentsRequest) (*grpcv1.ListStaffAssignmentsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*grpcv1.StaffAssignment
	for _, a := range f.assignments {
		if req.GetExternalId() == "" || a.GetExternalId() == req.GetExternalId() {
			out = append(out, a)
		}
	}
	return &grpcv1.ListStaffAssignmentsResponse{Assignments: out}, nil
}

func (f *fakeIamServer) AssignStaffRole(ctx context.Context, req *grpcv1.AssignStaffRoleRequest) (*grpcv1.AssignStaffRoleResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.assignments {
		if a.GetExternalId() == req.GetExternalId() && a.GetRole() == req.GetRole() {
			return nil, status.Error(codes.AlreadyExists, "роль уже назначена этому пользователю")
		}
	}
	found := false
	for _, r := range f.roles {
		if r.GetName() == req.GetRole() {
			found = true
		}
	}
	if !found {
		return nil, status.Error(codes.NotFound, "роль не найдена")
	}
	f.nextID++
	a := &grpcv1.StaffAssignment{
		Id:         f.nextID,
		ExternalId: req.GetExternalId(),
		Role:       req.GetRole(),
		GrantedBy:  req.GetGrantedBy(),
		GrantedAt:  timestamppb.New(time.Now()),
	}
	f.assignments = append(f.assignments, a)
	return &grpcv1.AssignStaffRoleResponse{Assignment: a}, nil
}

func (f *fakeIamServer) RevokeStaffRole(ctx context.Context, req *grpcv1.RevokeStaffRoleRequest) (*grpcv1.RevokeStaffRoleResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, a := range f.assignments {
		if a.GetExternalId() == req.GetExternalId() && a.GetRole() == req.GetRole() {
			f.assignments = append(f.assignments[:i], f.assignments[i+1:]...)
			return &grpcv1.RevokeStaffRoleResponse{Revoked: true}, nil
		}
	}
	return &grpcv1.RevokeStaffRoleResponse{Revoked: false}, nil
}

// fakeCredentialIssuerServer — минимальная in-memory реализация
// grpcv1.CredentialIssuerServiceServer для роутер-тестов, тот же принцип,
// что fakeIamServer выше: RotateCredential фиксирует последний
// RotateCredentialRequest (в частности IssuedBy) для проверки, что он
// реально приходит из JWT, не из тела запроса (у POST .../rotate вообще
// нет тела запроса в контракте — см. credentials.go package doc), и
// поддерживает специальные partner_id-триггеры для маппинга кодов ошибок
// (writeCredentialIssuerGRPCError, credentials.go).
type fakeCredentialIssuerServer struct {
	grpcv1.UnimplementedCredentialIssuerServiceServer

	mu          sync.Mutex
	lastRequest *grpcv1.RotateCredentialRequest
	secrets     map[string][]*grpcv1.IssuedSecretSummary
	nextVersion int32
}

func newFakeCredentialIssuerServer() *fakeCredentialIssuerServer {
	return &fakeCredentialIssuerServer{
		secrets:     map[string][]*grpcv1.IssuedSecretSummary{},
		nextVersion: 1,
	}
}

func (f *fakeCredentialIssuerServer) RotateCredential(ctx context.Context, req *grpcv1.RotateCredentialRequest) (*grpcv1.RotateCredentialResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastRequest = req

	switch req.GetPartnerId() {
	case "unknown-partner":
		return nil, status.Error(codes.NotFound, "партнёр не найден")
	case "bad-credential-ref":
		return nil, status.Error(codes.FailedPrecondition, "credential_ref не парсится ожидаемой схемой")
	case "vault-down":
		return nil, status.Error(codes.Unavailable, "запись в Vault не удалась")
	case "boom":
		return nil, status.Error(codes.Internal, "рассинхронизация Vault/Postgres")
	}

	version := f.nextVersion
	f.nextVersion++
	now := time.Now()
	ref := "vault://partners/" + req.GetPartnerId() + "/" + req.GetApplicationId() + "/api_key"
	f.secrets[req.GetPartnerId()] = append(f.secrets[req.GetPartnerId()], &grpcv1.IssuedSecretSummary{
		Id:            int64(version),
		PartnerId:     req.GetPartnerId(),
		ApplicationId: req.GetApplicationId(),
		CredentialRef: ref,
		SecretVersion: version,
		Status:        "active",
		IssuedAt:      timestamppb.New(now),
		IssuedBy:      req.GetIssuedBy(),
	})
	return &grpcv1.RotateCredentialResponse{
		CredentialRef:   ref,
		SecretVersion:   version,
		PlaintextSecret: "s3cr3t-plaintext-value",
		IssuedAt:        timestamppb.New(now),
	}, nil
}

func (f *fakeCredentialIssuerServer) ListIssuedSecrets(ctx context.Context, req *grpcv1.ListIssuedSecretsRequest) (*grpcv1.ListIssuedSecretsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &grpcv1.ListIssuedSecretsResponse{Secrets: f.secrets[req.GetPartnerId()]}, nil
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

// testPostgres — реальный локальный Postgres (та же DSN-конвенция, что
// store/postgres_test.go testPool), нужен только для /v1/audit тестов, где
// handleAuditBrowse читает store.Postgres напрямую SQL'ем, а не через
// bufconn-фейк. Пропускает (не проваливает) тест, если Postgres недоступен
// в окружении — тот же safety net, что и store-пакет, при том что в этой
// сессии Postgres реально поднят и миграции применены.
func testPostgres(t *testing.T) *store.Postgres {
	t.Helper()
	dsn := os.Getenv("BACKOFFICE_API_TEST_DSN")
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
	t.Cleanup(pool.Close)
	return store.NewPostgres(pool)
}

func testDeps(t *testing.T) (Deps, *fakeConfigServer, *fakeIamServer, *fakeCredentialIssuerServer) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}

	configFake := &fakeConfigServer{}
	configConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, configFake) })
	ecConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterExecutionControlServiceServer(s, &fakeExecutionControlServer{}) })
	replayConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterReplayServiceServer(s, &fakeReplayServer{}) })

	iamFake := newFakeIamServer()
	iamConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterIamServiceServer(s, iamFake) })

	credentialIssuerFake := newFakeCredentialIssuerServer()
	credentialIssuerConn := dialBufconn(t, func(s *grpc.Server) {
		grpcv1.RegisterCredentialIssuerServiceServer(s, credentialIssuerFake)
	})

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return Deps{
		Validator:              auth.NewValidator(&key.PublicKey),
		ConfigClient:           grpcv1.NewConfigServiceClient(configConn),
		ExecutionControl:       grpcv1.NewExecutionControlServiceClient(ecConn),
		Replay:                 grpcv1.NewReplayServiceClient(replayConn),
		IamClient:              grpcv1.NewIamServiceClient(iamConn),
		CredentialIssuerClient: grpcv1.NewCredentialIssuerServiceClient(credentialIssuerConn),
		TracerProvider:         tp,
	}, configFake, iamFake, credentialIssuerFake
}

func TestHandleConfigCreateVersionUsesRequestedByFromJWT(t *testing.T) {
	deps, configFake, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "config:write")

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")
	body := `{"entity_type":"CONFIG_ENTITY_TYPE_PIPELINE","entity_id":"pl-1","payload_json":{"a":1}}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/config/versions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
	if configFake.lastRequestedBy != "ops@mpp" {
		t.Fatalf("requested_by = %q, want ops@mpp (из JWT, не из тела)", configFake.lastRequestedBy)
	}

	var out configVersionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.EntityID != "pl-1" || out.Version != 1 {
		t.Fatalf("неверный ответ: %+v", out)
	}
}

func TestHandleExecutionControlApplyOverrideEndToEnd(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "execution-control:write")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")
	body := `{"scope":"EXECUTION_CONTROL_SCOPE_GLOBAL","scope_id":"","state":"EXECUTION_CONTROL_STATE_PAUSED","admission_rate":0,"reason":"инцидент"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/execution-control/override", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
}

func TestHandleReplayRequestRejection(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "replay:request")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")
	body := `{"stage_execution_id":"reject-me"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/replay", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var out struct {
		Accepted        bool   `json:"accepted"`
		RejectionReason string `json:"rejection_reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.Accepted || out.RejectionReason != "TTL_EXPIRED" {
		t.Fatalf("неверный ответ: %+v", out)
	}
}

func TestHandleForceSchedulerCommandRejectsUnknownTaskType(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "scheduler:force")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")
	body := `{"stage_execution_id":"abc","task_type":"BOGUS"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/scheduler/force-command", strings.NewReader(body))
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

// TestNonPrivilegedTokenRejectedOnDestructiveRoutes — CODE_REVIEW.md
// CRITICAL finding: раньше ЛЮБОЙ валидный токен realm'а (включая read-only
// support-аккаунт без единой роли) мог поставить платформу на паузу через
// execution-control override. Проверяем, что токен без прав в IAM Service
// получает 403 на всех деструктивных/административных маршрутах, а
// read-only browse остаётся доступным.
func TestNonPrivilegedTokenRejectedOnDestructiveRoutes(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "readonly@mpp") // без прав вообще (fakeIamServer.grants пуст)

	destructive := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/v1/config/versions", `{"entity_type":"CONFIG_ENTITY_TYPE_PIPELINE","entity_id":"pl-1","payload_json":{}}`},
		{http.MethodPost, "/v1/config/versions/archive", `{"entity_type":"CONFIG_ENTITY_TYPE_PIPELINE","entity_id":"pl-1","version":1}`},
		{http.MethodPost, "/v1/execution-control/override", `{"scope":"EXECUTION_CONTROL_SCOPE_GLOBAL","state":"EXECUTION_CONTROL_STATE_PAUSED","reason":"x"}`},
		{http.MethodPost, "/v1/execution-control/override/clear", `{"scope":"EXECUTION_CONTROL_SCOPE_GLOBAL"}`},
		{http.MethodPost, "/v1/scheduler/force-command", `{"stage_execution_id":"abc","task_type":"CRITICAL_COMMAND_TYPE_FORCE_RETRY","reason":"x"}`},
		{http.MethodPost, "/v1/replay", `{"stage_execution_id":"abc"}`},
		{http.MethodGet, "/v1/audit", ""},
		{http.MethodGet, "/v1/iam/roles", ""},
		{http.MethodGet, "/v1/iam/staff-assignments", ""},
		{http.MethodPost, "/v1/iam/staff-assignments", `{"external_id":"x","role":"ops-viewer"}`},
		{http.MethodPost, "/v1/partners/click_uz/applications/main/credentials/rotate", ""},
		{http.MethodGet, "/v1/partners/click_uz/credentials", ""},
		{http.MethodGet, "/v1/support/messages/search?trace_id=x", ""},
	}

	for _, tc := range destructive {
		var bodyReader *strings.Reader
		if tc.body != "" {
			bodyReader = strings.NewReader(tc.body)
		} else {
			bodyReader = strings.NewReader("")
		}
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, bodyReader)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: request failed: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s %s: ожидали 403 без нужного права, получили %d", tc.method, tc.path, resp.StatusCode)
		}
	}

	// Read-only маршрут должен остаться доступным без каких-либо прав.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/config/versions?entity_type=CONFIG_ENTITY_TYPE_PIPELINE&entity_id=pl-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("read-only request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("read-only маршрут не должен требовать права, получили 403")
	}

	// GET /v1/me тоже должен остаться доступным без каких-либо прав (это
	// эндпоинт "какой у МЕНЯ доступ", по определению открыт всем валидным
	// токенам).
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("/v1/me request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/v1/me не должен требовать права, получили %d", resp.StatusCode)
	}
}

// TestHandleMeReturnsRolesAndUnionOfPermissions — /v1/me: ListStaffAssignments
// -> роли вызывающего, ListRoles -> права каждой роли, объединение без
// дублей.
func TestHandleMeReturnsRolesAndUnionOfPermissions(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.assignments = append(iamFake.assignments, &grpcv1.StaffAssignment{
		Id: 1, ExternalId: "ops-viewer@mpp", Role: "ops-viewer", GrantedBy: "admin@mpp", GrantedAt: timestamppb.New(time.Now()),
	})
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops-viewer@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out meResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.ExternalID != "ops-viewer@mpp" {
		t.Fatalf("external_id = %q, want ops-viewer@mpp", out.ExternalID)
	}
	if len(out.Roles) != 1 || out.Roles[0] != "ops-viewer" {
		t.Fatalf("roles = %+v, want [ops-viewer]", out.Roles)
	}
	if len(out.Permissions) != 1 || out.Permissions[0] != "ops:read" {
		t.Fatalf("permissions = %+v, want [ops:read]", out.Permissions)
	}
}

// TestHandleAuditBrowseHappyPath — /v1/audit против реального Postgres:
// проверяет только форму ответа (permission-gate уже покрыт
// TestNonPrivilegedTokenRejectedOnDestructiveRoutes) и что 200/валидный JSON
// возвращается для держателя audit:read. Полная проверка SQL-агрегации по
// всем источникам — store/postgres_test.go TestAuditBrowseMergesAndSortsAcrossSources.
func TestHandleAuditBrowseHappyPath(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	deps.Postgres = testPostgres(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("auditor@mpp", "audit:read")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "auditor@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/audit?limit=5", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out struct {
		Entries    []auditEntryResponse `json:"entries"`
		NextOffset *int                 `json:"next_offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(out.Entries) > 5 {
		t.Fatalf("limit не соблюдён: получили %d записей", len(out.Entries))
	}
}

// TestHandleIamRolesAndStaffAssignmentsRoundTrip — /v1/iam/*: список ролей,
// назначение, повторное назначение -> 409, список назначений содержит
// новое, отзыв -> revoked=true, повторный отзыв -> revoked=false (не
// ошибка, идемпотентно). granted_by/revoked_by берутся из JWT, не из тела.
func TestHandleIamRolesAndStaffAssignmentsRoundTrip(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("admin@mpp", "iam:manage")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "admin@mpp")

	// GET /v1/iam/roles
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/iam/roles", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/iam/roles failed: %v", err)
	}
	var rolesOut struct {
		Roles []iamRoleResponse `json:"roles"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rolesOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	resp.Body.Close()
	if len(rolesOut.Roles) != 2 {
		t.Fatalf("roles = %+v, want 2 роли (см. newFakeIamServer)", rolesOut.Roles)
	}

	// POST /v1/iam/staff-assignments — granted_by должен взяться из JWT,
	// даже если тело пытается его подделать (поля granted_by в теле нет
	// в контракте вообще, но проверяем, что сервер не читает произвольные
	// лишние поля как granted_by).
	assignBody := `{"external_id":"newstaff@mpp","role":"ops-viewer"}`
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/staff-assignments", strings.NewReader(assignBody))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/iam/staff-assignments failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d", resp.StatusCode)
	}
	var assignOut struct {
		Assignment iamStaffAssignmentResponse `json:"assignment"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&assignOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	resp.Body.Close()
	if assignOut.Assignment.GrantedBy != "admin@mpp" {
		t.Fatalf("granted_by = %q, want admin@mpp (из JWT, не из тела)", assignOut.Assignment.GrantedBy)
	}
	if assignOut.Assignment.ExternalID != "newstaff@mpp" || assignOut.Assignment.Role != "ops-viewer" {
		t.Fatalf("неверное назначение: %+v", assignOut.Assignment)
	}

	// Повторное назначение той же (external_id, role) -> 409.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/staff-assignments", strings.NewReader(assignBody))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("повторный POST failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("ожидали 409 на повторное назначение, получили %d", resp.StatusCode)
	}

	// GET /v1/iam/staff-assignments?external_id= — фильтр находит новое назначение.
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v1/iam/staff-assignments?external_id=newstaff@mpp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET staff-assignments failed: %v", err)
	}
	var listOut struct {
		Assignments []iamStaffAssignmentResponse `json:"assignments"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	resp.Body.Close()
	if len(listOut.Assignments) != 1 || listOut.Assignments[0].ExternalID != "newstaff@mpp" {
		t.Fatalf("неверная фильтрация: %+v", listOut.Assignments)
	}

	// DELETE /v1/iam/staff-assignments/{external_id}/{role} — revoked_by из JWT.
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/v1/iam/staff-assignments/newstaff@mpp/ops-viewer", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	var revokeOut struct {
		Revoked bool `json:"revoked"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&revokeOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	resp.Body.Close()
	if !revokeOut.Revoked {
		t.Fatalf("revoked = false, want true (первый отзыв реально существующего назначения)")
	}

	// Повторный отзыв — нечего отзывать, revoked=false, НЕ ошибка.
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/v1/iam/staff-assignments/newstaff@mpp/ops-viewer", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("повторный DELETE failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("повторный отзыв не должен быть ошибкой, получили %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&revokeOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	resp.Body.Close()
	if revokeOut.Revoked {
		t.Fatalf("revoked = true при повторном отзыве, want false (идемпотентность)")
	}
}

// TestHandleCredentialsRotateHappyPath — POST .../credentials/rotate:
// partner_id/application_id приходят из пути, issued_by — из JWT (тело
// запроса вообще не отправляется, credentials.go package doc), ответ несёт
// plaintext_secret (show-once, только в этом ответе).
func TestHandleCredentialsRotateHappyPath(t *testing.T) {
	deps, _, iamFake, credentialIssuerFake := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "credentials:issue")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/partners/click_uz/applications/main/credentials/rotate", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out rotateCredentialResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.CredentialRef != "vault://partners/click_uz/main/api_key" {
		t.Fatalf("credential_ref = %q, неверный", out.CredentialRef)
	}
	if out.SecretVersion != 1 {
		t.Fatalf("secret_version = %d, want 1", out.SecretVersion)
	}
	if out.PlaintextSecret == "" {
		t.Fatalf("plaintext_secret пуст, want непустое значение (show-once)")
	}

	// partner_id/application_id — из пути, issued_by — из JWT, не из тела
	// (тела вообще не было).
	last := credentialIssuerFake.lastRequest
	if last.GetPartnerId() != "click_uz" || last.GetApplicationId() != "main" {
		t.Fatalf("неверные partner_id/application_id, дошедшие до gRPC: %+v", last)
	}
	if last.GetIssuedBy() != "ops@mpp" {
		t.Fatalf("issued_by = %q, want ops@mpp (из JWT)", last.GetIssuedBy())
	}
}

// TestHandleCredentialsRotateGRPCErrorMapping — маппинг кодов gRPC-ошибок
// CredentialIssuerService в HTTP-статусы (writeCredentialIssuerGRPCError,
// credentials.go): NotFound->404, FailedPrecondition->422, Unavailable->503,
// прочее (Internal и т.п.)->500.
func TestHandleCredentialsRotateGRPCErrorMapping(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "credentials:issue")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")

	cases := []struct {
		partnerID  string
		wantStatus int
	}{
		{"unknown-partner", http.StatusNotFound},
		{"bad-credential-ref", http.StatusUnprocessableEntity},
		{"vault-down", http.StatusServiceUnavailable},
		{"boom", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/partners/"+tc.partnerID+"/applications/main/credentials/rotate", strings.NewReader(""))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("partner_id=%s: request failed: %v", tc.partnerID, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.wantStatus {
			t.Fatalf("partner_id=%s: ожидали %d, получили %d", tc.partnerID, tc.wantStatus, resp.StatusCode)
		}
	}
}

// TestHandleCredentialsListReturnsIssuedSecrets — GET
// /v1/partners/{partner_id}/credentials: проксирует ListIssuedSecrets,
// форма ответа {"secrets": [...]}, secret_version/status/issued_by
// правильно проброшены. Тот же credentials:issue gate, что и rotate (см.
// router.go комментарий — намеренно одно право на обе операции).
func TestHandleCredentialsListReturnsIssuedSecrets(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("ops@mpp", "credentials:issue")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "ops@mpp")

	// Сначала выпускаем секрет через rotate, чтобы list вернул реальную запись.
	rotateReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/partners/click_uz/applications/main/credentials/rotate", strings.NewReader(""))
	rotateReq.Header.Set("Authorization", "Bearer "+token)
	rotateResp, err := http.DefaultClient.Do(rotateReq)
	if err != nil {
		t.Fatalf("rotate request failed: %v", err)
	}
	rotateResp.Body.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/partners/click_uz/credentials", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out struct {
		Secrets []issuedSecretResponse `json:"secrets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(out.Secrets) != 1 {
		t.Fatalf("secrets = %+v, want 1 запись", out.Secrets)
	}
	s := out.Secrets[0]
	if s.PartnerID != "click_uz" || s.ApplicationID != "main" {
		t.Fatalf("неверные partner_id/application_id: %+v", s)
	}
	if s.SecretVersion != 1 || s.Status != "active" || s.IssuedBy != "ops@mpp" {
		t.Fatalf("неверные поля secret_version/status/issued_by: %+v", s)
	}
}

// TestHandleSupportMessagesSearchRequiresMessageIdOrTraceId — пустой запрос
// (ни message_id, ни trace_id) обязан вернуть 400, а не "верни всю таблицу
// кросс-партнёрски" (store.SupportMessageSearchFilter package doc).
func TestHandleSupportMessagesSearchRequiresMessageIdOrTraceId(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	deps.Postgres = testPostgres(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("support@mpp", "support:trace")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "support@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/support/messages/search", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("ожидали 400 без message_id/trace_id, получили %d", resp.StatusCode)
	}
}

// TestHandleSupportMessagesSearchFindsAcrossPartners — реальный Postgres,
// два разных partner_id, поиск по trace_id одного из них не фильтруется по
// вызывающему (в отличие от partner-api, у которого partner_id намертво
// зашит из JWT) — это и есть luminous-hugging-charm.md Ф9.
func TestHandleSupportMessagesSearchFindsAcrossPartners(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	pg := testPostgres(t)
	deps.Postgres = pg
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("support@mpp", "support:trace")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	messageID := uuid.NewString()
	traceID := uuid.NewString()
	// pg (store.Postgres) doesn't expose its underlying pool for direct
	// inserts — mirror testPostgres's own DSN resolution to open a second
	// connection just for seeding this row, same as other router tests that
	// need to write data testPostgres's wrapper has no method for.
	dsn := os.Getenv("BACKOFFICE_API_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}
	insertCtx := context.Background()
	pool, err := pgxpool.New(insertCtx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New failed: %v", err)
	}
	defer pool.Close()
	_, err = pool.Exec(insertCtx, `
		INSERT INTO messaging.message_read_model (message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, 'other_partner_router_test', 'app', $2, 'p1', '1', 'DELIVERED', true)
	`, messageID, traceID)
	if err != nil {
		t.Fatalf("insert message_read_model failed: %v", err)
	}

	token := testToken(t, key, "support@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/support/messages/search?trace_id="+traceID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out struct {
		Messages []supportMessageResponse `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(out.Messages) != 1 || out.Messages[0].PartnerID != "other_partner_router_test" {
		t.Fatalf("ожидали ровно одно сообщение partner_id=other_partner_router_test, получили %+v", out.Messages)
	}
}

// TestHandleConfigValidateVersionNoPermissionRequired — luminous-hugging-charm.md
// Ф10: POST /v1/config/versions/validate is the one POST route in this
// service with no permission gate (it writes nothing) — a token with zero
// grants must still get 200, not 403.
func TestHandleConfigValidateVersionNoPermissionRequired(t *testing.T) {
	deps, configFake, _, _ := testDeps(t)
	configFake.validateValid = false
	configFake.validateErrors = []string{"payload is not valid JSON"}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp") // без каких-либо прав
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/config/versions/validate",
		strings.NewReader(`{"entity_type":"CONFIG_ENTITY_TYPE_PARTNER","payload_json":{}}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200 (без gate прав), получили %d", resp.StatusCode)
	}

	var out struct {
		Valid  bool     `json:"valid"`
		Errors []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.Valid {
		t.Fatalf("ожидали valid=false")
	}
	if len(out.Errors) != 1 || out.Errors[0] != "payload is not valid JSON" {
		t.Fatalf("errors не пробросились верно: %v", out.Errors)
	}
}

func TestHandleConfigValidateVersionUnknownEntityTypeReturns400(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/config/versions/validate",
		strings.NewReader(`{"entity_type":"NOT_A_REAL_TYPE","payload_json":{}}`))
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

// TestHandleConfigDiffVersionsHappyPath — GET /v1/config/versions/diff,
// no permission gate, returns both payloads as-is.
func TestHandleConfigDiffVersionsHappyPath(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp")
	req, _ := http.NewRequest(http.MethodGet,
		srv.URL+"/v1/config/versions/diff?entity_type=CONFIG_ENTITY_TYPE_PARTNER&entity_id=acme&from=1&to=2", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}

	var out struct {
		FromVersion     int64           `json:"from_version"`
		FromPayloadJSON json.RawMessage `json:"from_payload_json"`
		ToVersion       int64           `json:"to_version"`
		ToPayloadJSON   json.RawMessage `json:"to_payload_json"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.FromVersion != 1 || out.ToVersion != 2 {
		t.Fatalf("неверные номера версий: from=%d to=%d", out.FromVersion, out.ToVersion)
	}
	if string(out.FromPayloadJSON) != `{"n":1}` || string(out.ToPayloadJSON) != `{"n":2}` {
		t.Fatalf("неверные payload: from=%s to=%s", out.FromPayloadJSON, out.ToPayloadJSON)
	}
}

func TestHandleConfigDiffVersionsMissingParamsReturn400(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp")
	cases := []string{
		"/v1/config/versions/diff?entity_id=acme&from=1&to=2",                                        // missing entity_type
		"/v1/config/versions/diff?entity_type=CONFIG_ENTITY_TYPE_PARTNER&from=1&to=2",                // missing entity_id
		"/v1/config/versions/diff?entity_type=CONFIG_ENTITY_TYPE_PARTNER&entity_id=acme&to=2",        // missing from
		"/v1/config/versions/diff?entity_type=CONFIG_ENTITY_TYPE_PARTNER&entity_id=acme&from=0&to=2", // from=0
	}
	for _, path := range cases {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: request failed: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: ожидали 400, получили %d", path, resp.StatusCode)
		}
	}
}

func TestHandleConfigDiffVersionsNotFoundReturns404(t *testing.T) {
	deps, configFake, _, _ := testDeps(t)
	configFake.diffNotFound = true
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp")
	req, _ := http.NewRequest(http.MethodGet,
		srv.URL+"/v1/config/versions/diff?entity_type=CONFIG_ENTITY_TYPE_PARTNER&entity_id=acme&from=1&to=99", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ожидали 404, получили %d", resp.StatusCode)
	}
}

// fakeIncidentServer — минимальная in-memory реализация
// grpcv1.IncidentServiceServer (luminous-hugging-charm.md Ф7). Отдельный
// helper (newIncidentFakeDeps), не встроена в testDeps, чтобы не менять
// сигнатуру testDeps и не трогать десятки существующих вызовов этой
// функции по всему файлу.
type fakeIncidentServer struct {
	grpcv1.UnimplementedIncidentServiceServer
	mu        sync.Mutex
	incidents map[int64]*grpcv1.Incident
	notes     map[int64][]*grpcv1.IncidentNote
	nextID    int64
}

func newFakeIncidentServer() *fakeIncidentServer {
	return &fakeIncidentServer{incidents: map[int64]*grpcv1.Incident{}, notes: map[int64][]*grpcv1.IncidentNote{}}
}

func (f *fakeIncidentServer) OpenIncident(ctx context.Context, req *grpcv1.OpenIncidentRequest) (*grpcv1.Incident, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	inc := &grpcv1.Incident{
		Id: f.nextID, Title: req.GetTitle(), Severity: req.GetSeverity(), Status: "OPEN",
		OpenedBy: req.GetOpenedBy(), OpenedAt: timestamppb.New(time.Now()),
	}
	f.incidents[inc.Id] = inc
	return inc, nil
}

func (f *fakeIncidentServer) ListIncidents(ctx context.Context, req *grpcv1.ListIncidentsRequest) (*grpcv1.ListIncidentsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*grpcv1.Incident
	for _, inc := range f.incidents {
		if req.GetStatus() == "" || inc.GetStatus() == req.GetStatus() {
			out = append(out, inc)
		}
	}
	return &grpcv1.ListIncidentsResponse{Incidents: out}, nil
}

func (f *fakeIncidentServer) GetIncident(ctx context.Context, req *grpcv1.GetIncidentRequest) (*grpcv1.IncidentDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	inc, ok := f.incidents[req.GetIncidentId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "инцидент не найден")
	}
	return &grpcv1.IncidentDetail{Incident: inc, Notes: f.notes[req.GetIncidentId()]}, nil
}

func (f *fakeIncidentServer) AddNote(ctx context.Context, req *grpcv1.AddNoteRequest) (*grpcv1.IncidentNote, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.incidents[req.GetIncidentId()]; !ok {
		return nil, status.Error(codes.NotFound, "инцидент не найден")
	}
	f.nextID++
	note := &grpcv1.IncidentNote{
		Id: f.nextID, IncidentId: req.GetIncidentId(), Author: req.GetAuthor(),
		Note: req.GetNote(), CreatedAt: timestamppb.New(time.Now()),
	}
	f.notes[req.GetIncidentId()] = append(f.notes[req.GetIncidentId()], note)
	return note, nil
}

func (f *fakeIncidentServer) ResolveIncident(ctx context.Context, req *grpcv1.ResolveIncidentRequest) (*grpcv1.Incident, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	inc, ok := f.incidents[req.GetIncidentId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "инцидент не найден")
	}
	if inc.GetStatus() == "RESOLVED" {
		return nil, status.Error(codes.FailedPrecondition, "уже закрыт")
	}
	inc.Status = "RESOLVED"
	inc.ResolvedBy = req.GetResolvedBy()
	inc.ResolvedAt = timestamppb.New(time.Now())
	inc.PostmortemNotes = req.GetPostmortemNotes()
	return inc, nil
}

func newIncidentFakeDeps(t *testing.T) (Deps, *fakeConfigServer, *fakeIamServer, *fakeIncidentServer) {
	deps, configFake, iamFake, _ := testDeps(t)
	incidentFake := newFakeIncidentServer()
	incidentConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterIncidentServiceServer(s, incidentFake) })
	deps.IncidentClient = grpcv1.NewIncidentServiceClient(incidentConn)
	return deps, configFake, iamFake, incidentFake
}

func TestIncidentsRequirePermission(t *testing.T) {
	deps, _, _, _ := newIncidentFakeDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "nobody@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/incidents", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("ожидали 403 без incident:manage, получили %d", resp.StatusCode)
	}
}

// TestIncidentsFullLifecycle — open -> get (timeline+notes) -> add note ->
// resolve -> re-resolve rejected (409). opened_by/author/resolved_by
// проверяются как пришедшие из JWT, не из тела.
func TestIncidentsFullLifecycle(t *testing.T) {
	deps, _, iamFake, _ := newIncidentFakeDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("oncall@mpp", "incident:manage")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "oncall@mpp")
	doReq := func(method, path, body string) *http.Response {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", method, path, err)
		}
		return resp
	}

	openResp := doReq(http.MethodPost, "/v1/incidents", `{"title":"Kafka lag spike","severity":"HIGH"}`)
	if openResp.StatusCode != http.StatusCreated {
		t.Fatalf("open: ожидали 201, получили %d", openResp.StatusCode)
	}
	var opened incidentResponse
	if err := json.NewDecoder(openResp.Body).Decode(&opened); err != nil {
		t.Fatalf("decode open response: %v", err)
	}
	openResp.Body.Close()
	if opened.OpenedBy != "oncall@mpp" {
		t.Fatalf("opened_by = %q, want из JWT oncall@mpp", opened.OpenedBy)
	}

	noteResp := doReq(http.MethodPost, fmt.Sprintf("/v1/incidents/%d/notes", opened.ID), `{"note":"investigating"}`)
	if noteResp.StatusCode != http.StatusCreated {
		t.Fatalf("add note: ожидали 201, получили %d", noteResp.StatusCode)
	}
	var note incidentNoteResponse
	_ = json.NewDecoder(noteResp.Body).Decode(&note)
	noteResp.Body.Close()
	if note.Author != "oncall@mpp" {
		t.Fatalf("author = %q, want из JWT oncall@mpp", note.Author)
	}

	getResp := doReq(http.MethodGet, fmt.Sprintf("/v1/incidents/%d", opened.ID), "")
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get: ожидали 200, получили %d", getResp.StatusCode)
	}
	var detail struct {
		Incident incidentResponse       `json:"incident"`
		Notes    []incidentNoteResponse `json:"notes"`
	}
	_ = json.NewDecoder(getResp.Body).Decode(&detail)
	getResp.Body.Close()
	if len(detail.Notes) != 1 || detail.Notes[0].Note != "investigating" {
		t.Fatalf("ожидали 1 заметку 'investigating', получили %+v", detail.Notes)
	}

	resolveResp := doReq(http.MethodPost, fmt.Sprintf("/v1/incidents/%d/resolve", opened.ID), `{"postmortem_notes":"root cause found and fixed"}`)
	if resolveResp.StatusCode != http.StatusOK {
		t.Fatalf("resolve: ожидали 200, получили %d", resolveResp.StatusCode)
	}
	var resolved incidentResponse
	_ = json.NewDecoder(resolveResp.Body).Decode(&resolved)
	resolveResp.Body.Close()
	if resolved.Status != "RESOLVED" || resolved.ResolvedBy != "oncall@mpp" {
		t.Fatalf("неверный результат resolve: %+v", resolved)
	}

	reResolveResp := doReq(http.MethodPost, fmt.Sprintf("/v1/incidents/%d/resolve", opened.ID), `{"postmortem_notes":"again"}`)
	if reResolveResp.StatusCode != http.StatusConflict {
		t.Fatalf("повторный resolve: ожидали 409, получили %d", reResolveResp.StatusCode)
	}
	reResolveResp.Body.Close()
}

func TestIncidentsGetUnknownReturns404(t *testing.T) {
	deps, _, iamFake, _ := newIncidentFakeDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("oncall@mpp", "incident:manage")
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	token := testToken(t, key, "oncall@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/incidents/999", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ожидали 404, получили %d", resp.StatusCode)
	}
}

// TestHandleOpsSnapshotProxiesBodyAndRequiresPermission — luminous-hugging-charm.md
// Ф8: GET /v1/ops/snapshot — plain HTTP proxy (не gRPC/bufconn, единственный
// такой в этом файле — см. internal/httpapi/ops.go package doc).
func TestHandleOpsSnapshotProxiesBodyAndRequiresPermission(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			t.Errorf("неожиданный путь на upstream: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kafka_lag_available":true,"readyz_available":true}`))
	}))
	defer upstream.Close()

	deps, _, iamFake, _ := testDeps(t)
	deps.HTTPClient = upstream.Client()
	deps.OpsVisibilityURL = upstream.URL
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Без ops:read — 403, upstream не должен быть вызван вообще (недоступно
	// напрямую проверить "не вызван" без счётчика, но статус уже
	// достаточен: RequirePermission блокирует до прокси-хендлера).
	noPermToken := testToken(t, key, "nobody@mpp")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/ops/snapshot", nil)
	req.Header.Set("Authorization", "Bearer "+noPermToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("ожидали 403 без ops:read, получили %d", resp.StatusCode)
	}

	iamFake.allow("ops-viewer@mpp", "ops:read")
	token := testToken(t, key, "ops-viewer@mpp")
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v1/ops/snapshot", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"kafka_lag_available":true`) {
		t.Fatalf("тело не проброшено от upstream как есть: %s", body)
	}
}

func TestHandleLoginHappyPathIssuesUsableToken(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	deps.TokenIssuer = auth.NewTokenIssuer(key)
	iamFake.setStaffCredentials("alice", "correct-password", "alice")
	iamFake.allow("alice", "iam:manage")

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	loginBody := `{"username":"alice","password":"correct-password"}`
	resp, err := http.Post(srv.URL+"/v1/auth/login", "application/json", strings.NewReader(loginBody))
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
	var out loginResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.Token == "" || out.ExpiresAt == "" {
		t.Fatalf("ожидали непустые token/expires_at, получили %+v", out)
	}

	// Токен, выпущенный логином, реально работает на защищённом маршруте —
	// не только "структурно похож на JWT".
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/iam/roles", nil)
	req.Header.Set("Authorization", "Bearer "+out.Token)
	authedResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("authed request failed: %v", err)
	}
	authedResp.Body.Close()
	if authedResp.StatusCode != http.StatusOK {
		t.Fatalf("токен от /v1/auth/login должен проходить iam:manage-защищённый маршрут, получили %d", authedResp.StatusCode)
	}
}

func TestHandleLoginRejectsWrongPassword(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	deps.TokenIssuer = auth.NewTokenIssuer(newTestRSAKey(t))
	iamFake.setStaffCredentials("alice", "correct-password", "alice")

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/auth/login", "application/json", strings.NewReader(`{"username":"alice","password":"wrong"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ожидали 401 на неверный пароль, получили %d", resp.StatusCode)
	}
}

func TestHandleLoginMissingFieldsReturns400(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	deps.TokenIssuer = auth.NewTokenIssuer(newTestRSAKey(t))

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/auth/login", "application/json", strings.NewReader(`{"username":"alice"}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("ожидали 400 без password, получили %d", resp.StatusCode)
	}
}

func TestHandleStaffAccountsCreateListDeactivateRoundTrip(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("admin@mpp", "iam:manage")

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()
	token := testToken(t, key, "admin@mpp")

	createReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/staff-accounts", strings.NewReader(`{"username":"bob","password":"pw","display_name":"Bob"}`))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d", createResp.StatusCode)
	}

	listReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/iam/staff-accounts", nil)
	listReq.Header.Set("Authorization", "Bearer "+token)
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list request failed: %v", err)
	}
	defer listResp.Body.Close()
	var listOut struct {
		Accounts []iamStaffAccountResponse `json:"accounts"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(listOut.Accounts) != 1 || listOut.Accounts[0].Username != "bob" || !listOut.Accounts[0].Active {
		t.Fatalf("неожиданный список: %+v", listOut.Accounts)
	}

	deactivateReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/staff-accounts/bob/deactivate", nil)
	deactivateReq.Header.Set("Authorization", "Bearer "+token)
	deactivateResp, err := http.DefaultClient.Do(deactivateReq)
	if err != nil {
		t.Fatalf("deactivate request failed: %v", err)
	}
	defer deactivateResp.Body.Close()
	var deactivateOut struct {
		Deactivated bool `json:"deactivated"`
	}
	if err := json.NewDecoder(deactivateResp.Body).Decode(&deactivateOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !deactivateOut.Deactivated {
		t.Fatalf("ожидали deactivated=true")
	}
}

// TestHandlePartnerPortalUsersCreateListDeactivateRoundTrip —
// BACKOFFICE_ROADMAP.md Production Readiness Review P0#5: PartnerUsersView.vue
// раньше могло только назначать роль external_id, который предполагался уже
// существующим — эти три маршрута реально ПРОИЗВОДЯТ логинящегося
// партнёрского пользователя. Тот же паттерн, что
// TestHandleStaffAccountsCreateListDeactivateRoundTrip выше.
func TestHandlePartnerPortalUsersCreateListDeactivateRoundTrip(t *testing.T) {
	deps, _, iamFake, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
	iamFake.allow("admin@mpp", "iam:manage")

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()
	token := testToken(t, key, "admin@mpp")

	createReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/partner-portal-users",
		strings.NewReader(`{"username":"acme-bob","password":"pw","partner_id":"acme","display_name":"Bob at Acme"}`))
	createReq.Header.Set("Authorization", "Bearer "+token)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d", createResp.StatusCode)
	}

	listReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/iam/partner-portal-users?partner_id=acme", nil)
	listReq.Header.Set("Authorization", "Bearer "+token)
	listResp, err := http.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list request failed: %v", err)
	}
	defer listResp.Body.Close()
	var listOut struct {
		Users []iamPartnerPortalUserResponse `json:"users"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(listOut.Users) != 1 || listOut.Users[0].Username != "acme-bob" || listOut.Users[0].PartnerID != "acme" || !listOut.Users[0].Active {
		t.Fatalf("неожиданный список: %+v", listOut.Users)
	}

	deactivateReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/iam/partner-portal-users/acme-bob/deactivate", nil)
	deactivateReq.Header.Set("Authorization", "Bearer "+token)
	deactivateResp, err := http.DefaultClient.Do(deactivateReq)
	if err != nil {
		t.Fatalf("deactivate request failed: %v", err)
	}
	defer deactivateResp.Body.Close()
	var deactivateOut struct {
		Deactivated bool `json:"deactivated"`
	}
	if err := json.NewDecoder(deactivateResp.Body).Decode(&deactivateOut); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if !deactivateOut.Deactivated {
		t.Fatalf("ожидали deactivated=true")
	}
}

// TestHandleJWKSPublicNoAuthAndMatchesIssuedToken — GET
// /v1/.well-known/jwks.json (jwks.go, BACKOFFICE_ROADMAP.md P0 "секреты"):
// доступен БЕЗ Authorization (тот же класс исключения, что /v1/auth/login),
// и несёт kid, реально совпадающий с kid токена, выпущенного TokenIssuer'ом
// того же ключа — доказывает, что issuer.go/jwt.go/jwks.go считают kid
// одинаково, не только по отдельности в unit-тестах internal/auth.
func TestHandleJWKSPublicNoAuthAndMatchesIssuedToken(t *testing.T) {
	deps, _, _, _ := testDeps(t)
	key := newTestRSAKey(t)
	deps.Validator = auth.NewValidator(&key.PublicKey)
	issuer := auth.NewTokenIssuer(key)
	deps.TokenIssuer = issuer

	router := NewRouter(deps)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/.well-known/jwks.json")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200 без Authorization, получили %d", resp.StatusCode)
	}

	var out auth.JWKSet
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(out.Keys) != 1 {
		t.Fatalf("ожидали 1 ключ в JWKS, получили %d", len(out.Keys))
	}
	jwk := out.Keys[0]
	if jwk.Kty != "RSA" || jwk.Alg != "RS256" || jwk.Use != "sig" {
		t.Fatalf("неожиданные метаданные JWK: %+v", jwk)
	}
	wantKid := auth.KeyID(&key.PublicKey)
	if jwk.Kid != wantKid {
		t.Fatalf("kid в JWKS = %q, want %q (auth.KeyID того же ключа)", jwk.Kid, wantKid)
	}

	token, _, err := issuer.Issue("alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	claims, err := deps.Validator.ParseBearer("Bearer " + token)
	if err != nil {
		t.Fatalf("токен, выпущенный TokenIssuer'ом этого ключа, должен проходить Validator: %v", err)
	}
	if claims.Subject != "alice" {
		t.Fatalf("sub = %q, want alice", claims.Subject)
	}
}
