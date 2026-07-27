// Тесты роутера — реальный HTTP round-trip (httptest.Server) через chi +
// auth.Middleware (JWT, тестовый ключ) + реальные gRPC-клиенты поверх
// bufconn (in-process transport, не мок интерфейса клиента — тот же
// принцип, что InProcessServerBuilder в Java-сервисах этой сессии) с
// фейковыми реализациями ConfigServiceServer/ExecutionControlServiceServer/
// ReplayServiceServer.
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
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

func testToken(t *testing.T, key *rsa.PrivateKey, subject string) string {
	t.Helper()
	claims := auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: subject, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
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

func testDeps(t *testing.T) (Deps, *fakeConfigServer) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}

	configFake := &fakeConfigServer{}
	configConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterConfigServiceServer(s, configFake) })
	ecConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterExecutionControlServiceServer(s, &fakeExecutionControlServer{}) })
	replayConn := dialBufconn(t, func(s *grpc.Server) { grpcv1.RegisterReplayServiceServer(s, &fakeReplayServer{}) })

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	return Deps{
		Validator:        auth.NewValidator(&key.PublicKey),
		ConfigClient:     grpcv1.NewConfigServiceClient(configConn),
		ExecutionControl: grpcv1.NewExecutionControlServiceClient(ecConn),
		Replay:           grpcv1.NewReplayServiceClient(replayConn),
		TracerProvider:   tp,
	}, configFake
}

func TestHandleConfigCreateVersionUsesRequestedByFromJWT(t *testing.T) {
	deps, configFake := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)

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
	deps, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
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
	deps, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
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
	deps, _ := testDeps(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("генерация ключа failed: %v", err)
	}
	deps.Validator = auth.NewValidator(&key.PublicKey)
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