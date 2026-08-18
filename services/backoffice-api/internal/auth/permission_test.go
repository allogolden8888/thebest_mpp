package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

// fakeIamClient — прямая реализация grpcv1.IamServiceClient с канонными
// ответами/ошибками, без bufconn — RequirePermission вызывает клиент
// напрямую как интерфейс, поднимать реальный gRPC-сервер здесь не нужно
// (bufconn-стиль используется в httpapi/router_test.go, где важен полный
// HTTP->gRPC round-trip; здесь достаточно интерфейсного фейка).
type fakeIamClient struct {
	grpcv1.IamServiceClient // embed nil — паникует, если тест вызовет незаданный метод

	allowed bool
	err     error

	lastReq *grpcv1.CheckPermissionRequest
}

func (f *fakeIamClient) CheckPermission(ctx context.Context, in *grpcv1.CheckPermissionRequest, opts ...grpc.CallOption) (*grpcv1.CheckPermissionResponse, error) {
	f.lastReq = in
	if f.err != nil {
		return nil, f.err
	}
	return &grpcv1.CheckPermissionResponse{Allowed: f.allowed, Roles: []string{"test-role"}}, nil
}

func requestWithClaims(subject string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	ctx := contextWithClaims(req.Context(), &Claims{})
	claims, _ := ClaimsFromContext(ctx)
	claims.Subject = subject
	return req.WithContext(ctx)
}

func TestRequirePermissionPassesThroughWhenAllowed(t *testing.T) {
	fake := &fakeIamClient{allowed: true}
	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handlerCalled = true })

	req := requestWithClaims("ops@mpp")
	rec := httptest.NewRecorder()
	RequirePermission("config:write", fake)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", rec.Code)
	}
	if !handlerCalled {
		t.Fatalf("next должен вызываться, когда allowed=true")
	}
	if fake.lastReq.GetExternalId() != "ops@mpp" || fake.lastReq.GetPermission() != "config:write" {
		t.Fatalf("неверный CheckPermissionRequest: %+v", fake.lastReq)
	}
}

func TestRequirePermissionRejectsWith403WhenNotAllowed(t *testing.T) {
	fake := &fakeIamClient{allowed: false}
	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handlerCalled = true })

	req := requestWithClaims("readonly@mpp")
	rec := httptest.NewRecorder()
	RequirePermission("config:write", fake)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("ожидали 403, получили %d", rec.Code)
	}
	if handlerCalled {
		t.Fatalf("next не должен вызываться, когда allowed=false")
	}
}

// TestRequirePermissionFailsClosedWith503OnGRPCError — важнейший тест этого
// файла: IAM Service недоступен (или таймаут) -> 503, НЕ pass-through.
// Сервис, способный поставить на паузу весь трафик платформы, не может
// по умолчанию проваливаться в "открыто" на ошибке своей же зависимости
// (см. docstring RequirePermission и platform-contracts/grpc/iam.proto
// CheckPermission).
func TestRequirePermissionFailsClosedWith503OnGRPCError(t *testing.T) {
	fake := &fakeIamClient{err: errors.New("iam-service недоступен")}
	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handlerCalled = true })

	req := requestWithClaims("ops@mpp")
	rec := httptest.NewRecorder()
	RequirePermission("config:write", fake)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ожидали 503 (fail-closed), получили %d", rec.Code)
	}
	if handlerCalled {
		t.Fatalf("next не должен вызываться, когда проверка прав недоступна")
	}
}

func TestRequirePermissionRequiresClaimsInContext(t *testing.T) {
	fake := &fakeIamClient{allowed: true}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("next не должен вызываться без claims в контексте")
	})

	req := httptest.NewRequest(http.MethodPost, "/", nil) // без claims
	rec := httptest.NewRecorder()
	RequirePermission("config:write", fake)(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("ожидали 500 (RequirePermission смонтирован до Middleware), получили %d", rec.Code)
	}
}
