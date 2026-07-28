// Package grpcserver реализует mpp.grpc.v1.ExecutionControlService
// (ApplyOverride/ClearOverride) — вызывается Backoffice API (manual
// override) и Billing Reconciliation (freeze/unfreeze, scope=PARTNER_STAGE,
// stage=BILLING, HLD §15.5), platform-contracts/grpc/internal_control.proto.
package grpcserver

import (
	"context"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/execution-control-service/internal/hysteresis"
	"mpp/execution-control-service/internal/kafkaio"
	"mpp/execution-control-service/internal/registry"
	"mpp/execution-control-service/internal/store"
)

// AuditPersister — минимальный интерфейс, который нужен серверу от
// store.AuditStore (позволяет подменять в тестах фейком без реального
// Postgres).
type AuditPersister interface {
	PersistOverrideAudit(ctx context.Context, e store.AuditEntry) (id int64, createdAt time.Time, err error)
}

// Server реализует grpcv1.ExecutionControlServiceServer.
//
// publisher — CODE_REVIEW.md CRITICAL finding: раньше ApplyOverride/
// ClearOverride не были связаны с Kafka вообще; единственное место,
// публикующее в execution.control, был периодический цикл в main.go,
// который эволюционирует только scope=GLOBAL. Override для PARTNER/STAGE/
// PARTNER_STAGE/OPERATOR_ROUTE (в частности freeze/unfreeze от Billing
// Reconciliation, HLD §15.5) применялся в registry и аудировался, но
// никогда не доходил ни до одного потребителя execution.control. Теперь
// сервер публикует запись сразу после каждого успешного ApplyOverride/
// ClearOverride, для любого scope.
type Server struct {
	grpcv1.UnimplementedExecutionControlServiceServer

	registry  *registry.Registry
	audit     AuditPersister
	publisher *kafkaio.Publisher
}

func New(reg *registry.Registry, audit AuditPersister, publisher *kafkaio.Publisher) *Server {
	return &Server{registry: reg, audit: audit, publisher: publisher}
}

// validAdmissionRate — admission_rate публикуется в ExecutionControlRecord и
// напрямую управляет реальными решениями admission ниже по потоку;
// CODE_REVIEW.md finding: раньше принимался любой float64 без границ
// (отрицательный, >1.0, NaN/Inf). control.execution_control_audit тоже
// имеет CHECK (admission_rate BETWEEN 0 AND 1), но здесь проверяется явно,
// чтобы вызывающий получил понятное InvalidArgument, а не сырую ошибку
// вставки в БД.
func validAdmissionRate(rate float64) bool {
	return !math.IsNaN(rate) && !math.IsInf(rate, 0) && rate >= 0 && rate <= 1
}

func scopeFromProto(s commonv1.ExecutionControlScope) hysteresis.Scope {
	switch s {
	case commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE:
		return hysteresis.ScopeStage
	case commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER:
		return hysteresis.ScopePartner
	case commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE:
		return hysteresis.ScopePartnerStage
	case commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_OPERATOR_ROUTE:
		return hysteresis.ScopeOperatorRoute
	default:
		return hysteresis.ScopeGlobal
	}
}

func stateFromProto(s commonv1.ExecutionControlState) hysteresis.State {
	switch s {
	case commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_DEGRADED:
		return hysteresis.StateDegraded
	case commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED:
		return hysteresis.StatePaused
	default:
		return hysteresis.StateActive
	}
}

// ApplyOverride — apply_manual_override + persist_override_audit
// (service_internal_methods.md §3.1) + publish_control_record.
//
// Порядок операций — audit ПЕРЕД мутацией registry, мутация ПЕРЕД publish:
// CODE_REVIEW.md HIGH finding — раньше registry мутировался первым, и если
// PersistOverrideAudit падал, RPC возвращал ошибку, но in-memory состояние
// уже изменилось (вызывающий не мог отличить "ничего не произошло" от
// "применилось, но не аудировалось"). PersistOverrideAudit не зависит от
// версии, которую выдаёт registry.ApplyOverride, так что audit можно
// выполнить первым без потери информации — если он падает, registry вообще
// не трогается, откатывать нечего.
func (s *Server) ApplyOverride(ctx context.Context, req *grpcv1.ApplyOverrideRequest) (*grpcv1.ApplyOverrideResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, fmt.Errorf("requested_by обязателен для аудита override")
	}
	if !validAdmissionRate(req.GetAdmissionRate()) {
		return nil, fmt.Errorf("admission_rate должен быть конечным числом в диапазоне [0,1], получили %v", req.GetAdmissionRate())
	}

	scope := scopeFromProto(req.GetScope())
	state := stateFromProto(req.GetState())
	key := registry.ScopeKey{Scope: scope, ScopeID: req.GetScopeId()}

	var expiresAt *time.Time
	if req.GetExpiresAt() != nil {
		t := req.GetExpiresAt().AsTime()
		expiresAt = &t
	}

	if s.audit != nil {
		if _, _, err := s.audit.PersistOverrideAudit(ctx, store.AuditEntry{
			Scope:         store.ScopeName(scope),
			ScopeID:       req.GetScopeId(),
			State:         store.StateName(state),
			AdmissionRate: req.GetAdmissionRate(),
			Reason:        req.GetReason(),
			RequestedBy:   req.GetRequestedBy(),
			ExpiresAt:     expiresAt,
		}); err != nil {
			return nil, fmt.Errorf("persist_override_audit: %w", err)
		}
	}

	version, appliedAt := s.registry.ApplyOverride(key, registry.Override{
		State:         state,
		AdmissionRate: req.GetAdmissionRate(),
		Reason:        req.GetReason(),
		RequestedBy:   req.GetRequestedBy(),
		ExpiresAt:     expiresAt,
	})

	if err := s.publish(ctx, key, registry.Evaluation{
		State: state, AdmissionRate: req.GetAdmissionRate(), DispatchRate: req.GetAdmissionRate(),
		Reason: req.GetReason(), Version: version, ExpiresAt: expiresAt,
	}, appliedAt); err != nil {
		return nil, fmt.Errorf("publish_control_record: %w", err)
	}

	return &grpcv1.ApplyOverrideResponse{
		Version:   version,
		AppliedAt: timestamppb.New(appliedAt),
	}, nil
}

// ClearOverride снимает override и аудирует это как отдельную запись
// (ACTIVE, rate=1.0, reason="override_cleared") — тот же аудит-след, что и
// применение, симметрично. См. ApplyOverride для обоснования порядка
// audit->registry->publish.
func (s *Server) ClearOverride(ctx context.Context, req *grpcv1.ClearOverrideRequest) (*grpcv1.ApplyOverrideResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, fmt.Errorf("requested_by обязателен для аудита override")
	}

	scope := scopeFromProto(req.GetScope())
	key := registry.ScopeKey{Scope: scope, ScopeID: req.GetScopeId()}
	appliedAt := time.Now().UTC()

	if s.audit != nil {
		if _, _, err := s.audit.PersistOverrideAudit(ctx, store.AuditEntry{
			Scope:       store.ScopeName(scope),
			ScopeID:     req.GetScopeId(),
			State:       store.StateName(hysteresis.StateActive),
			Reason:      "override_cleared",
			RequestedBy: req.GetRequestedBy(),
		}); err != nil {
			return nil, fmt.Errorf("persist_override_audit: %w", err)
		}
	}

	version := s.registry.ClearOverride(key)

	if err := s.publish(ctx, key, registry.Evaluation{
		State: hysteresis.StateActive, AdmissionRate: 1.0, DispatchRate: 1.0,
		Reason: "override_cleared", Version: version,
	}, appliedAt); err != nil {
		return nil, fmt.Errorf("publish_control_record: %w", err)
	}

	return &grpcv1.ApplyOverrideResponse{
		Version:   version,
		AppliedAt: timestamppb.New(appliedAt),
	}, nil
}

// publish — сборка + отправка ExecutionControlRecord. publisher может быть
// nil в тестах, которые не проверяют Kafka-путь отдельно.
func (s *Server) publish(ctx context.Context, key registry.ScopeKey, eval registry.Evaluation, now time.Time) error {
	if s.publisher == nil {
		return nil
	}
	rec := kafkaio.BuildControlRecord(key, eval, now)
	return s.publisher.Publish(ctx, key, rec)
}
