// Package grpcserver реализует mpp.grpc.v1.ExecutionControlService
// (ApplyOverride/ClearOverride) — вызывается Backoffice API (manual
// override) и Billing Reconciliation (freeze/unfreeze, scope=PARTNER_STAGE,
// stage=BILLING, HLD §15.5), platform-contracts/grpc/internal_control.proto.
package grpcserver

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/execution-control-service/internal/hysteresis"
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
type Server struct {
	grpcv1.UnimplementedExecutionControlServiceServer

	registry *registry.Registry
	audit    AuditPersister
}

func New(reg *registry.Registry, audit AuditPersister) *Server {
	return &Server{registry: reg, audit: audit}
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
// (service_internal_methods.md §3.1).
func (s *Server) ApplyOverride(ctx context.Context, req *grpcv1.ApplyOverrideRequest) (*grpcv1.ApplyOverrideResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, fmt.Errorf("requested_by обязателен для аудита override")
	}

	scope := scopeFromProto(req.GetScope())
	state := stateFromProto(req.GetState())
	key := registry.ScopeKey{Scope: scope, ScopeID: req.GetScopeId()}

	var expiresAt *time.Time
	if req.GetExpiresAt() != nil {
		t := req.GetExpiresAt().AsTime()
		expiresAt = &t
	}

	version, appliedAt := s.registry.ApplyOverride(key, registry.Override{
		State:         state,
		AdmissionRate: req.GetAdmissionRate(),
		Reason:        req.GetReason(),
		RequestedBy:   req.GetRequestedBy(),
		ExpiresAt:     expiresAt,
	})

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

	return &grpcv1.ApplyOverrideResponse{
		Version:   version,
		AppliedAt: timestamppb.New(appliedAt),
	}, nil
}

// ClearOverride снимает override и аудирует это как отдельную запись
// (ACTIVE, rate=1.0, reason="override_cleared") — тот же аудит-след, что и
// применение, симметрично.
func (s *Server) ClearOverride(ctx context.Context, req *grpcv1.ClearOverrideRequest) (*grpcv1.ApplyOverrideResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, fmt.Errorf("requested_by обязателен для аудита override")
	}

	scope := scopeFromProto(req.GetScope())
	key := registry.ScopeKey{Scope: scope, ScopeID: req.GetScopeId()}

	version := s.registry.ClearOverride(key)
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

	return &grpcv1.ApplyOverrideResponse{
		Version:   version,
		AppliedAt: timestamppb.New(appliedAt),
	}, nil
}
