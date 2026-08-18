// Package grpcserver реализует mpp.grpc.v1.ConfigService (CreateVersion/
// GetActiveVersion/ListVersions/ArchiveVersion — handle_crud_request,
// service_internal_methods.md §3.2). Вызывается Backoffice API.
package grpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/configuration-service/internal/store"
	"mpp/configuration-service/internal/validate"
)

// Store — минимальный интерфейс от store.Store (позволяет фейковать в тестах).
type Store interface {
	CreateImmutableVersionAndOutbox(ctx context.Context, entityType validate.EntityType, entityID string, payloadJSON []byte, createdBy string) (store.ConfigVersion, error)
	GetActiveVersion(ctx context.Context, entityType validate.EntityType, entityID string) (store.ConfigVersion, error)
	ListVersions(ctx context.Context, entityType validate.EntityType, entityID string, pageSize int32, pageToken string) ([]store.ConfigVersion, string, error)
	ArchiveVersion(ctx context.Context, entityType validate.EntityType, entityID string, version int32) (store.ConfigVersion, error)
	GetVersionByNumber(ctx context.Context, entityType validate.EntityType, entityID string, version int32) ([]byte, error)
}

// Validator — минимальный интерфейс от validate.Validator.
type Validator interface {
	Validate(entityType validate.EntityType, payloadJSON []byte) error
}

type Server struct {
	grpcv1.UnimplementedConfigServiceServer

	store     Store
	validator Validator
}

func New(store Store, validator Validator) *Server {
	return &Server{store: store, validator: validator}
}

func entityTypeFromProto(e commonv1.ConfigEntityType) validate.EntityType {
	switch e {
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE:
		return validate.EntityPipeline
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET:
		return validate.EntityPolicyRuleset
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE:
		return validate.EntityPolicyTemplate
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF:
		return validate.EntityBillingTariff
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE:
		return validate.EntityRoutingTable
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE:
		return validate.EntityNumberRange
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER:
		return validate.EntityPartner
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR:
		return validate.EntityOperator
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT:
		return validate.EntitySubscriberConsent
	default:
		return ""
	}
}

func entityTypeToProto(e validate.EntityType) commonv1.ConfigEntityType {
	switch e {
	case validate.EntityPipeline:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE
	case validate.EntityPolicyRuleset:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET
	case validate.EntityPolicyTemplate:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE
	case validate.EntityBillingTariff:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF
	case validate.EntityRoutingTable:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE
	case validate.EntityNumberRange:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE
	case validate.EntityPartner:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER
	case validate.EntityOperator:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR
	case validate.EntitySubscriberConsent:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT
	default:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED
	}
}

func toResponse(v store.ConfigVersion) *grpcv1.ConfigVersionResponse {
	resp := &grpcv1.ConfigVersionResponse{
		EntityType:  entityTypeToProto(v.EntityType),
		EntityId:    v.EntityID,
		Version:     int64(v.Version),
		Status:      v.Status,
		PayloadJson: v.Payload,
	}
	if !v.CreatedAt.IsZero() {
		resp.CreatedAt = timestamppb.New(v.CreatedAt)
	}
	return resp
}

// storeErrToStatus — сопоставляет ошибку store/validate с gRPC status code,
// чтобы вызывающий (Backoffice API) мог различить 400/404/500, не сравнивая
// текст ошибки строками (было CODE_REVIEW.md finding: "every error path is
// a bare fmt.Errorf, surfaced as codes.Unknown").
func storeErrToStatus(err error) error {
	if err == nil {
		return nil
	}
	var valErr *validate.ValidationError
	if errors.As(err, &valErr) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// CreateVersion — validate_config_change + create_immutable_version +
// write_config_and_outbox.
func (s *Server) CreateVersion(ctx context.Context, req *grpcv1.CreateVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "requested_by обязателен")
	}
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}

	if err := s.validator.Validate(entityType, req.GetPayloadJson()); err != nil {
		return nil, storeErrToStatus(err)
	}

	version, err := s.store.CreateImmutableVersionAndOutbox(ctx, entityType, req.GetEntityId(), req.GetPayloadJson(), req.GetRequestedBy())
	if err != nil {
		return nil, storeErrToStatus(err)
	}
	return toResponse(version), nil
}

func (s *Server) GetActiveVersion(ctx context.Context, req *grpcv1.GetActiveVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}
	version, err := s.store.GetActiveVersion(ctx, entityType, req.GetEntityId())
	if err != nil {
		return nil, storeErrToStatus(err)
	}
	return toResponse(version), nil
}

func (s *Server) ListVersions(ctx context.Context, req *grpcv1.ListVersionsRequest) (*grpcv1.ListVersionsResponse, error) {
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}
	versions, nextToken, err := s.store.ListVersions(ctx, entityType, req.GetEntityId(), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, storeErrToStatus(err)
	}
	resp := &grpcv1.ListVersionsResponse{NextPageToken: nextToken}
	for _, v := range versions {
		resp.Versions = append(resp.Versions, toResponse(v))
	}
	return resp, nil
}

// ValidateVersion — luminous-hugging-charm.md Фаза 10: тот же
// s.validator.Validate, что CreateVersion вызывает первым шагом, но здесь
// он единственный шаг — store не трогается вообще. Ошибка валидации не
// маппится в gRPC error (в отличие от CreateVersion, где ValidationError
// -> codes.InvalidArgument) — валидация здесь ОЖИДАЕМЫЙ, частый исход
// ("покажи мне, что не так"), не исключительная ситуация вызывающего;
// ValidateVersionResponse.valid=false с errors — нормальный успешный RPC
// ответ, не gRPC-уровня ошибка.
func (s *Server) ValidateVersion(_ context.Context, req *grpcv1.ValidateVersionRequest) (*grpcv1.ValidateVersionResponse, error) {
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}

	err := s.validator.Validate(entityType, req.GetPayloadJson())
	if err == nil {
		return &grpcv1.ValidateVersionResponse{Valid: true}, nil
	}

	var valErr *validate.ValidationError
	if errors.As(err, &valErr) {
		return &grpcv1.ValidateVersionResponse{Valid: false, Errors: valErr.Errors}, nil
	}
	// Ошибка не от Validator (например паника внутри jsonschema-библиотеки,
	// обёрнутая по пути) — это уже реальная внутренняя проблема сервиса, не
	// "невалидный payload", остаётся gRPC-уровня ошибкой.
	return nil, status.Error(codes.Internal, err.Error())
}

// DiffVersions — luminous-hugging-charm.md Фаза 10: две независимые
// выборки store.GetVersionByNumber, не транзакция (обе — read-only
// immutable-версии, между двумя SELECT не может возникнуть
// рассогласование, которое имело бы значение — config_versions строки
// никогда не изменяются после вставки, только помечаются archived, что не
// трогает сам payload).
func (s *Server) DiffVersions(ctx context.Context, req *grpcv1.DiffVersionsRequest) (*grpcv1.DiffVersionsResponse, error) {
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}
	if req.GetFromVersion() <= 0 || req.GetToVersion() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "from_version и to_version обязаны быть положительными")
	}

	fromPayload, err := s.store.GetVersionByNumber(ctx, entityType, req.GetEntityId(), int32(req.GetFromVersion()))
	if err != nil {
		return nil, storeErrToStatus(fmt.Errorf("from_version=%d: %w", req.GetFromVersion(), err))
	}
	toPayload, err := s.store.GetVersionByNumber(ctx, entityType, req.GetEntityId(), int32(req.GetToVersion()))
	if err != nil {
		return nil, storeErrToStatus(fmt.Errorf("to_version=%d: %w", req.GetToVersion(), err))
	}

	return &grpcv1.DiffVersionsResponse{
		FromVersion:     req.GetFromVersion(),
		FromPayloadJson: fromPayload,
		ToVersion:       req.GetToVersion(),
		ToPayloadJson:   toPayload,
	}, nil
}

func (s *Server) ArchiveVersion(ctx context.Context, req *grpcv1.ArchiveVersionRequest) (*grpcv1.ConfigVersionResponse, error) {
	if req.GetRequestedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "requested_by обязателен")
	}
	entityType := entityTypeFromProto(req.GetEntityType())
	if entityType == "" {
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный entity_type: %v", req.GetEntityType())
	}
	version, err := s.store.ArchiveVersion(ctx, entityType, req.GetEntityId(), int32(req.GetVersion()))
	if err != nil {
		return nil, storeErrToStatus(err)
	}
	return toResponse(version), nil
}
