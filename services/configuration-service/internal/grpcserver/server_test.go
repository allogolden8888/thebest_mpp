package grpcserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/configuration-service/internal/store"
	"mpp/configuration-service/internal/validate"
)

type fakeStore struct {
	versions map[string]store.ConfigVersion
	created  []store.ConfigVersion
	// payloads — keyed by "entityID:version", populated by
	// CreateImmutableVersionAndOutbox, read by GetVersionByNumber
	// (luminous-hugging-charm.md Ф10, DiffVersions).
	payloads map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{versions: map[string]store.ConfigVersion{}, payloads: map[string][]byte{}}
}

func (f *fakeStore) CreateImmutableVersionAndOutbox(ctx context.Context, entityType validate.EntityType, entityID string, payloadJSON []byte, createdBy string) (store.ConfigVersion, error) {
	v := store.ConfigVersion{EntityType: entityType, EntityID: entityID, Version: int32(len(f.created) + 1), Status: "active", CreatedBy: createdBy, Payload: payloadJSON}
	f.created = append(f.created, v)
	f.versions[entityID] = v
	f.payloads[fmt.Sprintf("%s:%d", entityID, v.Version)] = payloadJSON
	return v, nil
}

func (f *fakeStore) GetVersionByNumber(ctx context.Context, entityType validate.EntityType, entityID string, version int32) ([]byte, error) {
	payload, ok := f.payloads[fmt.Sprintf("%s:%d", entityID, version)]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return payload, nil
}

func (f *fakeStore) GetActiveVersion(ctx context.Context, entityType validate.EntityType, entityID string) (store.ConfigVersion, error) {
	v, ok := f.versions[entityID]
	if !ok {
		return store.ConfigVersion{}, pgx.ErrNoRows
	}
	return v, nil
}

func (f *fakeStore) ListVersions(ctx context.Context, entityType validate.EntityType, entityID string, pageSize int32, pageToken string) ([]store.ConfigVersion, string, error) {
	return f.created, "", nil
}

func (f *fakeStore) ArchiveVersion(ctx context.Context, entityType validate.EntityType, entityID string, version int32) (store.ConfigVersion, error) {
	v := f.versions[entityID]
	v.Status = "archived"
	f.versions[entityID] = v
	return v, nil
}

type alwaysValid struct{}

func (alwaysValid) Validate(entityType validate.EntityType, payloadJSON []byte) error { return nil }

type alwaysInvalid struct{}

func (alwaysInvalid) Validate(entityType validate.EntityType, payloadJSON []byte) error {
	return &validate.ValidationError{EntityType: entityType, Errors: []string{"forced failure"}}
}

func TestCreateVersionRejectsMissingRequestedBy(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
	})
	if err == nil {
		t.Fatalf("ожидали ошибку при отсутствующем requested_by")
	}
}

func TestCreateVersionRejectsUnknownEntityType(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		EntityId:    "acme",
		RequestedBy: "ops",
	})
	if err == nil {
		t.Fatalf("ожидали ошибку для CONFIG_ENTITY_TYPE_UNSPECIFIED")
	}
}

func TestCreateVersionPropagatesValidationError(t *testing.T) {
	srv := New(newFakeStore(), alwaysInvalid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		PayloadJson: []byte(`{}`),
		RequestedBy: "ops",
	})
	if err == nil {
		t.Fatalf("ожидали, что ошибка валидации пробросится наружу")
	}
}

func TestCreateVersionSucceedsAndReturnsVersion(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	resp, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		PayloadJson: []byte(`{"partner_id":"acme"}`),
		RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}
	if resp.GetVersion() != 1 {
		t.Fatalf("ожидали версию 1, получили %d", resp.GetVersion())
	}
	if resp.GetEntityType() != commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER {
		t.Fatalf("entity_type не пробросился обратно верно: %v", resp.GetEntityType())
	}
}

func TestArchiveVersionRejectsMissingRequestedBy(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.ArchiveVersion(context.Background(), &grpcv1.ArchiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
		Version:    1,
	})
	if err == nil {
		t.Fatalf("ожидали ошибку при отсутствующем requested_by")
	}
}

func TestGetActiveVersionAfterCreate(t *testing.T) {
	fs := newFakeStore()
	srv := New(fs, alwaysValid{})
	ctx := context.Background()

	_, err := srv.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR, EntityId: "beeline_uz",
		PayloadJson: []byte(`{}`), RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}

	resp, err := srv.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR, EntityId: "beeline_uz",
	})
	if err != nil {
		t.Fatalf("GetActiveVersion failed: %v", err)
	}
	if resp.GetEntityId() != "beeline_uz" {
		t.Fatalf("неверный entity_id: %s", resp.GetEntityId())
	}
}

// TestGetActiveVersionReturnsPayload — найдено при реализации
// partner-self-service-api (Фаза 3 плана): до появления PayloadJson в
// ConfigVersionResponse ни один клиент не мог прочитать текущее содержимое
// документа, только метаданные. Проверяем, что payload реально доезжает
// туда и обратно через CreateVersion -> GetActiveVersion.
func TestGetActiveVersionReturnsPayload(t *testing.T) {
	fs := newFakeStore()
	srv := New(fs, alwaysValid{})
	ctx := context.Background()

	wantPayload := []byte(`{"partner_id":"acme","applications":[]}`)
	_, err := srv.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		PayloadJson: wantPayload, RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("CreateVersion failed: %v", err)
	}

	resp, err := srv.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
	})
	if err != nil {
		t.Fatalf("GetActiveVersion failed: %v", err)
	}
	if string(resp.GetPayloadJson()) != string(wantPayload) {
		t.Fatalf("payload_json не пробросился: получили %q, ожидали %q", resp.GetPayloadJson(), wantPayload)
	}
}

func TestCreateVersionMissingRequestedByReturnsInvalidArgument(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}

func TestCreateVersionUnknownEntityTypeReturnsInvalidArgument(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		EntityId:    "acme",
		RequestedBy: "ops",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}

func TestCreateVersionValidationFailureReturnsInvalidArgument(t *testing.T) {
	srv := New(newFakeStore(), alwaysInvalid{})
	_, err := srv.CreateVersion(context.Background(), &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		PayloadJson: []byte(`{}`),
		RequestedBy: "ops",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}

func TestGetActiveVersionNotFoundReturnsNotFound(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.GetActiveVersion(context.Background(), &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "does-not-exist",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ожидали codes.NotFound, получили %v", status.Code(err))
	}
}

func TestGetActiveVersionUnknownEntityTypeReturnsInvalidArgument(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.GetActiveVersion(context.Background(), &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		EntityId:   "acme",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}

// TestValidateVersionValidPayloadReturnsValidTrue — luminous-hugging-charm.md
// Ф10: ValidateVersion — обычный успешный RPC-ответ, не ошибка, даже когда
// payload невалиден (см. package doc метода) — здесь happy path.
func TestValidateVersionValidPayloadReturnsValidTrue(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	resp, err := srv.ValidateVersion(context.Background(), &grpcv1.ValidateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		PayloadJson: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("неожиданная gRPC-ошибка: %v", err)
	}
	if !resp.GetValid() {
		t.Fatalf("ожидали valid=true, получили %+v", resp)
	}
	if len(resp.GetErrors()) != 0 {
		t.Fatalf("valid=true не должен нести errors, получили %v", resp.GetErrors())
	}
}

// TestValidateVersionInvalidPayloadReturnsValidFalseNotGrpcError — invalid
// payload НЕ становится gRPC-уровня ошибкой здесь (в отличие от
// CreateVersion) — это ожидаемый исход "покажи мне, что не так", RPC
// сам успешен.
func TestValidateVersionInvalidPayloadReturnsValidFalseNotGrpcError(t *testing.T) {
	srv := New(newFakeStore(), alwaysInvalid{})
	resp, err := srv.ValidateVersion(context.Background(), &grpcv1.ValidateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		PayloadJson: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("ValidateVersion не должен возвращать gRPC-ошибку на невалидный payload, получили %v", err)
	}
	if resp.GetValid() {
		t.Fatalf("ожидали valid=false")
	}
	if len(resp.GetErrors()) != 1 || resp.GetErrors()[0] != "forced failure" {
		t.Fatalf("ожидали errors=[forced failure], получили %v", resp.GetErrors())
	}
}

func TestValidateVersionUnknownEntityTypeReturnsInvalidArgument(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.ValidateVersion(context.Background(), &grpcv1.ValidateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED,
		PayloadJson: []byte(`{}`),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}

// TestDiffVersionsReturnsBothPayloadsAsIs — luminous-hugging-charm.md Ф10:
// два реальных payload двух версий, без вычисления diff здесь (см. package
// doc DiffVersions в internal_control.proto).
func TestDiffVersionsReturnsBothPayloadsAsIs(t *testing.T) {
	fs := newFakeStore()
	srv := New(fs, alwaysValid{})
	ctx := context.Background()

	if _, err := srv.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		PayloadJson: []byte(`{"version":1}`), RequestedBy: "ops",
	}); err != nil {
		t.Fatalf("CreateVersion (v1): %v", err)
	}
	if _, err := srv.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		PayloadJson: []byte(`{"version":2}`), RequestedBy: "ops",
	}); err != nil {
		t.Fatalf("CreateVersion (v2): %v", err)
	}

	resp, err := srv.DiffVersions(ctx, &grpcv1.DiffVersionsRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		FromVersion: 1, ToVersion: 2,
	})
	if err != nil {
		t.Fatalf("DiffVersions: %v", err)
	}
	if string(resp.GetFromPayloadJson()) != `{"version":1}` {
		t.Fatalf("неверный from_payload_json: %s", resp.GetFromPayloadJson())
	}
	if string(resp.GetToPayloadJson()) != `{"version":2}` {
		t.Fatalf("неверный to_payload_json: %s", resp.GetToPayloadJson())
	}
	if resp.GetFromVersion() != 1 || resp.GetToVersion() != 2 {
		t.Fatalf("неверные номера версий в ответе: from=%d to=%d", resp.GetFromVersion(), resp.GetToVersion())
	}
}

func TestDiffVersionsUnknownVersionReturnsNotFound(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.DiffVersions(context.Background(), &grpcv1.DiffVersionsRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		FromVersion: 1, ToVersion: 2,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ожидали codes.NotFound, получили %v", status.Code(err))
	}
}

func TestDiffVersionsRejectsNonPositiveVersionNumbers(t *testing.T) {
	srv := New(newFakeStore(), alwaysValid{})
	_, err := srv.DiffVersions(context.Background(), &grpcv1.DiffVersionsRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER, EntityId: "acme",
		FromVersion: 0, ToVersion: 2,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ожидали codes.InvalidArgument, получили %v", status.Code(err))
	}
}
