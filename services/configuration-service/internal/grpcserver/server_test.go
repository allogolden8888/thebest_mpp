package grpcserver

import (
	"context"
	"testing"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/configuration-service/internal/store"
	"mpp/configuration-service/internal/validate"
)

type fakeStore struct {
	versions map[string]store.ConfigVersion
	created  []store.ConfigVersion
}

func newFakeStore() *fakeStore {
	return &fakeStore{versions: map[string]store.ConfigVersion{}}
}

func (f *fakeStore) CreateImmutableVersionAndOutbox(ctx context.Context, entityType validate.EntityType, entityID string, payloadJSON []byte, createdBy string) (store.ConfigVersion, error) {
	v := store.ConfigVersion{EntityType: entityType, EntityID: entityID, Version: int32(len(f.created) + 1), Status: "active", CreatedBy: createdBy}
	f.created = append(f.created, v)
	f.versions[entityID] = v
	return v, nil
}

func (f *fakeStore) GetActiveVersion(ctx context.Context, entityType validate.EntityType, entityID string) (store.ConfigVersion, error) {
	v, ok := f.versions[entityID]
	if !ok {
		return store.ConfigVersion{}, context.DeadlineExceeded
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