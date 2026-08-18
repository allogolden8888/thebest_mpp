package grpcserver

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/iam-service/internal/store"
)

// fakeStore — реализует Store без реального Postgres, чтобы проверить
// маппинг ошибок/валидацию на уровне gRPC-сервера отдельно от store_test.go
// (тот же реальный round-trip против Postgres).
type fakeStore struct {
	allowed bool
	roles   []string

	roleList        []store.Role
	assignments     []store.StaffAssignment
	assignErr       error
	revokeResult    bool
	revokeErr       error
	lastAssignedExt string
	lastAssignedRol string
	lastGrantedBy   string
}

func (f *fakeStore) CheckPermission(_ context.Context, _, _ string) (bool, []string, error) {
	return f.allowed, f.roles, nil
}

func (f *fakeStore) ListRoles(_ context.Context) ([]store.Role, error) {
	return f.roleList, nil
}

func (f *fakeStore) ListStaffAssignments(_ context.Context, _ string) ([]store.StaffAssignment, error) {
	return f.assignments, nil
}

func (f *fakeStore) AssignStaffRole(_ context.Context, externalID, role, grantedBy string) (store.StaffAssignment, error) {
	f.lastAssignedExt, f.lastAssignedRol, f.lastGrantedBy = externalID, role, grantedBy
	if f.assignErr != nil {
		return store.StaffAssignment{}, f.assignErr
	}
	return store.StaffAssignment{ID: 1, ExternalID: externalID, Role: role, GrantedBy: grantedBy, GrantedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) RevokeStaffRole(_ context.Context, _, _, _ string) (bool, error) {
	return f.revokeResult, f.revokeErr
}

func grpcCode(t *testing.T, err error) codes.Code {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("ожидали gRPC status error, получили %v", err)
	}
	return st.Code()
}

func TestCheckPermissionRequiresExternalIdAndPermission(t *testing.T) {
	s := New(&fakeStore{})

	if _, err := s.CheckPermission(context.Background(), &grpcv1.CheckPermissionRequest{Permission: "ops:read"}); grpcCode(t, err) != codes.InvalidArgument {
		t.Errorf("пустой external_id должен давать InvalidArgument")
	}
	if _, err := s.CheckPermission(context.Background(), &grpcv1.CheckPermissionRequest{ExternalId: "u1"}); grpcCode(t, err) != codes.InvalidArgument {
		t.Errorf("пустой permission должен давать InvalidArgument")
	}
}

func TestCheckPermissionReturnsAllowedAndRoles(t *testing.T) {
	s := New(&fakeStore{allowed: true, roles: []string{"ops-viewer"}})

	resp, err := s.CheckPermission(context.Background(), &grpcv1.CheckPermissionRequest{ExternalId: "u1", Permission: "ops:read"})
	if err != nil {
		t.Fatalf("CheckPermission: %v", err)
	}
	if !resp.GetAllowed() || len(resp.GetRoles()) != 1 || resp.GetRoles()[0] != "ops-viewer" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestAssignStaffRoleValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.AssignStaffRoleRequest{
		{Role: "ops-viewer", GrantedBy: "admin"},
		{ExternalId: "u1", GrantedBy: "admin"},
		{ExternalId: "u1", Role: "ops-viewer"},
	}
	for _, req := range cases {
		if _, err := s.AssignStaffRole(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestAssignStaffRoleMapsStoreErrorsToGrpcCodes(t *testing.T) {
	cases := []struct {
		name    string
		storeErr error
		want    codes.Code
	}{
		{"unknown role", store.ErrRoleNotFound, codes.NotFound},
		{"already assigned", store.ErrAlreadyAssigned, codes.AlreadyExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(&fakeStore{assignErr: tc.storeErr})
			_, err := s.AssignStaffRole(context.Background(), &grpcv1.AssignStaffRoleRequest{ExternalId: "u1", Role: "ops-viewer", GrantedBy: "admin"})
			if grpcCode(t, err) != tc.want {
				t.Errorf("ожидали код %v, получили %v", tc.want, err)
			}
		})
	}
}

func TestAssignStaffRolePassesThroughToStore(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.AssignStaffRole(context.Background(), &grpcv1.AssignStaffRoleRequest{ExternalId: "u1", Role: "ops-viewer", GrantedBy: "admin-1"})
	if err != nil {
		t.Fatalf("AssignStaffRole: %v", err)
	}
	if fs.lastAssignedExt != "u1" || fs.lastAssignedRol != "ops-viewer" || fs.lastGrantedBy != "admin-1" {
		t.Errorf("store не получил ожидаемые аргументы: ext=%q role=%q grantedBy=%q", fs.lastAssignedExt, fs.lastAssignedRol, fs.lastGrantedBy)
	}
	if resp.GetAssignment().GetExternalId() != "u1" || resp.GetAssignment().GetRole() != "ops-viewer" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestRevokeStaffRoleValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.RevokeStaffRoleRequest{
		{Role: "ops-viewer", RevokedBy: "admin"},
		{ExternalId: "u1", RevokedBy: "admin"},
		{ExternalId: "u1", Role: "ops-viewer"},
	}
	for _, req := range cases {
		if _, err := s.RevokeStaffRole(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestRevokeStaffRoleReturnsFalseWithoutErrorWhenNothingToRevoke(t *testing.T) {
	s := New(&fakeStore{revokeResult: false})

	resp, err := s.RevokeStaffRole(context.Background(), &grpcv1.RevokeStaffRoleRequest{ExternalId: "u1", Role: "ops-viewer", RevokedBy: "admin"})
	if err != nil {
		t.Fatalf("RevokeStaffRole: %v", err)
	}
	if resp.GetRevoked() {
		t.Errorf("ожидали revoked=false, не ошибку, когда нечего отзывать")
	}
}

func TestListRolesTranslatesStoreRolesToProto(t *testing.T) {
	s := New(&fakeStore{roleList: []store.Role{
		{ID: 1, Name: "ops-viewer", Description: "d", Permissions: []string{"ops:read"}},
	}})

	resp, err := s.ListRoles(context.Background(), &grpcv1.ListRolesRequest{})
	if err != nil {
		t.Fatalf("ListRoles: %v", err)
	}
	if len(resp.GetRoles()) != 1 || resp.GetRoles()[0].GetName() != "ops-viewer" || len(resp.GetRoles()[0].GetPermissions()) != 1 {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}
