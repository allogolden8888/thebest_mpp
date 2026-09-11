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

	ppAssignments     []store.PartnerPortalAssignment
	ppAssignErr       error
	ppRevokeResult    bool
	ppRevokeErr       error
	ppLastAssignedExt string
	ppLastAssignedRol string
	ppLastGrantedBy   string

	staffAccounts        []store.StaffAccount
	createAccountErr     error
	deactivateResult     bool
	deactivateErr        error
	verifyExternalID     string
	verifyOK             bool
	verifyErr            error
	lastVerifyUsername   string
	lastVerifyPassword   string
	lastCreatedUsername  string
	lastCreatedPassword  string
	lastCreatedDisplay   string
	lastCreatedCreatedBy string

	partnerPortalUsers     []store.PartnerPortalUser
	createPPUserErr        error
	ppDeactivateResult     bool
	ppDeactivateErr        error
	ppVerifyExternalID     string
	ppVerifyPartnerID      string
	ppVerifyOK             bool
	ppVerifyErr            error
	lastPPVerifyUsername   string
	lastPPVerifyPassword   string
	lastPPCreatedUsername  string
	lastPPCreatedPassword  string
	lastPPCreatedPartnerID string
	lastPPCreatedDisplay   string
	lastPPCreatedCreatedBy string
	resolveAccessActive    bool
	resolveAccessPartnerID string
	resolveAccessRoles     []string
	resolveAccessErr       error
	lastResolveAccessExtID string
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

func (f *fakeStore) ListPartnerPortalAssignments(_ context.Context, _ string) ([]store.PartnerPortalAssignment, error) {
	return f.ppAssignments, nil
}

func (f *fakeStore) AssignPartnerPortalRole(_ context.Context, externalID, role, grantedBy string) (store.PartnerPortalAssignment, error) {
	f.ppLastAssignedExt, f.ppLastAssignedRol, f.ppLastGrantedBy = externalID, role, grantedBy
	if f.ppAssignErr != nil {
		return store.PartnerPortalAssignment{}, f.ppAssignErr
	}
	return store.PartnerPortalAssignment{ID: 1, ExternalID: externalID, Role: role, GrantedBy: grantedBy, GrantedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) RevokePartnerPortalRole(_ context.Context, _, _, _ string) (bool, error) {
	return f.ppRevokeResult, f.ppRevokeErr
}

func (f *fakeStore) CreateStaffAccount(_ context.Context, username, password, displayName, createdBy string) (store.StaffAccount, error) {
	f.lastCreatedUsername, f.lastCreatedPassword, f.lastCreatedDisplay, f.lastCreatedCreatedBy = username, password, displayName, createdBy
	if f.createAccountErr != nil {
		return store.StaffAccount{}, f.createAccountErr
	}
	return store.StaffAccount{ExternalID: username, Username: username, DisplayName: displayName, Active: true, CreatedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) ListStaffAccounts(_ context.Context, activeOnly bool) ([]store.StaffAccount, error) {
	if !activeOnly {
		return f.staffAccounts, nil
	}
	var out []store.StaffAccount
	for _, a := range f.staffAccounts {
		if a.Active {
			out = append(out, a)
		}
	}
	return out, nil
}

func (f *fakeStore) DeactivateStaffAccount(_ context.Context, _, _ string) (bool, error) {
	return f.deactivateResult, f.deactivateErr
}

func (f *fakeStore) VerifyStaffCredentials(_ context.Context, username, password string) (string, bool, error) {
	f.lastVerifyUsername, f.lastVerifyPassword = username, password
	return f.verifyExternalID, f.verifyOK, f.verifyErr
}

func (f *fakeStore) CreatePartnerPortalUser(_ context.Context, username, password, partnerID, displayName, createdBy string) (store.PartnerPortalUser, error) {
	f.lastPPCreatedUsername, f.lastPPCreatedPassword, f.lastPPCreatedPartnerID, f.lastPPCreatedDisplay, f.lastPPCreatedCreatedBy =
		username, password, partnerID, displayName, createdBy
	if f.createPPUserErr != nil {
		return store.PartnerPortalUser{}, f.createPPUserErr
	}
	return store.PartnerPortalUser{ExternalID: username, Username: username, PartnerID: partnerID, DisplayName: displayName, Active: true, CreatedAt: time.Unix(0, 0)}, nil
}

func (f *fakeStore) ListPartnerPortalUsers(_ context.Context, partnerID string) ([]store.PartnerPortalUser, error) {
	if partnerID == "" {
		return f.partnerPortalUsers, nil
	}
	var out []store.PartnerPortalUser
	for _, u := range f.partnerPortalUsers {
		if u.PartnerID == partnerID {
			out = append(out, u)
		}
	}
	return out, nil
}

func (f *fakeStore) DeactivatePartnerPortalUser(_ context.Context, _, _ string) (bool, error) {
	return f.ppDeactivateResult, f.ppDeactivateErr
}

func (f *fakeStore) VerifyPartnerPortalCredentials(_ context.Context, username, password string) (string, string, bool, error) {
	f.lastPPVerifyUsername, f.lastPPVerifyPassword = username, password
	return f.ppVerifyExternalID, f.ppVerifyPartnerID, f.ppVerifyOK, f.ppVerifyErr
}

func (f *fakeStore) ResolvePartnerPortalAccess(_ context.Context, externalID string) (bool, string, []string, error) {
	f.lastResolveAccessExtID = externalID
	return f.resolveAccessActive, f.resolveAccessPartnerID, f.resolveAccessRoles, f.resolveAccessErr
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
		name     string
		storeErr error
		want     codes.Code
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

func TestAssignPartnerPortalRoleValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.AssignPartnerPortalRoleRequest{
		{Role: "partner-admin", GrantedBy: "admin"},
		{ExternalId: "u1", GrantedBy: "admin"},
		{ExternalId: "u1", Role: "partner-admin"},
	}
	for _, req := range cases {
		if _, err := s.AssignPartnerPortalRole(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestAssignPartnerPortalRoleMapsStoreErrorsToGrpcCodes(t *testing.T) {
	cases := []struct {
		name     string
		storeErr error
		want     codes.Code
	}{
		{"invalid role", store.ErrInvalidPartnerPortalRole, codes.InvalidArgument},
		{"unknown partner user", store.ErrPartnerPortalUserNotFound, codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(&fakeStore{ppAssignErr: tc.storeErr})
			_, err := s.AssignPartnerPortalRole(context.Background(), &grpcv1.AssignPartnerPortalRoleRequest{ExternalId: "pu1", Role: "partner-admin", GrantedBy: "admin"})
			if grpcCode(t, err) != tc.want {
				t.Errorf("ожидали код %v, получили %v", tc.want, err)
			}
		})
	}
}

func TestAssignPartnerPortalRolePassesThroughToStore(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.AssignPartnerPortalRole(context.Background(), &grpcv1.AssignPartnerPortalRoleRequest{ExternalId: "pu1", Role: "partner-viewer", GrantedBy: "admin-1"})
	if err != nil {
		t.Fatalf("AssignPartnerPortalRole: %v", err)
	}
	if fs.ppLastAssignedExt != "pu1" || fs.ppLastAssignedRol != "partner-viewer" || fs.ppLastGrantedBy != "admin-1" {
		t.Errorf("store не получил ожидаемые аргументы: ext=%q role=%q grantedBy=%q", fs.ppLastAssignedExt, fs.ppLastAssignedRol, fs.ppLastGrantedBy)
	}
	if resp.GetAssignment().GetExternalId() != "pu1" || resp.GetAssignment().GetRole() != "partner-viewer" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestRevokePartnerPortalRoleValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.RevokePartnerPortalRoleRequest{
		{Role: "partner-admin", RevokedBy: "admin"},
		{ExternalId: "pu1", RevokedBy: "admin"},
		{ExternalId: "pu1", Role: "partner-admin"},
	}
	for _, req := range cases {
		if _, err := s.RevokePartnerPortalRole(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestRevokePartnerPortalRoleReturnsFalseWithoutErrorWhenNothingToRevoke(t *testing.T) {
	s := New(&fakeStore{ppRevokeResult: false})

	resp, err := s.RevokePartnerPortalRole(context.Background(), &grpcv1.RevokePartnerPortalRoleRequest{ExternalId: "pu1", Role: "partner-admin", RevokedBy: "admin"})
	if err != nil {
		t.Fatalf("RevokePartnerPortalRole: %v", err)
	}
	if resp.GetRevoked() {
		t.Errorf("ожидали revoked=false, не ошибку, когда нечего отзывать")
	}
}

func TestCreateStaffAccountValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.CreateStaffAccountRequest{
		{Password: "pw", DisplayName: "d", CreatedBy: "admin"},
		{Username: "u", DisplayName: "d", CreatedBy: "admin"},
		{Username: "u", Password: "pw", CreatedBy: "admin"},
		{Username: "u", Password: "pw", DisplayName: "d"},
	}
	for _, req := range cases {
		if _, err := s.CreateStaffAccount(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestCreateStaffAccountMapsUsernameTakenToAlreadyExists(t *testing.T) {
	s := New(&fakeStore{createAccountErr: store.ErrUsernameTaken})
	_, err := s.CreateStaffAccount(context.Background(), &grpcv1.CreateStaffAccountRequest{
		Username: "u", Password: "pw", DisplayName: "d", CreatedBy: "admin",
	})
	if grpcCode(t, err) != codes.AlreadyExists {
		t.Errorf("ожидали codes.AlreadyExists, получили %v", err)
	}
}

func TestCreateStaffAccountPassesThroughToStoreAndNeverEchoesPassword(t *testing.T) {
	fs := &fakeStore{}
	s := New(fs)

	resp, err := s.CreateStaffAccount(context.Background(), &grpcv1.CreateStaffAccountRequest{
		Username: "u1", Password: "hunter2", DisplayName: "User One", CreatedBy: "admin-1",
	})
	if err != nil {
		t.Fatalf("CreateStaffAccount: %v", err)
	}
	if fs.lastCreatedUsername != "u1" || fs.lastCreatedPassword != "hunter2" || fs.lastCreatedDisplay != "User One" || fs.lastCreatedCreatedBy != "admin-1" {
		t.Errorf("store не получил ожидаемые аргументы: %+v", fs)
	}
	if resp.GetAccount().GetExternalId() != "u1" || resp.GetAccount().GetUsername() != "u1" {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestDeactivateStaffAccountValidatesRequiredFields(t *testing.T) {
	s := New(&fakeStore{})

	cases := []*grpcv1.DeactivateStaffAccountRequest{
		{Actor: "admin"},
		{ExternalId: "u1"},
	}
	for _, req := range cases {
		if _, err := s.DeactivateStaffAccount(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestVerifyStaffCredentialsReturnsOkFalseWithoutErrorOnMismatch(t *testing.T) {
	s := New(&fakeStore{verifyOK: false})

	resp, err := s.VerifyStaffCredentials(context.Background(), &grpcv1.VerifyStaffCredentialsRequest{Username: "u1", Password: "wrong"})
	if err != nil {
		t.Fatalf("VerifyStaffCredentials: %v", err)
	}
	if resp.GetOk() || resp.GetExternalId() != "" {
		t.Errorf("ожидали ok=false external_id='', получили %+v", resp)
	}
}

func TestVerifyStaffCredentialsReturnsExternalIdOnSuccess(t *testing.T) {
	s := New(&fakeStore{verifyOK: true, verifyExternalID: "u1"})

	resp, err := s.VerifyStaffCredentials(context.Background(), &grpcv1.VerifyStaffCredentialsRequest{Username: "u1", Password: "correct"})
	if err != nil {
		t.Fatalf("VerifyStaffCredentials: %v", err)
	}
	if !resp.GetOk() || resp.GetExternalId() != "u1" {
		t.Errorf("ожидали ok=true external_id=u1, получили %+v", resp)
	}
}

func TestVerifyStaffCredentialsMissingFieldsReturnsOkFalseWithoutCallingStore(t *testing.T) {
	fs := &fakeStore{verifyOK: true, verifyExternalID: "should-not-be-returned"}
	s := New(fs)

	resp, err := s.VerifyStaffCredentials(context.Background(), &grpcv1.VerifyStaffCredentialsRequest{Username: "", Password: ""})
	if err != nil {
		t.Fatalf("VerifyStaffCredentials: %v", err)
	}
	if resp.GetOk() {
		t.Errorf("пустые username/password не должны проходить, даже если store сконфигурирован отвечать ok=true")
	}
	if fs.lastVerifyUsername != "" {
		t.Errorf("store не должен вызываться при пустых username/password")
	}
}
