// Package grpcserver реализует mpp.grpc.v1.IamService
// (platform-contracts/grpc/iam.proto) — вызывается Backoffice API (и
// позже Partner Self-Service API) на каждый мутирующий запрос
// (CheckPermission), плюс административные RPC для backoffice-ui
// "Access Control".
package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/iam-service/internal/store"
)

// Store — минимальный интерфейс, который нужен серверу от store.Postgres
// (позволяет подменять в тестах фейком без реального Postgres).
type Store interface {
	CheckPermission(ctx context.Context, externalID, permission string) (bool, []string, error)
	ListRoles(ctx context.Context) ([]store.Role, error)
	ListStaffAssignments(ctx context.Context, externalID string) ([]store.StaffAssignment, error)
	AssignStaffRole(ctx context.Context, externalID, role, grantedBy string) (store.StaffAssignment, error)
	RevokeStaffRole(ctx context.Context, externalID, role, revokedBy string) (bool, error)
	ListPartnerPortalAssignments(ctx context.Context, externalID string) ([]store.PartnerPortalAssignment, error)
	AssignPartnerPortalRole(ctx context.Context, externalID, role, grantedBy string) (store.PartnerPortalAssignment, error)
	RevokePartnerPortalRole(ctx context.Context, externalID, role, revokedBy string) (bool, error)
	CreateStaffAccount(ctx context.Context, username, password, displayName, createdBy string) (store.StaffAccount, error)
	ListStaffAccounts(ctx context.Context, activeOnly bool) ([]store.StaffAccount, error)
	DeactivateStaffAccount(ctx context.Context, externalID, actor string) (bool, error)
	VerifyStaffCredentials(ctx context.Context, username, password string) (string, bool, error)
	CreatePartnerPortalUser(ctx context.Context, username, password, partnerID, displayName, createdBy string) (store.PartnerPortalUser, error)
	ListPartnerPortalUsers(ctx context.Context, partnerID string) ([]store.PartnerPortalUser, error)
	DeactivatePartnerPortalUser(ctx context.Context, externalID, actor string) (bool, error)
	VerifyPartnerPortalCredentials(ctx context.Context, username, password string) (externalID, partnerID string, ok bool, err error)
	ResolvePartnerPortalAccess(ctx context.Context, externalID string) (active bool, partnerID string, roles []string, err error)
}

type Server struct {
	grpcv1.UnimplementedIamServiceServer

	store Store
}

func New(s Store) *Server {
	return &Server{store: s}
}

func (s *Server) CheckPermission(ctx context.Context, req *grpcv1.CheckPermissionRequest) (*grpcv1.CheckPermissionResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetPermission() == "" {
		return nil, status.Error(codes.InvalidArgument, "permission обязателен")
	}

	allowed, roles, err := s.store.CheckPermission(ctx, req.GetExternalId(), req.GetPermission())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.CheckPermissionResponse{Allowed: allowed, Roles: roles}, nil
}

func (s *Server) ListRoles(ctx context.Context, _ *grpcv1.ListRolesRequest) (*grpcv1.ListRolesResponse, error) {
	roles, err := s.store.ListRoles(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListRolesResponse{Roles: make([]*grpcv1.Role, 0, len(roles))}
	for _, r := range roles {
		resp.Roles = append(resp.Roles, &grpcv1.Role{
			Id:          r.ID,
			Name:        r.Name,
			Description: r.Description,
			Permissions: r.Permissions,
		})
	}
	return resp, nil
}

func (s *Server) ListStaffAssignments(ctx context.Context, req *grpcv1.ListStaffAssignmentsRequest) (*grpcv1.ListStaffAssignmentsResponse, error) {
	assignments, err := s.store.ListStaffAssignments(ctx, req.GetExternalId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListStaffAssignmentsResponse{Assignments: make([]*grpcv1.StaffAssignment, 0, len(assignments))}
	for _, a := range assignments {
		resp.Assignments = append(resp.Assignments, toProtoAssignment(a))
	}
	return resp, nil
}

func (s *Server) AssignStaffRole(ctx context.Context, req *grpcv1.AssignStaffRoleRequest) (*grpcv1.AssignStaffRoleResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "role обязателен")
	}
	if req.GetGrantedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "granted_by обязателен для аудита")
	}

	a, err := s.store.AssignStaffRole(ctx, req.GetExternalId(), req.GetRole(), req.GetGrantedBy())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrRoleNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		case errors.Is(err, store.ErrAlreadyAssigned):
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &grpcv1.AssignStaffRoleResponse{Assignment: toProtoAssignment(a)}, nil
}

func (s *Server) RevokeStaffRole(ctx context.Context, req *grpcv1.RevokeStaffRoleRequest) (*grpcv1.RevokeStaffRoleResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "role обязателен")
	}
	if req.GetRevokedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "revoked_by обязателен для аудита")
	}

	revoked, err := s.store.RevokeStaffRole(ctx, req.GetExternalId(), req.GetRole(), req.GetRevokedBy())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.RevokeStaffRoleResponse{Revoked: revoked}, nil
}

func toProtoAssignment(a store.StaffAssignment) *grpcv1.StaffAssignment {
	return &grpcv1.StaffAssignment{
		Id:         a.ID,
		ExternalId: a.ExternalID,
		Role:       a.Role,
		GrantedBy:  a.GrantedBy,
		GrantedAt:  timestamppb.New(a.GrantedAt),
	}
}

func (s *Server) ListPartnerPortalAssignments(ctx context.Context, req *grpcv1.ListPartnerPortalAssignmentsRequest) (*grpcv1.ListPartnerPortalAssignmentsResponse, error) {
	assignments, err := s.store.ListPartnerPortalAssignments(ctx, req.GetExternalId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListPartnerPortalAssignmentsResponse{Assignments: make([]*grpcv1.PartnerPortalAssignment, 0, len(assignments))}
	for _, a := range assignments {
		resp.Assignments = append(resp.Assignments, toProtoPartnerPortalAssignment(a))
	}
	return resp, nil
}

func (s *Server) AssignPartnerPortalRole(ctx context.Context, req *grpcv1.AssignPartnerPortalRoleRequest) (*grpcv1.AssignPartnerPortalRoleResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "role обязателен")
	}
	if req.GetGrantedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "granted_by обязателен для аудита")
	}

	a, err := s.store.AssignPartnerPortalRole(ctx, req.GetExternalId(), req.GetRole(), req.GetGrantedBy())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidPartnerPortalRole):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, store.ErrPartnerPortalUserNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &grpcv1.AssignPartnerPortalRoleResponse{Assignment: toProtoPartnerPortalAssignment(a)}, nil
}

func (s *Server) RevokePartnerPortalRole(ctx context.Context, req *grpcv1.RevokePartnerPortalRoleRequest) (*grpcv1.RevokePartnerPortalRoleResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "role обязателен")
	}
	if req.GetRevokedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "revoked_by обязателен для аудита")
	}

	revoked, err := s.store.RevokePartnerPortalRole(ctx, req.GetExternalId(), req.GetRole(), req.GetRevokedBy())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.RevokePartnerPortalRoleResponse{Revoked: revoked}, nil
}

func toProtoPartnerPortalAssignment(a store.PartnerPortalAssignment) *grpcv1.PartnerPortalAssignment {
	return &grpcv1.PartnerPortalAssignment{
		Id:         a.ID,
		ExternalId: a.ExternalID,
		Role:       a.Role,
		GrantedBy:  a.GrantedBy,
		GrantedAt:  timestamppb.New(a.GrantedAt),
	}
}

func (s *Server) CreateStaffAccount(ctx context.Context, req *grpcv1.CreateStaffAccountRequest) (*grpcv1.CreateStaffAccountResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "username обязателен")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password обязателен")
	}
	if req.GetDisplayName() == "" {
		return nil, status.Error(codes.InvalidArgument, "display_name обязателен")
	}
	if req.GetCreatedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "created_by обязателен для аудита")
	}

	a, err := s.store.CreateStaffAccount(ctx, req.GetUsername(), req.GetPassword(), req.GetDisplayName(), req.GetCreatedBy())
	if err != nil {
		if errors.Is(err, store.ErrUsernameTaken) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.CreateStaffAccountResponse{Account: toProtoStaffAccount(a)}, nil
}

func (s *Server) ListStaffAccounts(ctx context.Context, req *grpcv1.ListStaffAccountsRequest) (*grpcv1.ListStaffAccountsResponse, error) {
	accounts, err := s.store.ListStaffAccounts(ctx, req.GetActiveOnly())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListStaffAccountsResponse{Accounts: make([]*grpcv1.StaffAccount, 0, len(accounts))}
	for _, a := range accounts {
		resp.Accounts = append(resp.Accounts, toProtoStaffAccount(a))
	}
	return resp, nil
}

func (s *Server) DeactivateStaffAccount(ctx context.Context, req *grpcv1.DeactivateStaffAccountRequest) (*grpcv1.DeactivateStaffAccountResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetActor() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor обязателен для аудита")
	}

	deactivated, err := s.store.DeactivateStaffAccount(ctx, req.GetExternalId(), req.GetActor())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.DeactivateStaffAccountResponse{Deactivated: deactivated}, nil
}

// VerifyStaffCredentials — не codes.NotFound/Unauthenticated на неверные
// креды: ok=false в теле 200-подобного gRPC OK-ответа, тот же класс
// решения, что ValidateVersion в configuration-service (невалидный вход —
// ОЖИДАЕМЫЙ исход этого RPC, не ошибка вызова).
func (s *Server) VerifyStaffCredentials(ctx context.Context, req *grpcv1.VerifyStaffCredentialsRequest) (*grpcv1.VerifyStaffCredentialsResponse, error) {
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return &grpcv1.VerifyStaffCredentialsResponse{Ok: false}, nil
	}

	externalID, ok, err := s.store.VerifyStaffCredentials(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.VerifyStaffCredentialsResponse{Ok: ok, ExternalId: externalID}, nil
}

func toProtoStaffAccount(a store.StaffAccount) *grpcv1.StaffAccount {
	return &grpcv1.StaffAccount{
		ExternalId:  a.ExternalID,
		Username:    a.Username,
		DisplayName: a.DisplayName,
		Active:      a.Active,
		CreatedAt:   timestamppb.New(a.CreatedAt),
	}
}

// CreatePartnerPortalUser — BACKOFFICE_ROADMAP.md Production Readiness
// Review P0#5. Mirrors CreateStaffAccount validation exactly.
func (s *Server) CreatePartnerPortalUser(ctx context.Context, req *grpcv1.CreatePartnerPortalUserRequest) (*grpcv1.CreatePartnerPortalUserResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "username обязателен")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password обязателен")
	}
	if req.GetPartnerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "partner_id обязателен")
	}
	if req.GetDisplayName() == "" {
		return nil, status.Error(codes.InvalidArgument, "display_name обязателен")
	}
	if req.GetCreatedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "created_by обязателен для аудита")
	}

	u, err := s.store.CreatePartnerPortalUser(ctx, req.GetUsername(), req.GetPassword(), req.GetPartnerId(), req.GetDisplayName(), req.GetCreatedBy())
	if err != nil {
		if errors.Is(err, store.ErrPartnerPortalUsernameTaken) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.CreatePartnerPortalUserResponse{User: toProtoPartnerPortalUser(u)}, nil
}

func (s *Server) ListPartnerPortalUsers(ctx context.Context, req *grpcv1.ListPartnerPortalUsersRequest) (*grpcv1.ListPartnerPortalUsersResponse, error) {
	users, err := s.store.ListPartnerPortalUsers(ctx, req.GetPartnerId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &grpcv1.ListPartnerPortalUsersResponse{Users: make([]*grpcv1.PartnerPortalUser, 0, len(users))}
	for _, u := range users {
		resp.Users = append(resp.Users, toProtoPartnerPortalUser(u))
	}
	return resp, nil
}

func (s *Server) DeactivatePartnerPortalUser(ctx context.Context, req *grpcv1.DeactivatePartnerPortalUserRequest) (*grpcv1.DeactivatePartnerPortalUserResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}
	if req.GetActor() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor обязателен для аудита")
	}

	deactivated, err := s.store.DeactivatePartnerPortalUser(ctx, req.GetExternalId(), req.GetActor())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.DeactivatePartnerPortalUserResponse{Deactivated: deactivated}, nil
}

// VerifyPartnerPortalCredentials — не codes.NotFound/Unauthenticated на
// неверные креды, тот же класс решения, что VerifyStaffCredentials.
func (s *Server) VerifyPartnerPortalCredentials(ctx context.Context, req *grpcv1.VerifyPartnerPortalCredentialsRequest) (*grpcv1.VerifyPartnerPortalCredentialsResponse, error) {
	if req.GetUsername() == "" || req.GetPassword() == "" {
		return &grpcv1.VerifyPartnerPortalCredentialsResponse{Ok: false}, nil
	}

	externalID, partnerID, ok, err := s.store.VerifyPartnerPortalCredentials(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.VerifyPartnerPortalCredentialsResponse{Ok: ok, ExternalId: externalID, PartnerId: partnerID}, nil
}

// ResolvePartnerPortalAccess — вызывается partner-self-service-api на
// каждый запрос вместо разбора realm_access.roles из JWT
// (BACKOFFICE_ROADMAP.md Production Readiness Review P0#5), тот же
// fail-closed/no-caching контракт, что CheckPermission.
func (s *Server) ResolvePartnerPortalAccess(ctx context.Context, req *grpcv1.ResolvePartnerPortalAccessRequest) (*grpcv1.ResolvePartnerPortalAccessResponse, error) {
	if req.GetExternalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "external_id обязателен")
	}

	active, partnerID, roles, err := s.store.ResolvePartnerPortalAccess(ctx, req.GetExternalId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &grpcv1.ResolvePartnerPortalAccessResponse{Active: active, PartnerId: partnerID, Roles: roles}, nil
}

func toProtoPartnerPortalUser(u store.PartnerPortalUser) *grpcv1.PartnerPortalUser {
	return &grpcv1.PartnerPortalUser{
		ExternalId:  u.ExternalID,
		Username:    u.Username,
		PartnerId:   u.PartnerID,
		DisplayName: u.DisplayName,
		Active:      u.Active,
		CreatedAt:   timestamppb.New(u.CreatedAt),
	}
}
