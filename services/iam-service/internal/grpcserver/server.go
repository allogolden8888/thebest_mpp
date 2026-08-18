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
