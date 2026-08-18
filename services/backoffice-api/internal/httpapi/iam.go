// handleIam* — /v1/iam/* (permission iam:manage): административный экран
// backoffice-ui "Access Control" (iam-service/README.md), тонкий
// gRPC-прокси поверх IamService.{ListRoles,ListStaffAssignments,
// AssignStaffRole,RevokeStaffRole}. granted_by/revoked_by ВСЕГДА берутся
// из claims.Subject (проверенный JWT sub), никогда из тела запроса — тот же
// принцип, что requested_by в config.go/executioncontrol.go/replay.go:
// вызывающий не может подделать, кто выдал/отозвал доступ.
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

type iamRoleResponse struct {
	ID          int64    `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"`
}

func toIamRoleResponse(r *grpcv1.Role) iamRoleResponse {
	permissions := r.GetPermissions()
	if permissions == nil {
		permissions = []string{}
	}
	return iamRoleResponse{
		ID:          r.GetId(),
		Name:        r.GetName(),
		Description: r.GetDescription(),
		Permissions: permissions,
	}
}

// handleIamListRoles — GET /v1/iam/roles.
func handleIamListRoles(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListRoles(r.Context(), &grpcv1.ListRolesRequest{})
		if err != nil {
			internalError(w, http.StatusBadGateway, "iam_list_roles: gRPC-вызов IAM Service не удался", err)
			return
		}

		roles := make([]iamRoleResponse, 0, len(resp.GetRoles()))
		for _, role := range resp.GetRoles() {
			roles = append(roles, toIamRoleResponse(role))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Roles []iamRoleResponse `json:"roles"`
		}{Roles: roles})
	}
}

type iamStaffAssignmentResponse struct {
	ID         int64  `json:"id"`
	ExternalID string `json:"external_id"`
	Role       string `json:"role"`
	GrantedBy  string `json:"granted_by"`
	GrantedAt  string `json:"granted_at"`
}

func toIamStaffAssignmentResponse(a *grpcv1.StaffAssignment) iamStaffAssignmentResponse {
	return iamStaffAssignmentResponse{
		ID:         a.GetId(),
		ExternalID: a.GetExternalId(),
		Role:       a.GetRole(),
		GrantedBy:  a.GetGrantedBy(),
		GrantedAt:  formatTimestamp(a.GetGrantedAt()),
	}
}

// handleIamListStaffAssignments — GET /v1/iam/staff-assignments?external_id=
// (необязательный фильтр, passthrough в IamService).
func handleIamListStaffAssignments(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListStaffAssignments(r.Context(), &grpcv1.ListStaffAssignmentsRequest{
			ExternalId: r.URL.Query().Get("external_id"),
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "iam_list_staff_assignments: gRPC-вызов IAM Service не удался", err)
			return
		}

		assignments := make([]iamStaffAssignmentResponse, 0, len(resp.GetAssignments()))
		for _, a := range resp.GetAssignments() {
			assignments = append(assignments, toIamStaffAssignmentResponse(a))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Assignments []iamStaffAssignmentResponse `json:"assignments"`
		}{Assignments: assignments})
	}
}

type assignStaffRoleRequestBody struct {
	ExternalID string `json:"external_id"`
	Role       string `json:"role"`
}

// handleIamAssignStaffRole — POST /v1/iam/staff-assignments. granted_by —
// claims.Subject, НЕ из тела (см. package doc).
func handleIamAssignStaffRole(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body assignStaffRoleRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.ExternalID == "" {
			http.Error(w, "требуется external_id", http.StatusBadRequest)
			return
		}
		if body.Role == "" {
			http.Error(w, "требуется role", http.StatusBadRequest)
			return
		}

		resp, err := client.AssignStaffRole(r.Context(), &grpcv1.AssignStaffRoleRequest{
			ExternalId: body.ExternalID,
			Role:       body.Role,
			GrantedBy:  claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_assign_staff_role", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(struct {
			Assignment iamStaffAssignmentResponse `json:"assignment"`
		}{Assignment: toIamStaffAssignmentResponse(resp.GetAssignment())})
	}
}

// handleIamRevokeStaffRole — DELETE /v1/iam/staff-assignments/{external_id}/{role}.
// revoked_by — claims.Subject, НЕ из тела (см. package doc). revoked=false —
// НЕ ошибка (нечего было отзывать), тот же идемпотентный контракт, что
// IamService.RevokeStaffRole (iam-service/README.md).
func handleIamRevokeStaffRole(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		externalID := chi.URLParam(r, "external_id")
		role := chi.URLParam(r, "role")

		resp, err := client.RevokeStaffRole(r.Context(), &grpcv1.RevokeStaffRoleRequest{
			ExternalId: externalID,
			Role:       role,
			RevokedBy:  claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_revoke_staff_role", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Revoked bool `json:"revoked"`
		}{Revoked: resp.GetRevoked()})
	}
}

// writeIamGRPCError — маппинг кодов ошибок IamService (codes.NotFound/
// AlreadyExists — server.go iam-service, AssignStaffRole) в HTTP-статусы
// (service_internal_methods.md-стиль error mapping, тот же, что нигде явно
// не задокументирован для остальных proxy-хендлеров, но здесь важен для
// backoffice-ui — 409 "уже назначено" vs 404 "нет такой роли" не одно и
// то же для UI-обратной связи).
func writeIamGRPCError(w http.ResponseWriter, context string, err error) {
	switch status.Code(err) {
	case codes.NotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case codes.AlreadyExists:
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		internalError(w, http.StatusInternalServerError, context+": gRPC-вызов IAM Service не удался", err)
	}
}
