package httpapi

import (
	"encoding/json"
	"net/http"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

type meResponse struct {
	ExternalID  string   `json:"external_id"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
}

// handleMe — GET /v1/me: "какой у меня самого доступ" (без gate прав, любой
// валидный токен realm'а — luminous-hugging-charm.md Фаза 0). Активные роли
// вызывающего берутся через ListStaffAssignments(external_id=sub), затем для
// каждой роли — её права через ListRoles, объединение без дублей.
func handleMe(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		assignmentsResp, err := client.ListStaffAssignments(r.Context(), &grpcv1.ListStaffAssignmentsRequest{ExternalId: claims.Subject})
		if err != nil {
			internalError(w, http.StatusBadGateway, "me: ListStaffAssignments в IAM Service не удался", err)
			return
		}
		roleNames := make(map[string]struct{}, len(assignmentsResp.GetAssignments()))
		for _, a := range assignmentsResp.GetAssignments() {
			roleNames[a.GetRole()] = struct{}{}
		}

		rolesResp, err := client.ListRoles(r.Context(), &grpcv1.ListRolesRequest{})
		if err != nil {
			internalError(w, http.StatusBadGateway, "me: ListRoles в IAM Service не удался", err)
			return
		}

		permissionSet := make(map[string]struct{})
		for _, role := range rolesResp.GetRoles() {
			if _, ok := roleNames[role.GetName()]; !ok {
				continue
			}
			for _, p := range role.GetPermissions() {
				permissionSet[p] = struct{}{}
			}
		}

		roles := make([]string, 0, len(roleNames))
		for name := range roleNames {
			roles = append(roles, name)
		}
		permissions := make([]string, 0, len(permissionSet))
		for p := range permissionSet {
			permissions = append(permissions, p)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(meResponse{
			ExternalID:  claims.Subject,
			Roles:       roles,
			Permissions: permissions,
		})
	}
}
