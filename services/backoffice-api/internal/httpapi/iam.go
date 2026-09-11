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
	// codes.InvalidArgument — AssignPartnerPortalRole (role не входит в
	// CHECK-набор partner-admin/partner-viewer), единственный из четырёх
	// handleIam* мутирующих RPC, где store-уровень (не только HTTP-body
	// декодирование выше) может вернуть эту ошибку.
	case codes.InvalidArgument:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		internalError(w, http.StatusInternalServerError, context+": gRPC-вызов IAM Service не удался", err)
	}
}

type iamPartnerPortalAssignmentResponse struct {
	ID         int64  `json:"id"`
	ExternalID string `json:"external_id"`
	Role       string `json:"role"`
	GrantedBy  string `json:"granted_by"`
	GrantedAt  string `json:"granted_at"`
}

func toIamPartnerPortalAssignmentResponse(a *grpcv1.PartnerPortalAssignment) iamPartnerPortalAssignmentResponse {
	return iamPartnerPortalAssignmentResponse{
		ID:         a.GetId(),
		ExternalID: a.GetExternalId(),
		Role:       a.GetRole(),
		GrantedBy:  a.GetGrantedBy(),
		GrantedAt:  formatTimestamp(a.GetGrantedAt()),
	}
}

// handleIamListPartnerPortalAssignments — GET
// /v1/iam/partner-portal-assignments?external_id= (BACKOFFICE_DESIGN_SPEC.md
// Экран 35 "Partner Users"), тот же необязательный-фильтр passthrough, что
// handleIamListStaffAssignments.
func handleIamListPartnerPortalAssignments(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListPartnerPortalAssignments(r.Context(), &grpcv1.ListPartnerPortalAssignmentsRequest{
			ExternalId: r.URL.Query().Get("external_id"),
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "iam_list_partner_portal_assignments: gRPC-вызов IAM Service не удался", err)
			return
		}

		assignments := make([]iamPartnerPortalAssignmentResponse, 0, len(resp.GetAssignments()))
		for _, a := range resp.GetAssignments() {
			assignments = append(assignments, toIamPartnerPortalAssignmentResponse(a))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Assignments []iamPartnerPortalAssignmentResponse `json:"assignments"`
		}{Assignments: assignments})
	}
}

type assignPartnerPortalRoleRequestBody struct {
	ExternalID string `json:"external_id"`
	Role       string `json:"role"`
}

// handleIamAssignPartnerPortalRole — POST /v1/iam/partner-portal-assignments.
// granted_by — claims.Subject, НЕ из тела (см. package doc).
func handleIamAssignPartnerPortalRole(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body assignPartnerPortalRoleRequestBody
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

		resp, err := client.AssignPartnerPortalRole(r.Context(), &grpcv1.AssignPartnerPortalRoleRequest{
			ExternalId: body.ExternalID,
			Role:       body.Role,
			GrantedBy:  claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_assign_partner_portal_role", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(struct {
			Assignment iamPartnerPortalAssignmentResponse `json:"assignment"`
		}{Assignment: toIamPartnerPortalAssignmentResponse(resp.GetAssignment())})
	}
}

// handleIamRevokePartnerPortalRole — DELETE
// /v1/iam/partner-portal-assignments/{external_id}/{role}. revoked_by —
// claims.Subject, НЕ из тела (см. package doc). revoked=false — НЕ ошибка,
// тот же идемпотентный контракт, что handleIamRevokeStaffRole.
func handleIamRevokePartnerPortalRole(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		externalID := chi.URLParam(r, "external_id")
		role := chi.URLParam(r, "role")

		resp, err := client.RevokePartnerPortalRole(r.Context(), &grpcv1.RevokePartnerPortalRoleRequest{
			ExternalId: externalID,
			Role:       role,
			RevokedBy:  claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_revoke_partner_portal_role", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Revoked bool `json:"revoked"`
		}{Revoked: resp.GetRevoked()})
	}
}

type iamStaffAccountResponse struct {
	ExternalID  string `json:"external_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Active      bool   `json:"active"`
	CreatedAt   string `json:"created_at"`
}

func toIamStaffAccountResponse(a *grpcv1.StaffAccount) iamStaffAccountResponse {
	return iamStaffAccountResponse{
		ExternalID:  a.GetExternalId(),
		Username:    a.GetUsername(),
		DisplayName: a.GetDisplayName(),
		Active:      a.GetActive(),
		CreatedAt:   formatTimestamp(a.GetCreatedAt()),
	}
}

// handleIamListStaffAccounts — GET /v1/iam/staff-accounts?active_only=true.
// Пароль/хеш никогда не покидают IamService — см. StaffAccount package doc
// (iam.proto).
func handleIamListStaffAccounts(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		activeOnly := r.URL.Query().Get("active_only") == "true"
		resp, err := client.ListStaffAccounts(r.Context(), &grpcv1.ListStaffAccountsRequest{ActiveOnly: activeOnly})
		if err != nil {
			internalError(w, http.StatusBadGateway, "iam_list_staff_accounts: gRPC-вызов IAM Service не удался", err)
			return
		}

		accounts := make([]iamStaffAccountResponse, 0, len(resp.GetAccounts()))
		for _, a := range resp.GetAccounts() {
			accounts = append(accounts, toIamStaffAccountResponse(a))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Accounts []iamStaffAccountResponse `json:"accounts"`
		}{Accounts: accounts})
	}
}

type createStaffAccountRequestBody struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

// handleIamCreateStaffAccount — POST /v1/iam/staff-accounts. created_by —
// claims.Subject, НЕ из тела (тот же принцип, что granted_by/revoked_by
// везде в этом файле).
func handleIamCreateStaffAccount(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body createStaffAccountRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.Username == "" || body.Password == "" || body.DisplayName == "" {
			http.Error(w, "требуются username, password и display_name", http.StatusBadRequest)
			return
		}

		resp, err := client.CreateStaffAccount(r.Context(), &grpcv1.CreateStaffAccountRequest{
			Username:    body.Username,
			Password:    body.Password,
			DisplayName: body.DisplayName,
			CreatedBy:   claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_create_staff_account", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(struct {
			Account iamStaffAccountResponse `json:"account"`
		}{Account: toIamStaffAccountResponse(resp.GetAccount())})
	}
}

// handleIamDeactivateStaffAccount — POST /v1/iam/staff-accounts/
// {external_id}/deactivate (тот же POST-action паттерн, что
// /v1/config/versions/archive — не DELETE, потому что деактивация не
// удаляет строку, только переключает флаг). actor — claims.Subject.
func handleIamDeactivateStaffAccount(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		externalID := chi.URLParam(r, "external_id")

		resp, err := client.DeactivateStaffAccount(r.Context(), &grpcv1.DeactivateStaffAccountRequest{
			ExternalId: externalID,
			Actor:      claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_deactivate_staff_account", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Deactivated bool `json:"deactivated"`
		}{Deactivated: resp.GetDeactivated()})
	}
}

type iamPartnerPortalUserResponse struct {
	ExternalID  string `json:"external_id"`
	Username    string `json:"username"`
	PartnerID   string `json:"partner_id"`
	DisplayName string `json:"display_name"`
	Active      bool   `json:"active"`
	CreatedAt   string `json:"created_at"`
}

func toIamPartnerPortalUserResponse(u *grpcv1.PartnerPortalUser) iamPartnerPortalUserResponse {
	return iamPartnerPortalUserResponse{
		ExternalID:  u.GetExternalId(),
		Username:    u.GetUsername(),
		PartnerID:   u.GetPartnerId(),
		DisplayName: u.GetDisplayName(),
		Active:      u.GetActive(),
		CreatedAt:   formatTimestamp(u.GetCreatedAt()),
	}
}

// handleIamListPartnerPortalUsers — GET /v1/iam/partner-portal-users?partner_id=
// (BACKOFFICE_ROADMAP.md Production Readiness Review P0#5 — до этого захода
// PartnerUsersView.vue могло только назначать РОЛИ external_id, который
// предполагался уже существующим ("заводится при первом логине" — это
// предположение полагалось на JIT-provisioning через реальный Keycloak,
// который никогда не был построен, см. platform-contracts/grpc/iam.proto
// package doc). Пустой partner_id — все пользователи (тот же passthrough-
// фильтр, что ListPartnerPortalAssignments).
func handleIamListPartnerPortalUsers(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListPartnerPortalUsers(r.Context(), &grpcv1.ListPartnerPortalUsersRequest{
			PartnerId: r.URL.Query().Get("partner_id"),
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "iam_list_partner_portal_users: gRPC-вызов IAM Service не удался", err)
			return
		}

		users := make([]iamPartnerPortalUserResponse, 0, len(resp.GetUsers()))
		for _, u := range resp.GetUsers() {
			users = append(users, toIamPartnerPortalUserResponse(u))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Users []iamPartnerPortalUserResponse `json:"users"`
		}{Users: users})
	}
}

type createPartnerPortalUserRequestBody struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	PartnerID   string `json:"partner_id"`
	DisplayName string `json:"display_name"`
}

// handleIamCreatePartnerPortalUser — POST /v1/iam/partner-portal-users.
// created_by — claims.Subject, НЕ из тела (тот же принцип, что везде в этом
// файле). Это единственный способ произвести реально логинящегося партнёра
// со стороны бэкофиса — существующий /v1/iam/partner-portal-assignments
// (выше) только назначает РОЛЬ уже существующему external_id, ничего не
// создаёт.
func handleIamCreatePartnerPortalUser(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body createPartnerPortalUserRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.Username == "" || body.Password == "" || body.PartnerID == "" || body.DisplayName == "" {
			http.Error(w, "требуются username, password, partner_id и display_name", http.StatusBadRequest)
			return
		}

		resp, err := client.CreatePartnerPortalUser(r.Context(), &grpcv1.CreatePartnerPortalUserRequest{
			Username:    body.Username,
			Password:    body.Password,
			PartnerId:   body.PartnerID,
			DisplayName: body.DisplayName,
			CreatedBy:   claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_create_partner_portal_user", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(struct {
			User iamPartnerPortalUserResponse `json:"user"`
		}{User: toIamPartnerPortalUserResponse(resp.GetUser())})
	}
}

// handleIamDeactivatePartnerPortalUser — POST /v1/iam/partner-portal-users/
// {external_id}/deactivate, тот же POST-action/идемпотентный паттерн, что
// handleIamDeactivateStaffAccount. actor — claims.Subject.
func handleIamDeactivatePartnerPortalUser(client grpcv1.IamServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		externalID := chi.URLParam(r, "external_id")

		resp, err := client.DeactivatePartnerPortalUser(r.Context(), &grpcv1.DeactivatePartnerPortalUserRequest{
			ExternalId: externalID,
			Actor:      claims.Subject,
		})
		if err != nil {
			writeIamGRPCError(w, "iam_deactivate_partner_portal_user", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Deactivated bool `json:"deactivated"`
		}{Deactivated: resp.GetDeactivated()})
	}
}
