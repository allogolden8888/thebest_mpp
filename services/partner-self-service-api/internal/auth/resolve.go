// Package auth — resolve.go: BACKOFFICE_ROADMAP.md Production Readiness
// Review P0#5, second half of the finding closed by issuer.go. Before this
// file, every handler in this service trusted claims.PartnerID/claims.
// RealmAccess.Roles exactly as they arrived in the JWT (jwt.go) — a role
// granted or revoked through the backoffice-ui "Partner Users" screen
// (iam.partner_portal_role_assignments) had no effect until the caller's
// existing 8h token happened to expire and they logged in again. Same class
// of finding as staff_accounts.active not being checked by CheckPermission
// before Фаза 0 — see platform-contracts/grpc/iam.proto ResolvePartnerPortalAccess
// doc-comment.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"time"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

// resolveAccessTimeout — same value and same reasoning as backoffice-api's
// checkPermissionTimeout (internal/auth/permission.go there): an upper bound
// on how long a request is willing to wait on IAM Service before fail-closed
// kicks in on timeout rather than on an explicit transport error.
const resolveAccessTimeout = 3 * time.Second

// ResolveLiveAccess — mounted right after Validator.Middleware for the
// WHOLE /v1/self-service subtree (router.go), not just the RequireAdmin-
// gated mutating routes. Overwrites claims.PartnerID/claims.RealmAccess.Roles
// in place with the live result of IamService.ResolvePartnerPortalAccess
// (keyed by claims.Subject, the JWT `sub` — the one claim this service still
// trusts as-is, since it is the identity anchor VerifyPartnerPortalCredentials
// itself established at login) before any handler runs. Every handler in
// this service (applications.go/senders/credentials/webhook/templates/chat)
// already reads claims.PartnerID/claims.IsAdmin() via ClaimsFromContext —
// this mutates the SAME *Claims value already stored in the request context
// by Validator.Middleware, so no handler needs to change to pick up the live
// value.
//
// Mounted globally, including read-only routes: claims.PartnerID scopes
// EVERY Configuration Service/CredentialIssuerService/ChatService call this
// service makes, not just the RequireAdmin-gated mutations — a stale
// partner_id would misscope/leak read access just as much as a stale role
// would let a demoted admin keep writing.
//
// Same "no caching, synchronous gRPC call on every request, fail-closed on
// transport error" contract as backoffice-api's RequirePermission
// (documented explicitly for this exact RPC in platform-contracts/grpc/
// iam.proto's ResolvePartnerPortalAccess comment) — deliberately not cached
// per-request-batch or with a TTL: this service can rotate credentials and
// mutate a partner's live applications/senders/webhook config, so it can't
// default to "still allowed" just because IAM Service is briefly
// unreachable, and a deactivated partner-portal user must lose access on the
// very next request, not when their token happens to expire.
func ResolveLiveAccess(client grpcv1.IamServiceClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFromContext(r.Context())
			if !ok {
				http.Error(w, "нет claims в контексте (ResolveLiveAccess смонтирован до Middleware?)", http.StatusInternalServerError)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), resolveAccessTimeout)
			defer cancel()

			resp, err := client.ResolvePartnerPortalAccess(ctx, &grpcv1.ResolvePartnerPortalAccessRequest{
				ExternalId: claims.Subject,
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("проверка доступа недоступна (IAM Service): %v", err), http.StatusServiceUnavailable)
				return
			}
			// active=false покрывает и неизвестный external_id, и
			// деактивированного iam.partner_portal_users — вызывающий не
			// должен различать эти два случая, тот же принцип, что
			// VerifyPartnerPortalCredentials (см. iam.proto doc).
			if !resp.GetActive() {
				http.Error(w, "аккаунт неактивен или не найден", http.StatusUnauthorized)
				return
			}

			claims.PartnerID = resp.GetPartnerId()
			claims.RealmAccess.Roles = resp.GetRoles()

			next.ServeHTTP(w, r)
		})
	}
}
