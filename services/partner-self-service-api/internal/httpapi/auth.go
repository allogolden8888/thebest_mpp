// handleLogin — POST /v1/self-service/auth/login (BACKOFFICE_ROADMAP.md
// Production Readiness Review P0#5). Mirrors backoffice-api's handleLogin
// (internal/httpapi/auth.go there) exactly in shape: the only route in this
// service mounted WITHOUT auth.Validator.Middleware (router.go) — by
// definition the caller doesn't have a JWT yet at this step, this route is
// what issues one. IamService.VerifyPartnerPortalCredentials does the
// bcrypt comparison entirely inside iam-service — the password hash never
// crosses the gRPC boundary, this handler only ever sees ok/external_id/
// partner_id.
package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
)

type loginRequestBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

const timeFormat = "2006-01-02T15:04:05.000Z07:00"

func handleLogin(client grpcv1.IamServiceClient, issuer *auth.TokenIssuer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body loginRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.Username == "" || body.Password == "" {
			http.Error(w, "требуются username и password", http.StatusBadRequest)
			return
		}

		verifyResp, err := client.VerifyPartnerPortalCredentials(r.Context(), &grpcv1.VerifyPartnerPortalCredentialsRequest{
			Username: body.Username,
			Password: body.Password,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("login: gRPC-вызов IAM Service не удался: %v", err), http.StatusBadGateway)
			return
		}
		if !verifyResp.GetOk() {
			http.Error(w, "неверный логин или пароль", http.StatusUnauthorized)
			return
		}

		// Дополнительный вызов ResolvePartnerPortalAccess — исключительно
		// чтобы вложить в выпускаемый JWT снимок ТЕКУЩИХ ролей для
		// немедленного UX в partner-portal-ui сразу после логина (см.
		// internal/auth/issuer.go package doc). Реальная авторизация на
		// каждый последующий запрос всё равно идёт через ResolveLiveAccess
		// (resolve.go), не через то, что здесь записано в токен — если этот
		// вызов вернёт active=false в этой узкой гонке (аккаунт
		// деактивирован между двумя gRPC-вызовами), просто выпускаем токен
		// с пустыми ролями, а не проваливаем логин на этом крае: следующий
		// же защищённый запрос всё равно корректно получит 401 от
		// ResolveLiveAccess.
		var roles []string
		if accessResp, accessErr := client.ResolvePartnerPortalAccess(r.Context(), &grpcv1.ResolvePartnerPortalAccessRequest{
			ExternalId: verifyResp.GetExternalId(),
		}); accessErr == nil && accessResp.GetActive() {
			roles = accessResp.GetRoles()
		}

		token, expiresAt, err := issuer.Issue(verifyResp.GetExternalId(), verifyResp.GetPartnerId(), roles)
		if err != nil {
			http.Error(w, fmt.Sprintf("login: не удалось выпустить токен: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: token, ExpiresAt: expiresAt.Format(timeFormat)})
	}
}
