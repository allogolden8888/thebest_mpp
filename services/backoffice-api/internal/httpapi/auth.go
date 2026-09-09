// handleLogin — POST /v1/auth/login (BACKOFFICE_DESIGN_SPEC.md Экран 33
// "Admin users"). Единственный публичный (без Validator.Middleware)
// маршрут этого сервиса — заменяет прежний "вставь JWT вручную в textarea"
// (LoginView.vue) реальным username/password входом. IamService.
// VerifyStaffCredentials делает bcrypt-сравнение целиком у себя (хеш
// пароля никогда не покидает его процесс) — этот handler только просит
// external_id/ok и, при успехе, подписывает JWT сам (internal/auth/
// issuer.go).
package httpapi

import (
	"encoding/json"
	"net/http"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

type loginRequestBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

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

		resp, err := client.VerifyStaffCredentials(r.Context(), &grpcv1.VerifyStaffCredentialsRequest{
			Username: body.Username,
			Password: body.Password,
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "login: gRPC-вызов IAM Service не удался", err)
			return
		}
		if !resp.GetOk() {
			http.Error(w, "неверный логин или пароль", http.StatusUnauthorized)
			return
		}

		token, expiresAt, err := issuer.Issue(resp.GetExternalId())
		if err != nil {
			internalError(w, http.StatusInternalServerError, "login: не удалось выпустить токен", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: token, ExpiresAt: expiresAt.Format(timeFormat)})
	}
}
