// handleCredentials* — /v1/partners/{partner_id}/applications/{application_id}/credentials/rotate
// и /v1/partners/{partner_id}/credentials (право credentials:issue,
// migrations/V025__iam.sql — уже посеяно, ранее не использовалось ни одним
// маршрутом). Тонкий gRPC-прокси поверх CredentialIssuerService
// (credential-issuer-service/README.md, luminous-hugging-charm.md Ф1).
// issued_by ВСЕГДА берётся из claims.Subject (проверенный JWT sub), никогда
// из тела запроса — тот же принцип, что granted_by/revoked_by в iam.go и
// requested_by в config.go/executioncontrol.go/replay.go: вызывающий не
// может подделать, кто инициировал ротацию секрета.
//
// POST .../credentials/rotate намеренно без тела запроса вообще —
// partner_id/application_id приходят из пути, issued_by — из JWT, больше
// параметров у RotateCredential нет (credentials.proto).
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

type rotateCredentialResponse struct {
	CredentialRef   string `json:"credential_ref"`
	SecretVersion   int32  `json:"secret_version"`
	PlaintextSecret string `json:"plaintext_secret"`
	IssuedAt        string `json:"issued_at,omitempty"`
}

// handleCredentialsRotate — POST /v1/partners/{partner_id}/applications/{application_id}/credentials/rotate.
func handleCredentialsRotate(client grpcv1.CredentialIssuerServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		partnerID := chi.URLParam(r, "partner_id")
		applicationID := chi.URLParam(r, "application_id")

		resp, err := client.RotateCredential(r.Context(), &grpcv1.RotateCredentialRequest{
			PartnerId:     partnerID,
			ApplicationId: applicationID,
			IssuedBy:      claims.Subject,
		})
		if err != nil {
			writeCredentialIssuerGRPCError(w, "credentials_rotate", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rotateCredentialResponse{
			CredentialRef:   resp.GetCredentialRef(),
			SecretVersion:   resp.GetSecretVersion(),
			PlaintextSecret: resp.GetPlaintextSecret(),
			IssuedAt:        formatTimestamp(resp.GetIssuedAt()),
		})
	}
}

type issuedSecretResponse struct {
	ID            int64  `json:"id"`
	PartnerID     string `json:"partner_id"`
	ApplicationID string `json:"application_id"`
	CredentialRef string `json:"credential_ref"`
	SecretVersion int32  `json:"secret_version"`
	Status        string `json:"status"`
	IssuedAt      string `json:"issued_at,omitempty"`
	IssuedBy      string `json:"issued_by"`
}

func toIssuedSecretResponse(s *grpcv1.IssuedSecretSummary) issuedSecretResponse {
	return issuedSecretResponse{
		ID:            s.GetId(),
		PartnerID:     s.GetPartnerId(),
		ApplicationID: s.GetApplicationId(),
		CredentialRef: s.GetCredentialRef(),
		SecretVersion: s.GetSecretVersion(),
		Status:        s.GetStatus(),
		IssuedAt:      formatTimestamp(s.GetIssuedAt()),
		IssuedBy:      s.GetIssuedBy(),
	}
}

// handleCredentialsList — GET /v1/partners/{partner_id}/credentials.
func handleCredentialsList(client grpcv1.CredentialIssuerServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		partnerID := chi.URLParam(r, "partner_id")

		resp, err := client.ListIssuedSecrets(r.Context(), &grpcv1.ListIssuedSecretsRequest{
			PartnerId: partnerID,
		})
		if err != nil {
			writeCredentialIssuerGRPCError(w, "credentials_list", err)
			return
		}

		secrets := make([]issuedSecretResponse, 0, len(resp.GetSecrets()))
		for _, s := range resp.GetSecrets() {
			secrets = append(secrets, toIssuedSecretResponse(s))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Secrets []issuedSecretResponse `json:"secrets"`
		}{Secrets: secrets})
	}
}

// writeCredentialIssuerGRPCError — маппинг кодов ошибок CredentialIssuerService
// (server.go RotateCredential — codes.NotFound/FailedPrecondition/Unavailable/
// Internal, credential-issuer-service/README.md "порядок операций") в HTTP-статусы.
//
// codes.FailedPrecondition -> 422 (Unprocessable Entity), не 400: это НЕ
// ошибка формы запроса вызывающего (partner_id/application_id в пути
// синтаксически валидны) — это реальная, хоть и редкая, проблема данных на
// сервере: credential_ref, уже сохранённый в PARTNER config
// (config.config_versions), не парсится ожидаемой схемой
// (vault://partners/<partner_id>/<application_id>/<field>, см.
// ParseCredentialRef в credential-issuer-service/README.md). 400 подразумевал
// бы, что вызывающий прислал что-то не так; 422 точнее говорит "запрос
// синтаксически корректен, но обработать его невозможно из-за состояния
// данных" — тот же RFC 4918 смысл, в котором 422 обычно используется.
func writeCredentialIssuerGRPCError(w http.ResponseWriter, context string, err error) {
	switch status.Code(err) {
	case codes.NotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case codes.FailedPrecondition:
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case codes.Unavailable:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		internalError(w, http.StatusInternalServerError, context+": gRPC-вызов Credential Issuer Service не удался", err)
	}
}
