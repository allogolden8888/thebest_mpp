// Package httpapi — GET /v1/self-service/credentials, POST
// /v1/self-service/applications/{application_id}/credentials/rotate (Фаза 3
// плана, /Users/Alisher/.claude/plans/luminous-hugging-charm.md). Оба
// хендлера бьют в d.CredentialClient (CredentialIssuerServiceClient) —
// единственный писатель/читатель по Vault-путям credential_ref
// (см. doc-комментарий CredentialIssuerServiceClient в
// credentials_grpc.pb.go). partner_id — всегда из JWT-claim вызывающего
// (claims.PartnerID), никогда из URL/query/body: партнёр не должен получить
// возможность ротировать чужой credential, подставив чужой application_id
// в path — тот же инвариант, что и в partnerconfig.go/applications.go.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
)

const credentialRPCTimeout = 5 * time.Second

func mountCredentials(r chi.Router, d Deps) {
	r.Get("/credentials", handleListCredentials(d.CredentialClient))
	r.With(auth.RequireAdmin).Post("/applications/{application_id}/credentials/rotate", handleRotateCredential(d.CredentialClient))
}

// handleListCredentials — GET /credentials. Только чтение, роль не
// проверяется (mirрор open-read паттерна compliance-api/consent.go).
// IssuedSecretSummary структурно не несёт plaintext-значения секрета — это
// поле есть только на RotateCredentialResponse (show-once, см. proto) — так
// что здесь нечего скрывать вручную, отдаём весь ответ как есть.
func handleListCredentials(client grpcv1.CredentialIssuerServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), credentialRPCTimeout)
		defer cancel()

		resp, err := client.ListIssuedSecrets(ctx, &grpcv1.ListIssuedSecretsRequest{
			PartnerId: claims.PartnerID,
		})
		if err != nil {
			writeGRPCError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp.GetSecrets())
	}
}

// handleRotateCredential — POST /applications/{application_id}/credentials/rotate.
// Гейт RequireAdmin — деструктивная операция, старый секрет теряет силу
// сразу же (см. router.go/jwt.go). Один RPC на "первый выпуск" и "ротацию"
// (см. doc-комментарий RotateCredential) — на HTTP-стороне тоже один
// эндпоинт, отдельного issue не заводим. Ответ включает PlaintextSecret —
// единственный раз, когда значение секрета вообще где-либо показывается
// (show-once, обеспечено на стороне сервера/proto); наша обязанность —
// не логировать и не сохранять его, только сразу отдать в теле ответа.
func handleRotateCredential(client grpcv1.CredentialIssuerServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		applicationID := chi.URLParam(r, "application_id")
		if applicationID == "" {
			http.Error(w, "application_id обязателен", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), credentialRPCTimeout)
		defer cancel()

		resp, err := client.RotateCredential(ctx, &grpcv1.RotateCredentialRequest{
			PartnerId:     claims.PartnerID,
			ApplicationId: applicationID,
			IssuedBy:      claims.Subject,
		})
		if err != nil {
			writeGRPCError(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// writeGRPCError — маппинг ошибки нижестоящего RPC (CredentialIssuerService)
// в HTTP-статус: NotFound (application_id не заведён в PARTNER-конфиге, или
// у него ещё нет credential_ref) -> 404, InvalidArgument -> 400, всё
// остальное -> 502 (Bad Gateway — сбой нижестоящей зависимости, не самого
// self-service-api). Клиентский эквивалент серверного storeErrToStatus-
// маппинга, используемого в других сервисах этой кодовой базы.
func writeGRPCError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	switch code {
	case codes.NotFound:
		http.Error(w, fmt.Sprintf("не найдено: %v", err), http.StatusNotFound)
	case codes.InvalidArgument:
		http.Error(w, fmt.Sprintf("невалидный запрос: %v", err), http.StatusBadRequest)
	default:
		http.Error(w, fmt.Sprintf("сбой нижестоящего сервиса: %v", err), http.StatusBadGateway)
	}
}
