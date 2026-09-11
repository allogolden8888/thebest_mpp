// handleJWKS — GET /v1/.well-known/jwks.json (BACKOFFICE_ROADMAP.md P0
// "секреты" — JWKS/kid rotation, internal/auth/keys.go). Второй и последний
// маршрут этого сервиса БЕЗ d.Validator.Middleware — тот же класс
// исключения, что POST /v1/auth/login (auth.go package doc): любой клиент,
// желающий проверить подпись токена этого issuer'а (в первую очередь сам
// backoffice-api при рестарте с новым набором ключей, но по духу JWKS —
// публичный, не секретный документ, RFC 7517), не может уже иметь валидный
// Bearer-токен на этот момент.
package httpapi

import (
	"encoding/json"
	"net/http"

	"mpp/backoffice-api/internal/auth"
)

func handleJWKS(validator *auth.Validator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(validator.JWKS())
	}
}
