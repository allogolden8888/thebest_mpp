// RequirePermission — тот же паттерн, что services/backoffice-api/internal/auth/permission.go
// (luminous-hugging-charm.md Фаза 0/Фаза 6): гранулярная проверка права
// через IamService.CheckPermission, а не разбор JWT-claim'ов на месте.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"time"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

const checkPermissionTimeout = 3 * time.Second

// RequirePermission — fail-closed: недоступность IAM Service (таймаут,
// сеть, TLS) означает 503, не пропуск запроса дальше. compliance-api пишет
// в consent/blacklist-конфиг — та же категория чувствительности, что и у
// backoffice-api, тот же выбор в пользу безопасности при недоступности
// авторизации (см. permission.go в backoffice-api за полным обоснованием,
// не дублируется здесь).
func RequirePermission(permission string, client grpcv1.IamServiceClient) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFromContext(r.Context())
			if !ok {
				http.Error(w, "нет claims в контексте (RequirePermission смонтирован до Middleware?)", http.StatusInternalServerError)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), checkPermissionTimeout)
			defer cancel()

			resp, err := client.CheckPermission(ctx, &grpcv1.CheckPermissionRequest{
				ExternalId: claims.Subject,
				Permission: permission,
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("проверка прав недоступна (IAM Service): %v", err), http.StatusServiceUnavailable)
				return
			}
			if !resp.GetAllowed() {
				http.Error(w, fmt.Sprintf("требуется право %q", permission), http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
