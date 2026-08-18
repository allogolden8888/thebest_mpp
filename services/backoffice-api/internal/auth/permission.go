// RequirePermission — замена auth.RequireRole на гранулярную проверку прав
// через IAM Service (services/iam-service, platform-contracts/grpc/iam.proto),
// luminous-hugging-charm.md Фаза 0. Один бинарный "backoffice-admin" gate
// раньше защищал ВСЕ деструктивные маршруты одинаково (см. package doc
// jwt.go) — теперь каждый маршрут требует конкретное право
// (config:write/execution-control:write/scheduler:force/replay:request/
// iam:manage/audit:read), проверяемое централизованно в IAM Service, а не
// разбором realm_access.roles на месте.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"time"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

// checkPermissionTimeout — верхняя граница времени, которое запрос готов
// потратить на ожидание IAM Service, прежде чем fail-closed сработает по
// таймауту, а не по явной ошибке транспорта. Короткий таймаут — зависший
// IAM Service не должен подвешивать КАЖДЫЙ мутирующий запрос backoffice-api
// на неопределённое время (все они теперь проходят через этот gate).
const checkPermissionTimeout = 3 * time.Second

// RequirePermission — mux-уровневый gate поверх Middleware (полагается на
// claims в контексте, как и RequireRole). На каждый запрос синхронно
// вызывает IamService.CheckPermission(claims.Subject, permission).
//
// Fail-closed по контракту (platform-contracts/grpc/iam.proto
// CheckPermission docstring, тот же CRITICAL класс находки, который вся
// Фаза 0 закрывает): если сам gRPC-вызов к IAM Service завершается ошибкой
// (сервис недоступен, таймаут, TLS/сеть) — ответ 503, НЕ пропуск запроса
// дальше. Backoffice API способен поставить на паузу весь трафик платформы
// (execution-control override) и мутировать production-конфиг; сервис с
// такой властью не может по умолчанию проваливаться в "открыто" только
// потому, что его собственная зависимость (IAM Service) недоступна —
// недоступность авторизации должна означать "запрещено", а не "молчаливо
// разрешено". Это осознанно менее доступно, чем pass-through: обратная
// сторона fail-closed — отказ IAM Service временно блокирует ВСЕ
// деструктивные операции backoffice-api, что является намеренным
// компромиссом в пользу безопасности, а не забытым краевым случаем.
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
