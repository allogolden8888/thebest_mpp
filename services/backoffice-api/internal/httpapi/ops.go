// handleOpsSnapshot — GET /v1/ops/snapshot (право ops:read,
// luminous-hugging-charm.md Ф8). В отличие от каждой другой интеграции в
// этом файле — НЕ gRPC. ops-visibility-service не поднимает gRPC-сервер
// вообще (services/ops-visibility-service/README.md — GET /snapshot
// отдаётся с того же HEALTH_PORT=9090, что /healthz/readyz/metrics, у
// сервиса нет отдельного бизнес-порта). Здесь — простой HTTP-прокси:
// GET к ops-visibility-service, тело передаётся вызывающему как есть
// (тот же JSON, что ops-visibility-service уже отдаёт напрямую — не
// перепаковываем).
package httpapi

import (
	"context"
	"io"
	"net/http"
	"time"
)

// opsSnapshotTimeout — верхняя граница на сам прокси-запрос: если
// ops-visibility-service завис, backoffice-api не должен держать
// вызывающего неограниченно.
const opsSnapshotTimeout = 5 * time.Second

func handleOpsSnapshot(client *http.Client, opsVisibilityURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), opsSnapshotTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, opsVisibilityURL+"/snapshot", nil)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "ops_snapshot: сборка запроса не удалась", err)
			return
		}

		resp, err := client.Do(req)
		if err != nil {
			internalError(w, http.StatusBadGateway, "ops_snapshot: запрос к Ops Visibility Service не удался", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			internalError(w, http.StatusBadGateway, "ops_snapshot: чтение ответа Ops Visibility Service не удалось", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	}
}
