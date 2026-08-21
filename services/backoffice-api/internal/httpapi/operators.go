// handleOperatorRoutesList — GET /v1/operators/routes
// (BACKOFFICE_ROADMAP.md §2 "Операторы (SMPP routes)"): read-only снимок
// живых operator_route:* ключей из Runtime Redis (store/redis.go).
package httpapi

import (
	"encoding/json"
	"net/http"
	"sort"

	"mpp/backoffice-api/internal/store"
)

type operatorRouteResponse struct {
	OperatorID       string `json:"operator_id"`
	RouteID          string `json:"route_id"`
	Protocol         string `json:"protocol"`
	OwningInstanceID string `json:"owning_instance_id"`
	Endpoint         string `json:"endpoint"`
	RouteEpoch       int64  `json:"route_epoch"`
	Heartbeat        string `json:"heartbeat"`
	TTLSeconds       int64  `json:"ttl_seconds"`
}

func toOperatorRouteResponse(r store.OperatorRoute) operatorRouteResponse {
	return operatorRouteResponse{
		OperatorID:       r.OperatorID,
		RouteID:          r.RouteID,
		Protocol:         r.Protocol,
		OwningInstanceID: r.OwningInstanceID,
		Endpoint:         r.Endpoint,
		RouteEpoch:       r.RouteEpoch,
		Heartbeat:        r.Heartbeat.Format(timeFormat),
		TTLSeconds:       r.TTLSeconds,
	}
}

func handleOperatorRoutesList(rdb *store.Redis) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		routes, err := rdb.ScanOperatorRoutes(r.Context())
		if err != nil {
			internalError(w, http.StatusInternalServerError, "operator_routes: scan Runtime Redis не удался", err)
			return
		}

		out := make([]operatorRouteResponse, 0, len(routes))
		for _, route := range routes {
			out = append(out, toOperatorRouteResponse(route))
		}
		// SCAN не гарантирует порядок — стабильная сортировка для
		// предсказуемого ответа (не влияет на корректность, только на UX
		// таблицы в backoffice-ui).
		sort.Slice(out, func(i, j int) bool {
			if out[i].OperatorID != out[j].OperatorID {
				return out[i].OperatorID < out[j].OperatorID
			}
			return out[i].RouteID < out[j].RouteID
		})

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Routes []operatorRouteResponse `json:"routes"`
		}{out})
	}
}
