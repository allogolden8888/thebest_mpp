package httpapi

import (
	"encoding/json"
	"net/http"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

type replayRequestBody struct {
	StageExecutionID string `json:"stage_execution_id"`
}

// handle_replay_request — service_internal_methods.md §7.3: HTTP-запрос,
// stage_execution_id -> gRPC-проксирование в Replay Service.
func handleReplayRequest(client grpcv1.ReplayServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body replayRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if body.StageExecutionID == "" {
			http.Error(w, "требуется stage_execution_id", http.StatusBadRequest)
			return
		}

		resp, err := client.RequestReplay(r.Context(), &grpcv1.RequestReplayRequest{
			StageExecutionId: body.StageExecutionID,
			RequestedBy:      claims.Subject,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Accepted        bool   `json:"accepted"`
			RejectionReason string `json:"rejection_reason,omitempty"`
		}{Accepted: resp.GetAccepted(), RejectionReason: resp.GetRejectionReason()})
	}
}
