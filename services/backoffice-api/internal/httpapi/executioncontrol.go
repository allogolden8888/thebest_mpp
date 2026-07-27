package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"google.golang.org/protobuf/types/known/timestamppb"

	"mpp/backoffice-api/internal/auth"
)

func parseScope(s string) (commonv1.ExecutionControlScope, bool) {
	v, ok := commonv1.ExecutionControlScope_value[s]
	if !ok {
		return 0, false
	}
	return commonv1.ExecutionControlScope(v), true
}

func parseState(s string) (commonv1.ExecutionControlState, bool) {
	v, ok := commonv1.ExecutionControlState_value[s]
	if !ok {
		return 0, false
	}
	return commonv1.ExecutionControlState(v), true
}

type applyOverrideRequestBody struct {
	Scope         string  `json:"scope"`
	ScopeID       string  `json:"scope_id"`
	State         string  `json:"state"`
	AdmissionRate float64 `json:"admission_rate"`
	Reason        string  `json:"reason"`
	// ExpiresAt — RFC3339, пусто = override действует до явного ClearOverride
	// (platform-contracts/grpc/internal_control.proto ApplyOverrideRequest).
	ExpiresAt string `json:"expires_at,omitempty"`
}

// handle_execution_control_override (ApplyOverride часть) —
// service_internal_methods.md §7.3: HTTP-запрос от UI -> gRPC-проксирование
// в Execution Control Service.
func handleExecutionControlApplyOverride(client grpcv1.ExecutionControlServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body applyOverrideRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		scope, ok := parseScope(body.Scope)
		if !ok {
			http.Error(w, "неизвестный scope: "+body.Scope, http.StatusBadRequest)
			return
		}
		state, ok := parseState(body.State)
		if !ok {
			http.Error(w, "неизвестный state: "+body.State, http.StatusBadRequest)
			return
		}

		req := &grpcv1.ApplyOverrideRequest{
			Scope:         scope,
			ScopeId:       body.ScopeID,
			State:         state,
			AdmissionRate: body.AdmissionRate,
			Reason:        body.Reason,
			RequestedBy:   claims.Subject,
		}
		if body.ExpiresAt != "" {
			t, err := time.Parse(time.RFC3339, body.ExpiresAt)
			if err != nil {
				http.Error(w, "неверный формат expires_at (ожидался RFC3339)", http.StatusBadRequest)
				return
			}
			req.ExpiresAt = timestamppb.New(t)
		}

		resp, err := client.ApplyOverride(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeOverrideResponse(w, resp)
	}
}

type clearOverrideRequestBody struct {
	Scope   string `json:"scope"`
	ScopeID string `json:"scope_id"`
}

// handle_execution_control_override (ClearOverride часть).
func handleExecutionControlClearOverride(client grpcv1.ExecutionControlServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body clearOverrideRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		scope, ok := parseScope(body.Scope)
		if !ok {
			http.Error(w, "неизвестный scope: "+body.Scope, http.StatusBadRequest)
			return
		}

		resp, err := client.ClearOverride(r.Context(), &grpcv1.ClearOverrideRequest{
			Scope:       scope,
			ScopeId:     body.ScopeID,
			RequestedBy: claims.Subject,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeOverrideResponse(w, resp)
	}
}

func writeOverrideResponse(w http.ResponseWriter, resp *grpcv1.ApplyOverrideResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Version   int64  `json:"version"`
		AppliedAt string `json:"applied_at,omitempty"`
	}{
		Version:   resp.GetVersion(),
		AppliedAt: formatTimestamp(resp.GetAppliedAt()),
	})
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format("2006-01-02T15:04:05.000Z07:00")
}
