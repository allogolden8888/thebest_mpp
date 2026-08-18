// handleIncident* — /v1/incidents/* (право incident:manage,
// luminous-hugging-charm.md Ф7). Тонкий gRPC-прокси поверх IncidentService
// (platform-contracts/grpc/incident.proto) для экрана backoffice-ui
// "Incidents". opened_by/author/resolved_by ВСЕГДА берутся из
// claims.Subject, никогда из тела запроса — тот же принцип, что
// granted_by/revoked_by в iam.go и issued_by в credentials.go: вызывающий
// не может подделать, кто открыл инцидент/оставил заметку/закрыл разбор.
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

type incidentResponse struct {
	ID              int64  `json:"id"`
	Title           string `json:"title"`
	Severity        string `json:"severity"`
	Status          string `json:"status"`
	OpenedBy        string `json:"opened_by"`
	OpenedAt        string `json:"opened_at,omitempty"`
	ResolvedBy      string `json:"resolved_by,omitempty"`
	ResolvedAt      string `json:"resolved_at,omitempty"`
	PostmortemNotes string `json:"postmortem_notes,omitempty"`
}

func toIncidentResponse(inc *grpcv1.Incident) incidentResponse {
	return incidentResponse{
		ID:              inc.GetId(),
		Title:           inc.GetTitle(),
		Severity:        inc.GetSeverity(),
		Status:          inc.GetStatus(),
		OpenedBy:        inc.GetOpenedBy(),
		OpenedAt:        formatTimestamp(inc.GetOpenedAt()),
		ResolvedBy:      inc.GetResolvedBy(),
		ResolvedAt:      formatTimestamp(inc.GetResolvedAt()),
		PostmortemNotes: inc.GetPostmortemNotes(),
	}
}

type openIncidentRequestBody struct {
	Title    string `json:"title"`
	Severity string `json:"severity"`
}

// handleOpenIncident — POST /v1/incidents.
func handleOpenIncident(client grpcv1.IncidentServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		var body openIncidentRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := client.OpenIncident(r.Context(), &grpcv1.OpenIncidentRequest{
			Title: body.Title, Severity: body.Severity, OpenedBy: claims.Subject,
		})
		if err != nil {
			writeIncidentGRPCError(w, "incidents_open", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(toIncidentResponse(resp))
	}
}

// handleListIncidents — GET /v1/incidents?status=.
func handleListIncidents(client grpcv1.IncidentServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := client.ListIncidents(r.Context(), &grpcv1.ListIncidentsRequest{
			Status: r.URL.Query().Get("status"),
		})
		if err != nil {
			writeIncidentGRPCError(w, "incidents_list", err)
			return
		}
		incidents := make([]incidentResponse, 0, len(resp.GetIncidents()))
		for _, inc := range resp.GetIncidents() {
			incidents = append(incidents, toIncidentResponse(inc))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Incidents []incidentResponse `json:"incidents"`
		}{Incidents: incidents})
	}
}

type timelineEntryResponse struct {
	ID            int64   `json:"id"`
	Scope         string  `json:"scope"`
	ScopeID       string  `json:"scope_id"`
	State         string  `json:"state"`
	AdmissionRate float64 `json:"admission_rate"`
	Reason        string  `json:"reason"`
	RequestedBy   string  `json:"requested_by"`
	CreatedAt     string  `json:"created_at,omitempty"`
	ExpiresAt     string  `json:"expires_at,omitempty"`
}

type incidentNoteResponse struct {
	ID         int64  `json:"id"`
	IncidentID int64  `json:"incident_id"`
	Author     string `json:"author"`
	Note       string `json:"note"`
	CreatedAt  string `json:"created_at,omitempty"`
}

// handleGetIncident — GET /v1/incidents/{incident_id}: карточка + таймлайн
// + заметки в одном ответе (IncidentDetail — см. package doc
// incident.proto GetIncident, композиция трёх независимых источников
// внутри incident-service, не здесь).
func handleGetIncident(client grpcv1.IncidentServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		incidentID, ok := parsePositiveInt64(chi.URLParam(r, "incident_id"))
		if !ok {
			http.Error(w, "неверный incident_id", http.StatusBadRequest)
			return
		}

		resp, err := client.GetIncident(r.Context(), &grpcv1.GetIncidentRequest{IncidentId: incidentID})
		if err != nil {
			writeIncidentGRPCError(w, "incidents_get", err)
			return
		}

		timeline := make([]timelineEntryResponse, 0, len(resp.GetTimeline()))
		for _, e := range resp.GetTimeline() {
			timeline = append(timeline, timelineEntryResponse{
				ID: e.GetId(), Scope: e.GetScope(), ScopeID: e.GetScopeId(), State: e.GetState(),
				AdmissionRate: e.GetAdmissionRate(), Reason: e.GetReason(), RequestedBy: e.GetRequestedBy(),
				CreatedAt: formatTimestamp(e.GetCreatedAt()), ExpiresAt: formatTimestamp(e.GetExpiresAt()),
			})
		}
		notes := make([]incidentNoteResponse, 0, len(resp.GetNotes()))
		for _, n := range resp.GetNotes() {
			notes = append(notes, incidentNoteResponse{
				ID: n.GetId(), IncidentID: n.GetIncidentId(), Author: n.GetAuthor(),
				Note: n.GetNote(), CreatedAt: formatTimestamp(n.GetCreatedAt()),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Incident incidentResponse        `json:"incident"`
			Timeline []timelineEntryResponse `json:"timeline"`
			Notes    []incidentNoteResponse  `json:"notes"`
		}{Incident: toIncidentResponse(resp.GetIncident()), Timeline: timeline, Notes: notes})
	}
}

type addNoteRequestBody struct {
	Note string `json:"note"`
}

// handleAddIncidentNote — POST /v1/incidents/{incident_id}/notes.
func handleAddIncidentNote(client grpcv1.IncidentServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		incidentID, ok := parsePositiveInt64(chi.URLParam(r, "incident_id"))
		if !ok {
			http.Error(w, "неверный incident_id", http.StatusBadRequest)
			return
		}
		var body addNoteRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := client.AddNote(r.Context(), &grpcv1.AddNoteRequest{
			IncidentId: incidentID, Author: claims.Subject, Note: body.Note,
		})
		if err != nil {
			writeIncidentGRPCError(w, "incidents_add_note", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(incidentNoteResponse{
			ID: resp.GetId(), IncidentID: resp.GetIncidentId(), Author: resp.GetAuthor(),
			Note: resp.GetNote(), CreatedAt: formatTimestamp(resp.GetCreatedAt()),
		})
	}
}

type resolveIncidentRequestBody struct {
	PostmortemNotes string `json:"postmortem_notes"`
}

// handleResolveIncident — POST /v1/incidents/{incident_id}/resolve.
func handleResolveIncident(client grpcv1.IncidentServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		incidentID, ok := parsePositiveInt64(chi.URLParam(r, "incident_id"))
		if !ok {
			http.Error(w, "неверный incident_id", http.StatusBadRequest)
			return
		}
		var body resolveIncidentRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := client.ResolveIncident(r.Context(), &grpcv1.ResolveIncidentRequest{
			IncidentId: incidentID, ResolvedBy: claims.Subject, PostmortemNotes: body.PostmortemNotes,
		})
		if err != nil {
			writeIncidentGRPCError(w, "incidents_resolve", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(toIncidentResponse(resp))
	}
}

func parsePositiveInt64(s string) (int64, bool) {
	n, ok := parseNonNegativeInt(s)
	if !ok || n == 0 {
		return 0, false
	}
	return int64(n), true
}

// writeIncidentGRPCError — маппинг кодов ошибок IncidentService
// (codes.NotFound/InvalidArgument/FailedPrecondition — server.go
// ResolveIncident на уже закрытый инцидент) в HTTP-статусы.
func writeIncidentGRPCError(w http.ResponseWriter, context string, err error) {
	switch status.Code(err) {
	case codes.NotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case codes.InvalidArgument:
		http.Error(w, err.Error(), http.StatusBadRequest)
	case codes.FailedPrecondition:
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		internalError(w, http.StatusInternalServerError, context+": gRPC-вызов Incident Service не удался", err)
	}
}
