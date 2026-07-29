package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
)

// parseEntityType — принимает точное имя enum-значения из
// common/enums.proto ("CONFIG_ENTITY_TYPE_PIPELINE" и т.п.), не выдумывает
// сокращённый alias поверх сгенерированной карты commonv1.ConfigEntityType_value.
func parseEntityType(s string) (commonv1.ConfigEntityType, bool) {
	v, ok := commonv1.ConfigEntityType_value[s]
	if !ok {
		return 0, false
	}
	return commonv1.ConfigEntityType(v), true
}

type createVersionRequestBody struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	PayloadJSON json.RawMessage `json:"payload_json"`
}

// handle_config_crud (CreateVersion часть) — service_internal_methods.md
// §7.3: HTTP-запрос от UI -> gRPC-проксирование в Configuration Service.
func handleConfigCreateVersion(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body createVersionRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		entityType, ok := parseEntityType(body.EntityType)
		if !ok {
			http.Error(w, "неизвестный entity_type: "+body.EntityType, http.StatusBadRequest)
			return
		}

		resp, err := client.CreateVersion(r.Context(), &grpcv1.CreateVersionRequest{
			EntityType:  entityType,
			EntityId:    body.EntityID,
			PayloadJson: body.PayloadJSON,
			RequestedBy: claims.Subject,
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "config_create_version: gRPC-вызов Configuration Service не удался", err)
			return
		}

		writeConfigVersionResponse(w, resp)
	}
}

// handle_config_crud (GetActiveVersion часть).
func handleConfigGetActiveVersion(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		entityType, ok := parseEntityType(q.Get("entity_type"))
		if !ok {
			http.Error(w, "неизвестный entity_type", http.StatusBadRequest)
			return
		}

		resp, err := client.GetActiveVersion(r.Context(), &grpcv1.GetActiveVersionRequest{
			EntityType: entityType,
			EntityId:   q.Get("entity_id"),
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "config_get_active_version: gRPC-вызов Configuration Service не удался", err)
			return
		}
		writeConfigVersionResponse(w, resp)
	}
}

// handle_config_crud (ListVersions часть).
func handleConfigListVersions(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		entityType, ok := parseEntityType(q.Get("entity_type"))
		if !ok {
			http.Error(w, "неизвестный entity_type", http.StatusBadRequest)
			return
		}

		pageSize := 0
		if ps := q.Get("page_size"); ps != "" {
			n, err := strconv.Atoi(ps)
			if err != nil {
				http.Error(w, "неверный page_size", http.StatusBadRequest)
				return
			}
			pageSize = n
		}

		resp, err := client.ListVersions(r.Context(), &grpcv1.ListVersionsRequest{
			EntityType: entityType,
			EntityId:   q.Get("entity_id"),
			PageSize:   int32(pageSize),
			PageToken:  q.Get("page_token"),
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "config_list_versions: gRPC-вызов Configuration Service не удался", err)
			return
		}

		versions := make([]configVersionResponse, 0, len(resp.GetVersions()))
		for _, v := range resp.GetVersions() {
			versions = append(versions, toConfigVersionResponse(v))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Versions      []configVersionResponse `json:"versions"`
			NextPageToken string                  `json:"next_page_token"`
		}{Versions: versions, NextPageToken: resp.GetNextPageToken()})
	}
}

type archiveVersionRequestBody struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Version    int64  `json:"version"`
}

// handle_config_crud (ArchiveVersion часть).
func handleConfigArchiveVersion(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var body archiveVersionRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		entityType, ok := parseEntityType(body.EntityType)
		if !ok {
			http.Error(w, "неизвестный entity_type: "+body.EntityType, http.StatusBadRequest)
			return
		}

		resp, err := client.ArchiveVersion(r.Context(), &grpcv1.ArchiveVersionRequest{
			EntityType:  entityType,
			EntityId:    body.EntityID,
			Version:     body.Version,
			RequestedBy: claims.Subject,
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "config_archive_version: gRPC-вызов Configuration Service не удался", err)
			return
		}
		writeConfigVersionResponse(w, resp)
	}
}

type configVersionResponse struct {
	EntityType string `json:"entity_type"`
	EntityID   string `json:"entity_id"`
	Version    int64  `json:"version"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at,omitempty"`
}

func toConfigVersionResponse(v *grpcv1.ConfigVersionResponse) configVersionResponse {
	return configVersionResponse{
		EntityType: v.GetEntityType().String(),
		EntityID:   v.GetEntityId(),
		Version:    v.GetVersion(),
		Status:     v.GetStatus(),
		CreatedAt:  formatTimestamp(v.GetCreatedAt()),
	}
}

func writeConfigVersionResponse(w http.ResponseWriter, v *grpcv1.ConfigVersionResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toConfigVersionResponse(v))
}
