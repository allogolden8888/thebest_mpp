package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	EntityType  string          `json:"entity_type"`
	EntityID    string          `json:"entity_id"`
	Version     int64           `json:"version"`
	Status      string          `json:"status"`
	CreatedAt   string          `json:"created_at,omitempty"`
	PayloadJSON json.RawMessage `json:"payload_json,omitempty"`
}

// toConfigVersionResponse — CONFIG_ENTITY_TYPE_CATEGORY/_CTN (luminous-
// hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экраны 32/23) нашли этот
// пробел: ConfigVersionResponse.payload_json (internal_control.proto,
// заведено ради read-modify-write в partner-self-service-api) уже реально
// заполняется configuration-service'ом в GetActiveVersion/ListVersions/
// CreateVersion, но ни разу не прокидывался дальше в HTTP-ответ — ни одна
// сущность до сих пор не нуждалась в списке/просмотре payload'а без
// сравнения через /diff (ConfigView.vue сегодня — черновик payload вручную,
// без предзаполнения из списка). Category/CTN экраны — первые, которым
// нужно РЕАЛЬНО показать содержимое активных версий в таблице, не только
// entity_id/version/status — без этого поля список был бы бесполезен.
func toConfigVersionResponse(v *grpcv1.ConfigVersionResponse) configVersionResponse {
	return configVersionResponse{
		EntityType:  v.GetEntityType().String(),
		EntityID:    v.GetEntityId(),
		Version:     v.GetVersion(),
		Status:      v.GetStatus(),
		CreatedAt:   formatTimestamp(v.GetCreatedAt()),
		PayloadJSON: v.GetPayloadJson(),
	}
}

func writeConfigVersionResponse(w http.ResponseWriter, v *grpcv1.ConfigVersionResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toConfigVersionResponse(v))
}

type validateVersionRequestBody struct {
	EntityType  string          `json:"entity_type"`
	PayloadJSON json.RawMessage `json:"payload_json"`
}

// handleConfigValidateVersion — luminous-hugging-charm.md Ф10, POST
// /v1/config/versions/validate. Без gate прав — тот же класс, что
// read-only browse маршрутов (не мутирует ничего, ConfigService.
// ValidateVersion не пишет ни в config_versions, ни в config_outbox).
// Invalid payload — НЕ 400: valid=false с errors в теле 200-ответа, тот
// же принцип, что gRPC-уровне (см. package doc ValidateVersion в
// internal_control.proto) — "покажи, что не так" ожидаемый исход
// успешного запроса, не ошибка запроса как такового.
func handleConfigValidateVersion(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body validateVersionRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "неверное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		entityType, ok := parseEntityType(body.EntityType)
		if !ok {
			http.Error(w, "неизвестный entity_type: "+body.EntityType, http.StatusBadRequest)
			return
		}

		resp, err := client.ValidateVersion(r.Context(), &grpcv1.ValidateVersionRequest{
			EntityType:  entityType,
			PayloadJson: body.PayloadJSON,
		})
		if err != nil {
			internalError(w, http.StatusBadGateway, "config_validate_version: gRPC-вызов Configuration Service не удался", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Valid  bool     `json:"valid"`
			Errors []string `json:"errors"`
		}{Valid: resp.GetValid(), Errors: resp.GetErrors()})
	}
}

// handleConfigDiffVersions — luminous-hugging-charm.md Ф10, GET
// /v1/config/versions/diff?entity_type=&entity_id=&from=&to=. Возвращает
// оба payload_json как есть — вычисление самого diff остаётся на стороне
// backoffice-ui (см. package doc DiffVersions в internal_control.proto).
func handleConfigDiffVersions(client grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		entityType, ok := parseEntityType(q.Get("entity_type"))
		if !ok {
			http.Error(w, "неизвестный entity_type", http.StatusBadRequest)
			return
		}
		entityID := q.Get("entity_id")
		if entityID == "" {
			http.Error(w, "требуется entity_id", http.StatusBadRequest)
			return
		}
		fromVersion, err := strconv.ParseInt(q.Get("from"), 10, 64)
		if err != nil || fromVersion <= 0 {
			http.Error(w, "неверный from: ожидалось положительное целое", http.StatusBadRequest)
			return
		}
		toVersion, err := strconv.ParseInt(q.Get("to"), 10, 64)
		if err != nil || toVersion <= 0 {
			http.Error(w, "неверный to: ожидалось положительное целое", http.StatusBadRequest)
			return
		}

		resp, callErr := client.DiffVersions(r.Context(), &grpcv1.DiffVersionsRequest{
			EntityType:  entityType,
			EntityId:    entityID,
			FromVersion: fromVersion,
			ToVersion:   toVersion,
		})
		if callErr != nil {
			if status.Code(callErr) == codes.NotFound {
				http.Error(w, callErr.Error(), http.StatusNotFound)
				return
			}
			internalError(w, http.StatusBadGateway, "config_diff_versions: gRPC-вызов Configuration Service не удался", callErr)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			FromVersion     int64           `json:"from_version"`
			FromPayloadJSON json.RawMessage `json:"from_payload_json"`
			ToVersion       int64           `json:"to_version"`
			ToPayloadJSON   json.RawMessage `json:"to_payload_json"`
		}{
			FromVersion:     resp.GetFromVersion(),
			FromPayloadJSON: resp.GetFromPayloadJson(),
			ToVersion:       resp.GetToVersion(),
			ToPayloadJSON:   resp.GetToPayloadJson(),
		})
	}
}
