// Package httpapi — GET/POST /v1/compliance/consent (Фаза 6 плана,
// /Users/Alisher/.claude/plans/luminous-hugging-charm.md). Чтение — прямой
// point-lookup в Runtime Redis (тот же формат ключей, что
// consent-cache-projector реально поддерживает живым — единственное
// действительно живое состояние сейчас, policy.subscriber_consent само по
// себе ничем не наполняется, см. src/projector.rs-эквивалент в Ф4 за тем
// же классом находки для policy_template). Запись — через
// ConfigServiceClient.CreateVersion(entity_type=SUBSCRIBER_CONSENT), НЕ
// напрямую в Redis и НЕ напрямую в Postgres — тот же единый путь записи,
// что configuration-service использует для этого entity_type
// (config_version_id=NULL, только outbox), сохраняет config.changes как
// единственный источник правды и естественный audit trail (кто/когда через
// requested_by).
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/compliance-api/internal/auth"
	"mpp/compliance-api/internal/redisio"
)

const createVersionTimeout = 5 * time.Second

// handleConsentLookup — GET /v1/compliance/consent?msisdn=. Всегда 200 с
// (возможно пустыми) списками — отсутствие блокировок для MSISDN является
// штатным результатом lookup'а, не ошибкой.
func handleConsentLookup(redis *redisio.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		msisdn := r.URL.Query().Get("msisdn")
		if msisdn == "" {
			http.Error(w, "msisdn обязателен", http.StatusBadRequest)
			return
		}

		status, err := redis.Lookup(r.Context(), msisdn)
		if err != nil {
			http.Error(w, fmt.Sprintf("consent lookup: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	}
}

// manualConsentRequest.Action — "block" (добавить в блэклист, в payload
// пишется status=active) или "unblock" (снять, status=archived) — тот же
// смысл status, что уже закреплён consent-cache-projector'ом за
// entity_type=subscriber_consent (append/delete по PRIMARY KEY, не
// version-based) — не изобретаем новую семантику здесь, переиспользуем
// существующую. status кладётся ВНУТРЬ payload_json (см. handleManualConsent),
// не в отдельное поле — config-event-publisher.ResolveStatus читает его
// оттуда и переносит в исходящий ConfigChangeEvent.status.
type manualConsentRequest struct {
	MSISDN     string `json:"msisdn"`
	ScopeType  string `json:"scope_type"` // "CATEGORY" | "SENDER"
	ScopeValue string `json:"scope_value"`
	Channel    string `json:"channel"`
	Action     string `json:"action"` // "block" | "unblock"
	Reason     string `json:"reason"`
}

func (req manualConsentRequest) validate() error {
	if req.MSISDN == "" {
		return fmt.Errorf("msisdn обязателен")
	}
	if req.ScopeType != "CATEGORY" && req.ScopeType != "SENDER" {
		return fmt.Errorf("scope_type должен быть CATEGORY или SENDER")
	}
	if req.ScopeValue == "" {
		return fmt.Errorf("scope_value обязателен")
	}
	if req.Channel == "" {
		return fmt.Errorf("channel обязателен")
	}
	if req.Action != "block" && req.Action != "unblock" {
		return fmt.Errorf("action должен быть block или unblock")
	}
	if req.Reason == "" {
		return fmt.Errorf("reason обязателен — ручная compliance-запись должна быть объяснена")
	}
	return nil
}

// handleManualConsent — POST /v1/compliance/consent: ручная блокировка/
// разблокировка в обход обычного потока STOP-ключевых слов (например, по
// требованию регулятора или жалобе, см. Фаза 6 плана). requested_by берётся
// из JWT sub (тот же паттерн, что backoffice-api — не из тела запроса).
func handleManualConsent(configClient grpcv1.ConfigServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req manualConsentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "невалидное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := req.validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		status := "active"
		if req.Action == "unblock" {
			status = "archived"
		}

		// payload обязан нести status начиная с Ф6-фикса
		// config_schemas/subscriber_consent.schema.json (см. коммит
		// "subscriber_consent: close status gap") — до этого фикса поля
		// status в payload не было вообще, и revocation была структурно
		// недостижима через весь config.changes pipeline. status здесь —
		// не отдельное поле ConfigChangeEvent верхнего уровня, а часть
		// payload_json, ровно тот же паттерн, что уже работает для
		// policy_template (config-event-publisher.ResolveStatus читает
		// его оттуда).
		payload, err := json.Marshal(struct {
			MSISDN     string `json:"msisdn"`
			ScopeType  string `json:"scope_type"`
			ScopeValue string `json:"scope_value"`
			Channel    string `json:"channel"`
			Status     string `json:"status"`
		}{req.MSISDN, req.ScopeType, req.ScopeValue, req.Channel, status})
		if err != nil {
			http.Error(w, "не удалось сериализовать payload", http.StatusInternalServerError)
			return
		}

		// entity_id — синтетический составной ключ, тот же PRIMARY KEY, что
		// policy.subscriber_consent (msisdn, scope_type, scope_value,
		// channel) — не провалидировано платформой ни для одного
		// entity_type (configuration-service нигде не проверяет
		// entity_id против содержимого payload, см. Ф4/Ф6 находки), но
		// осмысленный, воспроизводимый ключ для трассировки в аудите/логах.
		entityID := fmt.Sprintf("%s:%s:%s:%s", req.MSISDN, req.ScopeType, req.ScopeValue, req.Channel)

		ctx, cancel := context.WithTimeout(r.Context(), createVersionTimeout)
		defer cancel()

		resp, err := configClient.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
			EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
			EntityId:    entityID,
			PayloadJson: payload,
			RequestedBy: claims.Subject,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось записать consent-изменение: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			EntityID string `json:"entity_id"`
			Version  int64  `json:"version"`
			Status   string `json:"status"`
		}{resp.GetEntityId(), resp.GetVersion(), resp.GetStatus()})
	}
}
