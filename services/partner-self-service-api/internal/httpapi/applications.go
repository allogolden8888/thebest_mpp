// Package httpapi — GET/POST/PUT /v1/self-service/applications и
// GET/POST/PATCH /v1/self-service/senders (Фаза 3 плана,
// /Users/Alisher/.claude/plans/luminous-hugging-charm.md). Read-modify-write
// поверх PARTNER-документа через getPartnerConfig/putPartnerConfig
// (partnerconfig.go) — собственных копий типов не заводит.
package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"mpp/partner-self-service-api/internal/auth"
)

func mountApplications(r chi.Router, d Deps) {
	r.Get("/applications", handleListApplications(d))
	r.With(auth.RequireAdmin).Post("/applications", handleCreateApplication(d))
	r.With(auth.RequireAdmin).Put("/applications/{application_id}", handleUpdateApplication(d))
}

func mountSenders(r chi.Router, d Deps) {
	r.Get("/senders", handleListSenders(d))
	r.With(auth.RequireAdmin).Post("/senders", handleCreateSender(d))
	r.With(auth.RequireAdmin).Patch("/senders/{sender_id}", handleUpdateSenderStatus(d))
}

func handleListApplications(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg.Applications)
	}
}

// createApplicationRequest — намеренно отдельный от application тип: поле
// notification_callback_url на создании не задаётся здесь (см. webhook.go —
// отдельный PUT .../webhook эндпоинт для этого поля, единая точка записи).
type createApplicationRequest struct {
	ApplicationID   string          `json:"application_id"`
	DisplayName     string          `json:"display_name"`
	Auth            applicationAuth `json:"auth"`
	IPAllowlist     []string        `json:"ip_allowlist"`
	RateLimitTPS    int             `json:"rate_limit_tps"`
	AllowedChannels []string        `json:"allowed_channels"`
}

func (req createApplicationRequest) validate() error {
	if req.ApplicationID == "" {
		return fmt.Errorf("application_id обязателен")
	}
	if req.DisplayName == "" {
		return fmt.Errorf("display_name обязателен")
	}
	if req.Auth.Type == "" || req.Auth.CredentialRef == "" {
		return fmt.Errorf("auth.type и auth.credential_ref обязательны")
	}
	if req.RateLimitTPS <= 0 {
		return fmt.Errorf("rate_limit_tps должен быть положительным")
	}
	if len(req.AllowedChannels) == 0 {
		return fmt.Errorf("allowed_channels не может быть пустым")
	}
	return nil
}

func handleCreateApplication(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var req createApplicationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "невалидное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := req.validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		if _, found := findApplication(cfg, req.ApplicationID); found {
			http.Error(w, "application с таким application_id уже существует", http.StatusConflict)
			return
		}

		cfg.Applications = append(cfg.Applications, application{
			ApplicationID:   req.ApplicationID,
			DisplayName:     req.DisplayName,
			Auth:            req.Auth,
			IPAllowlist:     req.IPAllowlist,
			RateLimitTPS:    req.RateLimitTPS,
			AllowedChannels: req.AllowedChannels,
		})

		if err := putPartnerConfig(r.Context(), d.ConfigClient, cfg, claims.Subject); err != nil {
			http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(cfg.Applications[len(cfg.Applications)-1])
	}
}

// updateApplicationRequest — auth.credential_ref намеренно исключён:
// выпуск/ротация секрета — исключительно через credentials.go
// (CredentialIssuerService, Ф1), менять credential_ref напрямую здесь
// означало бы расходиться с тем, что реально лежит в Vault.
type updateApplicationRequest struct {
	DisplayName             string   `json:"display_name"`
	IPAllowlist             []string `json:"ip_allowlist"`
	RateLimitTPS            int      `json:"rate_limit_tps"`
	AllowedChannels         []string `json:"allowed_channels"`
	NotificationCallbackURL string   `json:"notification_callback_url"`
}

func handleUpdateApplication(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		applicationID := chi.URLParam(r, "application_id")

		var req updateApplicationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "невалидное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.DisplayName == "" {
			http.Error(w, "display_name обязателен", http.StatusBadRequest)
			return
		}
		if req.RateLimitTPS <= 0 {
			http.Error(w, "rate_limit_tps должен быть положительным", http.StatusBadRequest)
			return
		}
		if len(req.AllowedChannels) == 0 {
			http.Error(w, "allowed_channels не может быть пустым", http.StatusBadRequest)
			return
		}

		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		idx, found := findApplication(cfg, applicationID)
		if !found {
			http.Error(w, "application не найден в конфиге партнёра", http.StatusNotFound)
			return
		}

		cfg.Applications[idx].DisplayName = req.DisplayName
		cfg.Applications[idx].IPAllowlist = req.IPAllowlist
		cfg.Applications[idx].RateLimitTPS = req.RateLimitTPS
		cfg.Applications[idx].AllowedChannels = req.AllowedChannels
		cfg.Applications[idx].NotificationCallbackURL = req.NotificationCallbackURL

		if err := putPartnerConfig(r.Context(), d.ConfigClient, cfg, claims.Subject); err != nil {
			http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg.Applications[idx])
	}
}

func findSender(cfg partnerConfig, senderID string) (int, bool) {
	for i := range cfg.Senders {
		if cfg.Senders[i].SenderID == senderID {
			return i, true
		}
	}
	return 0, false
}

func handleListSenders(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg.Senders)
	}
}

type createSenderRequest struct {
	SenderID string `json:"sender_id"`
	Type     string `json:"type"`
}

func (req createSenderRequest) validate() error {
	if req.SenderID == "" {
		return fmt.Errorf("sender_id обязателен")
	}
	if req.Type != "ALPHANAME" && req.Type != "SHORT_NUMBER" {
		return fmt.Errorf("type должен быть ALPHANAME или SHORT_NUMBER")
	}
	return nil
}

func handleCreateSender(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		var req createSenderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "невалидное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := req.validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		if _, found := findSender(cfg, req.SenderID); found {
			http.Error(w, "sender с таким sender_id уже существует", http.StatusConflict)
			return
		}

		// status всегда "active" на создании — вызывающий не может завести
		// сразу archived-запись, это бессмысленно (archived — только через
		// PATCH ниже, как явный акт отзыва уже активного sender'а).
		cfg.Senders = append(cfg.Senders, sender{SenderID: req.SenderID, Type: req.Type, Status: "active"})

		if err := putPartnerConfig(r.Context(), d.ConfigClient, cfg, claims.Subject); err != nil {
			http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(cfg.Senders[len(cfg.Senders)-1])
	}
}

type updateSenderStatusRequest struct {
	Status string `json:"status"`
}

func handleUpdateSenderStatus(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		senderID := chi.URLParam(r, "sender_id")

		var req updateSenderStatusRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "невалидное тело запроса: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Status != "active" && req.Status != "archived" {
			http.Error(w, "status должен быть active или archived", http.StatusBadRequest)
			return
		}

		cfg, err := getPartnerConfig(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		idx, found := findSender(cfg, senderID)
		if !found {
			http.Error(w, "sender не найден в конфиге партнёра", http.StatusNotFound)
			return
		}

		cfg.Senders[idx].Status = req.Status

		if err := putPartnerConfig(r.Context(), d.ConfigClient, cfg, claims.Subject); err != nil {
			http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg.Senders[idx])
	}
}
