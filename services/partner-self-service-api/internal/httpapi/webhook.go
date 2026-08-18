// Package httpapi — GET/PUT /v1/self-service/applications/{application_id}/webhook
// + POST .../webhook/test (Фаза 3 плана,
// /Users/Alisher/.claude/plans/luminous-hugging-charm.md). notification_callback_url
// живёт ВНУТРИ конкретного application, не на верхнем уровне partnerConfig
// (config_schemas/partner.schema.json $defs.application.properties.
// notification_callback_url) — read-modify-write здесь переиспользует те же
// getPartnerConfig/putPartnerConfig и типы application/partnerConfig, что
// applications.go/senders.go (см. partnerconfig.go), собственных копий не
// заводит.
//
// Персистентность истории test-send (partnerportal.webhook_test_log из
// исходного плана) сознательно отложена в этом проходе: миграции для этой
// таблицы нет, а Deps (router.go) не несёт Postgres-пул — заводить его
// здесь означало бы редактировать main.go/router.go, которыми в этом
// проходе владеют параллельные агенты. Test-send поэтому синхронный
// request/response без записи куда-либо.
package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"

	"mpp/partner-self-service-api/internal/auth"
)

// webhookTestClientTimeout — этот хендлер синхронно дожидается ответа от
// URL, которым управляет сам партнёр (может быть медленным/недоступным
// сервисом), поэтому не bare http.Post — нужен собственный http.Client с
// жёстким таймаутом, чтобы не подвесить обработчик на неопределённое время.
const webhookTestClientTimeout = 10 * time.Second

// findApplication — общий поиск application по ID внутри уже прочитанного
// partnerConfig вызывающего партнёра. cfg всегда получен через
// getPartnerConfig(ctx, ..., claims.PartnerID) — чужой конфиг сюда попасть
// не может.
func findApplication(cfg partnerConfig, applicationID string) (int, bool) {
	for i := range cfg.Applications {
		if cfg.Applications[i].ApplicationID == applicationID {
			return i, true
		}
	}
	return 0, false
}

// mountWebhook — GET/PUT .../webhook читают/меняют notification_callback_url
// конкретного application в PARTNER-конфиге вызывающего; POST .../webhook/test
// делает синхронный тестовый POST на УЖЕ СОХРАНЁННЫЙ в конфиге URL. Целевой
// URL теста никогда не принимается от вызывающего (тело запроса test-send
// пустое) — иначе любой аутентифицированный партнёр мог бы превратить этот
// сервис в open SSRF-прокси на произвольный внутренний адрес.
func mountWebhook(r chi.Router, d Deps) {
	r.Get("/applications/{application_id}/webhook", handleGetWebhook(d))
	r.With(auth.RequireAdmin).Put("/applications/{application_id}/webhook", handlePutWebhook(d))
	// test-send не мутирует конфиг партнёра — только делает исходящий HTTP-
	// вызов на URL, который партнёр САМ уже сохранил через PUT выше. Тот же
	// конвенционный выбор, что compliance-api/consent.go: гейтить мутации,
	// оставлять чтения/side-effect-free действия открытыми любому валидному
	// partner-токену (partner-viewer тоже может проверить свой вебхук, не
	// только partner-admin).
	r.Post("/applications/{application_id}/webhook/test", handleTestWebhook(d))
}

func handleGetWebhook(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		applicationID := chi.URLParam(r, "application_id")

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

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			ApplicationID           string `json:"application_id"`
			NotificationCallbackURL string `json:"notification_callback_url"`
		}{applicationID, cfg.Applications[idx].NotificationCallbackURL})
	}
}

type putWebhookRequest struct {
	NotificationCallbackURL string `json:"notification_callback_url"`
}

// validate — URL позже будет вызываться (dial'иться) не только этим
// test-send хендлером, но и другими бэкенд-сервисами (Partner Notification
// Service) — поэтому здесь строгая проверка схемы, а не просто
// непустая строка: только http/https, ParseRequestURI (абсолютный URL с
// обязательным хостом), никаких file:// / javascript: и т.п.
func (req putWebhookRequest) validate() error {
	if req.NotificationCallbackURL == "" {
		return fmt.Errorf("notification_callback_url обязателен")
	}
	u, err := url.ParseRequestURI(req.NotificationCallbackURL)
	if err != nil {
		return fmt.Errorf("notification_callback_url — невалидный URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("notification_callback_url должен использовать схему http:// или https://")
	}
	if u.Host == "" {
		return fmt.Errorf("notification_callback_url должен содержать хост")
	}
	return nil
}

func handlePutWebhook(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		applicationID := chi.URLParam(r, "application_id")

		var req putWebhookRequest
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

		idx, found := findApplication(cfg, applicationID)
		if !found {
			http.Error(w, "application не найден в конфиге партнёра", http.StatusNotFound)
			return
		}

		cfg.Applications[idx].NotificationCallbackURL = req.NotificationCallbackURL

		if err := putPartnerConfig(r.Context(), d.ConfigClient, cfg, claims.Subject); err != nil {
			http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			ApplicationID           string `json:"application_id"`
			NotificationCallbackURL string `json:"notification_callback_url"`
		}{applicationID, req.NotificationCallbackURL})
	}
}

// webhookTestResult — единственный результат синхронного test-send. Не
// персистируется (см. комментарий в начале файла) — возвращается
// исключительно в теле ответа этого запроса.
type webhookTestResult struct {
	HTTPStatus int   `json:"http_status"`
	LatencyMs  int64 `json:"latency_ms"`
}

func handleTestWebhook(d Deps) http.HandlerFunc {
	client := &http.Client{Timeout: webhookTestClientTimeout}

	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}
		applicationID := chi.URLParam(r, "application_id")

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

		targetURL := cfg.Applications[idx].NotificationCallbackURL
		if targetURL == "" {
			http.Error(w, "webhook not configured for this application", http.StatusBadRequest)
			return
		}

		// targetURL взят исключительно из уже сохранённого конфига партнёра
		// (getPartnerConfig выше, ограничено claims.PartnerID) — тело этого
		// запроса не содержит и не может переопределить целевой URL, иначе
		// это был бы open SSRF-прокси на произвольный адрес по требованию
		// любого аутентифицированного вызывающего.
		payload, err := json.Marshal(struct {
			Event         string `json:"event"`
			ApplicationID string `json:"application_id"`
			SentAt        string `json:"sent_at"`
		}{"test", applicationID, time.Now().UTC().Format(time.RFC3339)})
		if err != nil {
			http.Error(w, "не удалось сериализовать тестовый payload", http.StatusInternalServerError)
			return
		}

		testReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, bytes.NewReader(payload))
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось построить тестовый запрос: %v", err), http.StatusInternalServerError)
			return
		}
		testReq.Header.Set("Content-Type", "application/json")

		start := time.Now()
		resp, err := client.Do(testReq)
		latency := time.Since(start)
		if err != nil {
			// Ошибка самого запроса (DNS, connection refused, таймаут) — это
			// отказ этого хендлера, не результат теста. Не 4xx: вызывающий
			// не ошибся, это webhook-эндпоинт партнёра не ответил.
			http.Error(w, fmt.Sprintf("тестовый запрос к webhook не выполнен: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		// Любой код ответа (включая non-2xx) — штатный результат test-send,
		// не ошибка хендлера: в этом и смысл теста — показать партнёру, что
		// реально вернул его endpoint.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(webhookTestResult{
			HTTPStatus: resp.StatusCode,
			LatencyMs:  latency.Milliseconds(),
		})
	}
}
