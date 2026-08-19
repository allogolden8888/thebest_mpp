package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// CallbackPayload — REST-контракт партнёрского callback'а, нигде не
// специфицирован дословно ни в одном документе этой сессии
// (`service_io_contracts.md` §7.1 говорит только "Партнёр | REST callback
// / WebSocket | статус") — конкретное, обоснованное решение здесь, тот же
// уровень детализации, что `partner-rest-receiver`'s собственный входной
// REST-контракт (см. его README): простой, стабильный JSON, поля
// напрямую из `MessageLifecycleEvent`.
type CallbackPayload struct {
	MessageID        string `json:"message_id"`
	Status           string `json:"status"`
	LifecycleVersion int64  `json:"lifecycle_version"`
	Terminal         bool   `json:"terminal"`
	OccurredAt       string `json:"occurred_at"`
}

type RestClient struct {
	client    *http.Client
	timeout   time.Duration
	ssrfGuard bool
}

// NewRestClient — production-конструктор, SSRF guard ВСЕГДА включён (см.
// ssrf_guard.go). notification_callback_url — конфигурационное значение
// партнёра, не что-то, что этот сервис должен доверять слепо.
func NewRestClient(timeout time.Duration) *RestClient {
	return newRestClient(timeout, true)
}

// newRestClientWithoutSSRFGuard — ТОЛЬКО для тестов, гоняющих
// httptest.Server (http://127.0.0.1:PORT — loopback, который production
// guard корректно отверг бы). Отдельный конструктор, а не флаг по
// умолчанию, чтобы production-путь (NewRestClient) физически не мог
// оказаться незащищённым по забытому аргументу.
func newRestClientWithoutSSRFGuard(timeout time.Duration) *RestClient {
	return newRestClient(timeout, false)
}

func newRestClient(timeout time.Duration, ssrfGuard bool) *RestClient {
	client := &http.Client{Timeout: timeout}
	if ssrfGuard {
		client.Transport = &http.Transport{DialContext: ssrfSafeDialer().DialContext, MaxIdleConnsPerHost: 64}
	}
	return &RestClient{client: client, timeout: timeout, ssrfGuard: ssrfGuard}
}

func (c *RestClient) SendCallback(ctx context.Context, callbackURL string, event *eventsv1.MessageLifecycleEvent) (Outcome, error) {
	if c.ssrfGuard {
		if err := checkScheme(callbackURL); err != nil {
			return OutcomePermanentFailure, err
		}
	}

	payload := CallbackPayload{
		MessageID:        event.GetMessageId(),
		Status:           event.GetStatus().String(),
		LifecycleVersion: event.GetLifecycleVersion(),
		Terminal:         event.GetTerminal(),
		OccurredAt:       event.GetOccurredAt().AsTime().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return OutcomePermanentFailure, fmt.Errorf("marshal callback payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		return OutcomePermanentFailure, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		var ssrfErr *SSRFRejectedError
		if errors.As(err, &ssrfErr) {
			// Guard отверг сам адрес (DNS rebinding в приватный/metadata IP
			// между checkScheme и dial'ом, либо редирект на такой адрес) —
			// повтор того же URL никогда не поможет, не Retryable.
			return OutcomePermanentFailure, fmt.Errorf("POST %s: %w", callbackURL, err)
		}
		return OutcomeRetryable, fmt.Errorf("POST %s: %w", callbackURL, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return OutcomeDelivered, nil
	// 4xx (кроме 429) — партнёрский endpoint отверг запрос по причине,
	// которую повтор не исправит (malformed URL config, авторизация и
	// т.п.) — permanent, не retryable.
	case resp.StatusCode == http.StatusTooManyRequests:
		return OutcomeRetryable, nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return OutcomePermanentFailure, fmt.Errorf("callback вернул %d", resp.StatusCode)
	default:
		return OutcomeRetryable, fmt.Errorf("callback вернул %d", resp.StatusCode)
	}
}
