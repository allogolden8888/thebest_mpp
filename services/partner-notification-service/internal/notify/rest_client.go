package notify

import (
	"bytes"
	"context"
	"encoding/json"
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
	client  *http.Client
	timeout time.Duration
}

func NewRestClient(timeout time.Duration) *RestClient {
	return &RestClient{client: &http.Client{Timeout: timeout}, timeout: timeout}
}

func (c *RestClient) SendCallback(ctx context.Context, callbackURL string, event *eventsv1.MessageLifecycleEvent) (Outcome, error) {
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
