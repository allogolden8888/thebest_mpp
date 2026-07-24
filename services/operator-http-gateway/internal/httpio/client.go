// Package httpio — send_http_submit + handle_http_submit_response
// (service_internal_methods.md §1.3a).
//
// **Открытый вопрос**: ни один документ этой сессии не специфицирует
// конкретный HTTP-формат submit-запроса/ответа per-operator (в отличие от
// SMPP, где протокол фиксирован стандартом). Здесь — рабочее предположение:
// простой JSON POST/JSON-ответ. Реальные операторские HTTP API (Узбекистан)
// потребуют per-operator адаптер поверх этого клиента — не реализовано,
// см. README.
package httpio

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type SubmitRequestPayload struct {
	DestinationAddress string `json:"destination_address"`
	ContentBase64       string `json:"content_base64"`
	Encoding            string `json:"encoding"`
	QueueMsgID          string `json:"queue_msg_id"`
}

type SubmitResponsePayload struct {
	Status        string `json:"status"` // "accepted" | "rejected"
	SmscMessageID string `json:"smsc_message_id"`
	ReasonCode    string `json:"reason_code"`
}

type OutcomeStatus int

const (
	OutcomeAccepted OutcomeStatus = iota
	OutcomeRejected
	OutcomeAmbiguous
)

type SubmitOutcome struct {
	Status        OutcomeStatus
	SmscMessageID string
	ReasonCode    string
}

// BuildSubmitRequest — чистая функция, сборка JSON-тела запроса.
func BuildSubmitRequest(destinationAddress string, content []byte, encoding, queueMsgID string) ([]byte, error) {
	payload := SubmitRequestPayload{
		DestinationAddress: destinationAddress,
		ContentBase64:      base64.StdEncoding.EncodeToString(content),
		Encoding:           encoding,
		QueueMsgID:         queueMsgID,
	}
	return json.Marshal(payload)
}

// ParseSubmitResponse — handle_http_submit_response: чистая функция, разбор
// HTTP-ответа оператора в SubmitOutcome. statusCode >= 500 или ошибка сети
// (обрабатывается вызывающей стороной) — Ambiguous, не Rejected (HLD §13:
// "ответ не получен/не дошёл до подтверждения").
func ParseSubmitResponse(statusCode int, body []byte) (SubmitOutcome, error) {
	if statusCode >= 500 {
		return SubmitOutcome{Status: OutcomeAmbiguous, ReasonCode: fmt.Sprintf("HTTP_%d", statusCode)}, nil
	}
	if statusCode >= 400 {
		return SubmitOutcome{Status: OutcomeRejected, ReasonCode: fmt.Sprintf("HTTP_%d", statusCode)}, nil
	}

	var payload SubmitResponsePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return SubmitOutcome{}, fmt.Errorf("unmarshal submit response: %w", err)
	}

	switch payload.Status {
	case "accepted":
		return SubmitOutcome{Status: OutcomeAccepted, SmscMessageID: payload.SmscMessageID}, nil
	case "rejected":
		return SubmitOutcome{Status: OutcomeRejected, ReasonCode: payload.ReasonCode}, nil
	default:
		return SubmitOutcome{Status: OutcomeAmbiguous, ReasonCode: "UNKNOWN_STATUS_" + payload.Status}, nil
	}
}

// Client — реальный HTTP-клиент. Не проверялся против реального
// оператора в этой песочнице (см. README).
type Client struct {
	httpClient *http.Client
}

func NewClient(timeout time.Duration) *Client {
	return &Client{httpClient: &http.Client{Timeout: timeout}}
}

// SubmitSegment — send_http_submit + handle_http_submit_response целиком:
// реальный HTTP POST на endpointURL.
func (c *Client) SubmitSegment(ctx context.Context, endpointURL, destinationAddress string, content []byte, encoding, queueMsgID string) (SubmitOutcome, error) {
	body, err := BuildSubmitRequest(destinationAddress, content, encoding, queueMsgID)
	if err != nil {
		return SubmitOutcome{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return SubmitOutcome{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Сетевая ошибка — ответ не получен, Ambiguous (не Rejected).
		return SubmitOutcome{Status: OutcomeAmbiguous, ReasonCode: "NETWORK_ERROR: " + err.Error()}, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return SubmitOutcome{}, err
	}

	return ParseSubmitResponse(resp.StatusCode, respBody)
}