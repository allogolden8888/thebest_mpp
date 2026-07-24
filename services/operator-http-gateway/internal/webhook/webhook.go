// Package webhook — handle_dlr_webhook + normalize_and_publish_dlr
// (service_internal_methods.md §1.3a): входящий HTTP POST от оператора ->
// webhook_auth проверка -> RawDlr -> тот же формат, что сырой SMPP DLR
// (OperatorDlr, platform-contracts/events/operator_events.proto) —
// "нормализация" здесь означает единый формат между протоколами, не
// операторский код -> normalized_status (это DLR Manager, platform_contracts.md §4).
package webhook

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Authenticator — webhook_auth (services_specifictaion.md §2.9a: "подпись/
// токен для валидации входящего DLR"). StaticTokenAuthenticator — заглушка
// на один общий secret (реальный источник — per-operator config snapshot,
// не подключён, см. README).
type Authenticator interface {
	Authenticate(r *http.Request, body []byte) bool
}

type StaticTokenAuthenticator struct {
	Token string
}

func (a StaticTokenAuthenticator) Authenticate(r *http.Request, body []byte) bool {
	return r.Header.Get("Authorization") == "Bearer "+a.Token
}

// DlrWebhookPayload — рабочее предположение формата (см. пакетный докстринг
// httpio — ни один документ не специфицирует конкретный per-operator формат).
type DlrWebhookPayload struct {
	SmscMessageID string `json:"smsc_message_id"`
	Status        string `json:"status"`
}

// RawDlr — handle_dlr_webhook выход, до публикации в OperatorDlr proto.
type RawDlr struct {
	SmscMessageID string
	RawStatus     string
}

// ParseDlrWebhookPayload — чистая функция, разбор тела запроса.
func ParseDlrWebhookPayload(body []byte) (RawDlr, error) {
	var payload DlrWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return RawDlr{}, fmt.Errorf("unmarshal DLR webhook payload: %w", err)
	}
	if payload.SmscMessageID == "" {
		return RawDlr{}, fmt.Errorf("DLR webhook payload без smsc_message_id")
	}
	return RawDlr{SmscMessageID: payload.SmscMessageID, RawStatus: payload.Status}, nil
}

// Handler — HTTP-обработчик /webhook/dlr. onValid вызывается для успешно
// аутентифицированного и разобранного RawDlr (публикация в Kafka —
// ответственность вызывающей стороны, см. cmd/.../main.go).
func Handler(authenticator Authenticator, onValid func(RawDlr)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !authenticator.Authenticate(r, body) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		dlr, err := ParseDlrWebhookPayload(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		onValid(dlr)
		w.WriteHeader(http.StatusOK)
	}
}
