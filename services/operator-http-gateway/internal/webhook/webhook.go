// Package webhook — handle_dlr_webhook + normalize_and_publish_dlr
// (service_internal_methods.md §1.3a): входящий HTTP POST от оператора ->
// webhook_auth проверка -> RawDlr -> тот же формат, что сырой SMPP DLR
// (OperatorDlr, platform-contracts/events/operator_events.proto) —
// "нормализация" здесь означает единый формат между протоколами, не
// операторский код -> normalized_status (это DLR Manager, platform_contracts.md §4).
package webhook

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxWebhookBodyBytes — CODE_REVIEW.md CRITICAL finding: io.ReadAll(r.Body)
// раньше выполнялся без http.MaxBytesReader и без ReadTimeout/
// MaxHeaderBytes на сервере (см. cmd/operator-http-gateway/main.go) —
// поскольку этот эндпоинт обязан быть доступен из интернета (реальные
// операторы), любой неаутентифицированный вызывающий мог прислать
// произвольно большое или медленно льющееся тело и вызвать неограниченную
// буферизацию в памяти без таймаута на прерывание чтения — тривиальный
// DoS без единого валидного credential'а. 64 КиБ — с большим запасом
// покрывает реальный DLR-payload (несколько строковых полей).
const maxWebhookBodyBytes = 64 * 1024

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

// Authenticate — CODE_REVIEW.md HIGH finding: раньше `==` сравнение
// строк — не constant-time, время сравнения зависит от длины совпавшего
// префикса, теоретически позволяя восстановить токен побайтово через
// замеры времени ответа. subtle.ConstantTimeCompare требует равной длины
// операндов — сравниваем через промежуточный fixed-size хэш не нужно,
// т.к. ConstantTimeCompare сам возвращает 0 при разной длине без утечки
// через ранний return (в отличие от `==`).
func (a StaticTokenAuthenticator) Authenticate(r *http.Request, body []byte) bool {
	got := r.Header.Get("Authorization")
	want := "Bearer " + a.Token
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// DlrWebhookPayload — рабочее предположение формата (см. пакетный докстринг
// httpio — ни один документ не специфицирует конкретный per-operator формат).
//
// SegmentID — CODE_REVIEW.md MEDIUM finding: раньше отсутствовал вообще,
// OperatorDlr.segment_id (platform-contracts/events/operator_events.proto)
// никогда не заполнялся — per-segment DLR-корреляция для HTTP-пути была
// фактически нереализована (DLR для сегмента 1/2 многосегментного SMS не
// мог быть сопоставлен с конкретным сегментом). Опционален в payload —
// оператор, не поддерживающий multi-part DLR, может его не присылать
// (тогда 0, тот же дефолт, что и раньше для single-segment сообщений).
type DlrWebhookPayload struct {
	SmscMessageID string `json:"smsc_message_id"`
	SegmentID     int32  `json:"segment_id"`
	Status        string `json:"status"`
}

// RawDlr — handle_dlr_webhook выход, до публикации в OperatorDlr proto.
type RawDlr struct {
	SmscMessageID string
	SegmentID     int32
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
	return RawDlr{SmscMessageID: payload.SmscMessageID, SegmentID: payload.SegmentID, RawStatus: payload.Status}, nil
}

// Handler — HTTP-обработчик /webhook/dlr. onValid вызывается для успешно
// аутентифицированного и разобранного RawDlr (публикация в Kafka —
// ответственность вызывающей стороны, см. cmd/.../main.go).
func Handler(authenticator Authenticator, onValid func(RawDlr)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
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
