// Package webhook — handle_dlr_webhook + normalize_and_publish_dlr
// (service_internal_methods.md §1.3a): входящий HTTP POST от оператора ->
// webhook_auth проверка -> RawDlr -> тот же формат, что сырой SMPP DLR
// (OperatorDlr, platform-contracts/events/operator_events.proto) —
// "нормализация" здесь означает единый формат между протоколами, не
// операторский код -> normalized_status (это DLR Manager, platform_contracts.md §4).
package webhook

import (
	"context"
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
// токен для валидации входящего DLR").
type Authenticator interface {
	Authenticate(r *http.Request, body []byte) bool
}

// ExpectedTokenLookup — "для этого operator_id, какой сейчас действующий
// bearer-токен webhook". Composition (Configuration Redis credential_ref +
// Vault secret value + TTL cache) живёт в internal/webhookauth, не здесь —
// этот пакет только использует результат через узкий интерфейс, чтобы
// оставаться юнит-тестируемым без реального Redis/Vault (см. webhook_test.go).
type ExpectedTokenLookup interface {
	// ExpectedToken возвращает found=false (без ошибки), когда для
	// operatorID нет действующего токена (не сконфигурирован, конфиг ещё
	// не опубликован, auth.type не BEARER_TOKEN и т.п.) — OperatorTokenAuthenticator
	// трактует found=false и err!=nil ОДИНАКОВО (fail closed), см. Authenticate.
	ExpectedToken(ctx context.Context, operatorID string) (token string, found bool, err error)
}

// dummyOperatorToken — BACKOFFICE_ROADMAP.md P0#1 timing-safety: тот же
// принцип, что iam-service dummyHash (internal/store/store.go
// VerifyStaffCredentials) — известный/несуществующий operator_id ВСЕГДА
// выполняет один и тот же ConstantTimeCompare той же формы, что и запрос с
// реально известным operator_id, но неверным токеном. Без этого
// "operator_id не существует/не сконфигурирован" отвечал бы быстрее, чем
// "operator_id существует, токен неверный" (ExpectedToken для неизвестного
// вернулся бы раньше, до реального Vault-round-trip), раскрывая по времени
// ответа, какие operator_id вообще сконфигурированы в этой платформе.
const dummyOperatorToken = "timing-safety-dummy-operator-webhook-token"

// OperatorTokenAuthenticator — per-operator bearer-токен, resolve по
// operator_id из URL (r.PathValue("operator_id"), см. Handler/main.go route
// "/webhook/dlr/{operator_id}"). Заменяет прежний StaticTokenAuthenticator
// (один общий WEBHOOK_AUTH_TOKEN env var на ВСЕХ операторов сразу —
// BACKOFFICE_ROADMAP.md P0#1: утечка/ротация credential'а ОДНОГО оператора
// затрагивала всех остальных, и не было способа отозвать доступ только
// одному оператору).
type OperatorTokenAuthenticator struct {
	Lookup ExpectedTokenLookup
}

// Authenticate — CODE_REVIEW.md HIGH finding (унаследовано от прежнего
// StaticTokenAuthenticator): `==` сравнение строк не constant-time.
// subtle.ConstantTimeCompare сохранён здесь один-в-один, но теперь
// сравнивается с per-operator значением, не с одним общим на всех.
func (a OperatorTokenAuthenticator) Authenticate(r *http.Request, _ []byte) bool {
	got := []byte(r.Header.Get("Authorization"))
	operatorID := r.PathValue("operator_id")

	if operatorID == "" {
		subtle.ConstantTimeCompare(got, []byte("Bearer "+dummyOperatorToken))
		return false
	}

	expected, found, err := a.Lookup.ExpectedToken(r.Context(), operatorID)
	if err != nil || !found {
		subtle.ConstantTimeCompare(got, []byte("Bearer "+dummyOperatorToken))
		return false
	}

	want := []byte("Bearer " + expected)
	return subtle.ConstantTimeCompare(got, want) == 1
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
//
// OperatorID — BACKOFFICE_ROADMAP.md P0#1: раньше отсутствовал здесь,
// main.go тегировало КАЖДЫЙ входящий DLR статическим OPERATOR_ID пода
// (env-переменная), независимо от того, какой оператор его физически
// прислал — на одном /webhook/dlr эндпоинте без per-operator идентификации
// это было единственным источником истины (неверным, если пул реплик
// когда-либо обслуживал больше одного оператора). Теперь заполняется
// Handler'ом из URL (/webhook/dlr/{operator_id}), не из env — тот же путь,
// что уже используется для резолва per-operator webhook-токена.
type RawDlr struct {
	OperatorID    string
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

// Handler — HTTP-обработчик /webhook/dlr/{operator_id}. onValid вызывается
// для успешно аутентифицированного и разобранного RawDlr (публикация в
// Kafka — ответственность вызывающей стороны, см. cmd/.../main.go).
func Handler(authenticator Authenticator, onValid func(RawDlr)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// operator_id обязателен маршрутом (main.go регистрирует
		// "/webhook/dlr/{operator_id}", не голый "/webhook/dlr") — пустое
		// значение здесь означало бы вызов handler'а в обход этого
		// маршрута (например напрямую в тесте) или экзотический путь вроде
		// "/webhook/dlr//", который net/http пропускает как пустой
		// PathValue, а не 404. Явная проверка вместо того, чтобы дать
		// Authenticate молча провалиться на operatorID="".
		if r.PathValue("operator_id") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

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
		dlr.OperatorID = r.PathValue("operator_id")

		onValid(dlr)
		w.WriteHeader(http.StatusOK)
	}
}
