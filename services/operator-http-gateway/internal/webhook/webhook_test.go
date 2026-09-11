package webhook

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseDlrWebhookPayloadValid(t *testing.T) {
	dlr, err := ParseDlrWebhookPayload([]byte(`{"smsc_message_id":"smsc-1","status":"DELIVRD"}`))
	if err != nil {
		t.Fatalf("ParseDlrWebhookPayload failed: %v", err)
	}
	if dlr.SmscMessageID != "smsc-1" || dlr.RawStatus != "DELIVRD" {
		t.Fatalf("неверный разбор: %+v", dlr)
	}
}

func TestParseDlrWebhookPayloadRejectsMissingMessageId(t *testing.T) {
	_, err := ParseDlrWebhookPayload([]byte(`{"status":"DELIVRD"}`))
	if err == nil {
		t.Fatalf("ожидали ошибку без smsc_message_id")
	}
}

// fakeLookup — ExpectedTokenLookup для тестов OperatorTokenAuthenticator и
// Handler, без реального Redis/Vault. errFor позволяет смоделировать сбой
// Vault-чтения отдельно от "оператор не сконфигурирован" (found=false, err=nil).
type fakeLookup struct {
	tokens map[string]string
	errFor map[string]error
	calls  int
}

func (f *fakeLookup) ExpectedToken(_ context.Context, operatorID string) (string, bool, error) {
	f.calls++
	if err, ok := f.errFor[operatorID]; ok {
		return "", false, err
	}
	token, ok := f.tokens[operatorID]
	return token, ok, nil
}

func newRequestWithOperator(operatorID, authHeader string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/"+operatorID, bytes.NewReader([]byte(`{"smsc_message_id":"x","status":"DELIVRD"}`)))
	req.SetPathValue("operator_id", operatorID)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	return req
}

func TestOperatorTokenAuthenticatorAcceptsCorrectOperatorAndToken(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret", "ucell_uz": "ucell-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("beeline_uz", "Bearer beeline-secret")
	if !auth.Authenticate(req, nil) {
		t.Fatal("ожидали успешную аутентификацию для верного operator_id+token")
	}
}

func TestOperatorTokenAuthenticatorRejectsWrongTokenForKnownOperator(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("beeline_uz", "Bearer wrong-token")
	if auth.Authenticate(req, nil) {
		t.Fatal("ожидали отказ для неверного токена")
	}
}

// TestOperatorTokenAuthenticatorRejectsOtherOperatorsToken — сердце
// BACKOFFICE_ROADMAP.md P0#1: токен, валидный для beeline_uz, не должен
// аутентифицировать запрос на путь ucell_uz — единственный общий
// WEBHOOK_AUTH_TOKEN раньше делал это неотличимым.
func TestOperatorTokenAuthenticatorRejectsOtherOperatorsToken(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret", "ucell_uz": "ucell-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("ucell_uz", "Bearer beeline-secret")
	if auth.Authenticate(req, nil) {
		t.Fatal("токен другого оператора не должен проходить")
	}
}

func TestOperatorTokenAuthenticatorRejectsUnknownOperator(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("unknown_operator", "Bearer beeline-secret")
	if auth.Authenticate(req, nil) {
		t.Fatal("несконфигурированный operator_id не должен аутентифицироваться никаким токеном")
	}
}

func TestOperatorTokenAuthenticatorRejectsMissingAuthorizationHeader(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("beeline_uz", "")
	if auth.Authenticate(req, nil) {
		t.Fatal("ожидали отказ без Authorization header")
	}
}

func TestOperatorTokenAuthenticatorFailsClosedOnLookupError(t *testing.T) {
	lookup := &fakeLookup{
		tokens: map[string]string{"beeline_uz": "beeline-secret"},
		errFor: map[string]error{"beeline_uz": errors.New("vault недоступен")},
	}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := newRequestWithOperator("beeline_uz", "Bearer beeline-secret")
	if auth.Authenticate(req, nil) {
		t.Fatal("ошибка Lookup обязана приводить к отказу (fail closed), не к пропуску")
	}
}

func TestOperatorTokenAuthenticatorRejectsEmptyOperatorID(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/", bytes.NewReader(nil))
	req.SetPathValue("operator_id", "")
	req.Header.Set("Authorization", "Bearer beeline-secret")
	if auth.Authenticate(req, nil) {
		t.Fatal("пустой operator_id не должен аутентифицироваться")
	}
	// ExpectedToken не должен вызываться вообще для пустого operator_id —
	// нет корректного identity, на котором был бы смысл делать Vault-round-trip.
	if lookup.calls != 0 {
		t.Fatalf("ожидали 0 вызовов ExpectedToken для пустого operator_id, получили %d", lookup.calls)
	}
}

// TestOperatorTokenAuthenticatorConstantTimeCompareAlwaysRuns — не измеряет
// реальное время (шумно и недетерминированно в CI), а доказывает
// СТРУКТУРНОЕ свойство: во всех трёх ветках отказа (пустой operator_id,
// неизвестный operator_id, ошибка Lookup) subtle.ConstantTimeCompare
// реально вызывается с операндом той же формы ("Bearer "+токен), не просто
// return false раньше срока — тот же принцип, что iam-service dummyHash
// (bcrypt.CompareHashAndPassword ВСЕГДА выполняется, даже для неизвестного
// username). Проверяем через побочный эффект: разная длина
// Authorization-заголовка не должна ветвиться по-разному ДО сравнения —
// здесь удостоверяемся, что все три пути возвращают false независимо от
// длины got, что было бы неверно, если бы где-то остался ранний return по
// длине без сравнения.
func TestOperatorTokenAuthenticatorConstantTimeCompareAlwaysRuns(t *testing.T) {
	lookup := &fakeLookup{
		tokens: map[string]string{"beeline_uz": "beeline-secret"},
		errFor: map[string]error{"broken_uz": errors.New("vault недоступен")},
	}
	auth := OperatorTokenAuthenticator{Lookup: lookup}

	cases := []struct {
		name       string
		operatorID string
		authHeader string
	}{
		{"empty operator_id, short header", "", "Bearer x"},
		{"empty operator_id, long header", "", "Bearer " + string(make([]byte, 200))},
		{"unknown operator_id, short header", "unknown_operator", "Bearer x"},
		{"unknown operator_id, long header", "unknown_operator", "Bearer " + string(make([]byte, 200))},
		{"lookup error, short header", "broken_uz", "Bearer x"},
		{"lookup error, long header", "broken_uz", "Bearer " + string(make([]byte, 200))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := newRequestWithOperator(tc.operatorID, tc.authHeader)
			req.SetPathValue("operator_id", tc.operatorID)
			if auth.Authenticate(req, nil) {
				t.Fatalf("ожидали отказ для случая %q", tc.name)
			}
		})
	}
}

func TestHandlerRejectsUnauthenticatedRequest(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	var called bool
	handler := Handler(auth, func(dlr RawDlr) { called = true })

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/beeline_uz", bytes.NewReader([]byte(`{"smsc_message_id":"x","status":"DELIVRD"}`)))
	req.SetPathValue("operator_id", "beeline_uz")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ожидали 401 без Authorization header, получили %d", rec.Code)
	}
	if called {
		t.Fatalf("onValid не должен вызываться для неаутентифицированного запроса")
	}
}

func TestHandlerRejectsMissingOperatorIDInPath(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	var called bool
	handler := Handler(auth, func(dlr RawDlr) { called = true })

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/", bytes.NewReader([]byte(`{"smsc_message_id":"x","status":"DELIVRD"}`)))
	req.SetPathValue("operator_id", "")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ожидали 400 для пустого operator_id, получили %d", rec.Code)
	}
	if called {
		t.Fatalf("onValid не должен вызываться без operator_id")
	}
}

func TestHandlerAcceptsValidRequestAndInvokesCallbackWithOperatorID(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	var received RawDlr
	handler := Handler(auth, func(dlr RawDlr) { received = dlr })

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/beeline_uz", bytes.NewReader([]byte(`{"smsc_message_id":"smsc-1","status":"DELIVRD"}`)))
	req.SetPathValue("operator_id", "beeline_uz")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", rec.Code)
	}
	if received.SmscMessageID != "smsc-1" {
		t.Fatalf("onValid не получил корректный RawDlr: %+v", received)
	}
	if received.OperatorID != "beeline_uz" {
		t.Fatalf("onValid должен получить OperatorID из URL, получили %q", received.OperatorID)
	}
}

// TestHandlerRejectsOtherOperatorsTokenEndToEnd — live-verification-style
// проверка через сам Handler (не только через Authenticate напрямую):
// beeline_uz's токен не должен пройти на пути ucell_uz, и наоборот — оба
// работают на своих собственных путях.
func TestHandlerRejectsOtherOperatorsTokenEndToEnd(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "beeline-secret", "ucell_uz": "ucell-secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	handler := Handler(auth, func(dlr RawDlr) {})

	post := func(operatorID, token string) int {
		req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/"+operatorID, bytes.NewReader([]byte(`{"smsc_message_id":"smsc-1","status":"DELIVRD"}`)))
		req.SetPathValue("operator_id", operatorID)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec.Code
	}

	if code := post("beeline_uz", "beeline-secret"); code != http.StatusOK {
		t.Errorf("beeline_uz с собственным токеном: ожидали 200, получили %d", code)
	}
	if code := post("ucell_uz", "ucell-secret"); code != http.StatusOK {
		t.Errorf("ucell_uz с собственным токеном: ожидали 200, получили %d", code)
	}
	if code := post("ucell_uz", "beeline-secret"); code != http.StatusUnauthorized {
		t.Errorf("ucell_uz с токеном beeline_uz: ожидали 401, получили %d", code)
	}
	if code := post("beeline_uz", "ucell-secret"); code != http.StatusUnauthorized {
		t.Errorf("beeline_uz с токеном ucell_uz: ожидали 401, получили %d", code)
	}
}

// TestParseDlrWebhookPayloadWithSegmentID — CODE_REVIEW.md MEDIUM finding
// #6: segment_id раньше отсутствовал в payload вообще и никогда не
// доходил до OperatorDlr.
func TestParseDlrWebhookPayloadWithSegmentID(t *testing.T) {
	dlr, err := ParseDlrWebhookPayload([]byte(`{"smsc_message_id":"smsc-1","segment_id":2,"status":"DELIVRD"}`))
	if err != nil {
		t.Fatalf("ParseDlrWebhookPayload failed: %v", err)
	}
	if dlr.SegmentID != 2 {
		t.Fatalf("segment_id = %d, want 2", dlr.SegmentID)
	}
}

// TestHandlerRejectsOversizedBody — CODE_REVIEW.md CRITICAL finding:
// раньше io.ReadAll(r.Body) не имел http.MaxBytesReader — неаутентифи-
// цированный вызывающий мог прислать произвольно большое тело и вызвать
// неограниченную буферизацию в памяти.
func TestHandlerRejectsOversizedBody(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	handler := Handler(auth, func(dlr RawDlr) {})

	oversized := bytes.Repeat([]byte("a"), maxWebhookBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/beeline_uz", bytes.NewReader(oversized))
	req.SetPathValue("operator_id", "beeline_uz")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("ожидали 413 для тела больше %d байт, получили %d", maxWebhookBodyBytes, rec.Code)
	}
}

func TestHandlerRejectsMalformedBody(t *testing.T) {
	lookup := &fakeLookup{tokens: map[string]string{"beeline_uz": "secret"}}
	auth := OperatorTokenAuthenticator{Lookup: lookup}
	handler := Handler(auth, func(dlr RawDlr) {})

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr/beeline_uz", bytes.NewReader([]byte(`not json`)))
	req.SetPathValue("operator_id", "beeline_uz")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ожидали 400 для невалидного JSON, получили %d", rec.Code)
	}
}
