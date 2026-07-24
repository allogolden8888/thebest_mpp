package webhook

import (
	"bytes"
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

func TestHandlerRejectsUnauthenticatedRequest(t *testing.T) {
	auth := StaticTokenAuthenticator{Token: "secret"}
	var called bool
	handler := Handler(auth, func(dlr RawDlr) { called = true })

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr", bytes.NewReader([]byte(`{"smsc_message_id":"x","status":"DELIVRD"}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ожидали 401 без Authorization header, получили %d", rec.Code)
	}
	if called {
		t.Fatalf("onValid не должен вызываться для неаутентифицированного запроса")
	}
}

func TestHandlerAcceptsValidRequestAndInvokesCallback(t *testing.T) {
	auth := StaticTokenAuthenticator{Token: "secret"}
	var received RawDlr
	handler := Handler(auth, func(dlr RawDlr) { received = dlr })

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr", bytes.NewReader([]byte(`{"smsc_message_id":"smsc-1","status":"DELIVRD"}`)))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", rec.Code)
	}
	if received.SmscMessageID != "smsc-1" {
		t.Fatalf("onValid не получил корректный RawDlr: %+v", received)
	}
}

func TestHandlerRejectsMalformedBody(t *testing.T) {
	auth := StaticTokenAuthenticator{Token: "secret"}
	handler := Handler(auth, func(dlr RawDlr) {})

	req := httptest.NewRequest(http.MethodPost, "/webhook/dlr", bytes.NewReader([]byte(`not json`)))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ожидали 400 для невалидного JSON, получили %d", rec.Code)
	}
}
