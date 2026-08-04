package httpio

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBuildSubmitRequestEncodesContentAsBase64(t *testing.T) {
	body, err := BuildSubmitRequest("998901234567", []byte("hello"), "GSM7", "queue-1")
	if err != nil {
		t.Fatalf("BuildSubmitRequest failed: %v", err)
	}
	var payload SubmitRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if payload.DestinationAddress != "998901234567" {
		t.Fatalf("неверный destination_address: %s", payload.DestinationAddress)
	}
	decoded, _ := base64.StdEncoding.DecodeString(payload.ContentBase64)
	if string(decoded) != "hello" {
		t.Fatalf("content_base64 не декодируется в исходный текст: %s", decoded)
	}
}

func TestParseSubmitResponseAccepted(t *testing.T) {
	body := []byte(`{"status":"accepted","smsc_message_id":"smsc-1"}`)
	outcome, err := ParseSubmitResponse(200, body)
	if err != nil {
		t.Fatalf("ParseSubmitResponse failed: %v", err)
	}
	if outcome.Status != OutcomeAccepted || outcome.SmscMessageID != "smsc-1" {
		t.Fatalf("неверный outcome: %+v", outcome)
	}
}

func TestParseSubmitResponseRejected(t *testing.T) {
	body := []byte(`{"status":"rejected","reason_code":"INVALID_DESTINATION"}`)
	outcome, err := ParseSubmitResponse(200, body)
	if err != nil {
		t.Fatalf("ParseSubmitResponse failed: %v", err)
	}
	if outcome.Status != OutcomeRejected || outcome.ReasonCode != "INVALID_DESTINATION" {
		t.Fatalf("неверный outcome: %+v", outcome)
	}
}

func TestParseSubmitResponseServerErrorIsAmbiguousNotRejected(t *testing.T) {
	outcome, err := ParseSubmitResponse(503, nil)
	if err != nil {
		t.Fatalf("ParseSubmitResponse failed: %v", err)
	}
	if outcome.Status != OutcomeAmbiguous {
		t.Fatalf("5xx должен быть Ambiguous (ответ не подтверждён), не Rejected — HLD §13, получили %v", outcome.Status)
	}
}

func TestParseSubmitResponseClientErrorIsRejected(t *testing.T) {
	outcome, err := ParseSubmitResponse(400, nil)
	if err != nil {
		t.Fatalf("ParseSubmitResponse failed: %v", err)
	}
	if outcome.Status != OutcomeRejected {
		t.Fatalf("4xx должен быть Rejected, получили %v", outcome.Status)
	}
}

// TestClientSubmitSegmentRealHttpRoundTrip — реальный HTTP round-trip против
// httptest.Server (не мок транспорта).
func TestClientSubmitSegmentRealHttpRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload SubmitRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode request failed: %v", err)
		}
		if payload.DestinationAddress != "998901234567" {
			t.Errorf("неверный destination_address получен сервером: %s", payload.DestinationAddress)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SubmitResponsePayload{Status: "accepted", SmscMessageID: "smsc-42"})
	}))
	defer srv.Close()

	client := NewClientForTests(2 * time.Second)
	outcome, err := client.SubmitSegment(t.Context(), srv.URL, "998901234567", []byte("hello"), "GSM7", "queue-1")
	if err != nil {
		t.Fatalf("SubmitSegment failed: %v", err)
	}
	if outcome.Status != OutcomeAccepted || outcome.SmscMessageID != "smsc-42" {
		t.Fatalf("неверный outcome: %+v", outcome)
	}
}