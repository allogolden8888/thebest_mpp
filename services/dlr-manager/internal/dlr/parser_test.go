package dlr

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func validDlr() *eventsv1.OperatorDlr {
	return &eventsv1.OperatorDlr{
		OperatorId:    "beeline",
		Protocol:      commonv1.Protocol_PROTOCOL_SMPP,
		SmscMessageId: "smsc-123",
		SegmentId:     1,
		RawStatus:     "DELIVRD",
		ReceivedAt:    timestamppb.New(time.Now()),
	}
}

func TestDecodeValidDlr(t *testing.T) {
	payload, _ := proto.Marshal(validDlr())
	event, err := DecodeOperatorDlr(payload)
	if err != nil {
		t.Fatalf("DecodeOperatorDlr: %v", err)
	}
	if event.GetOperatorId() != "beeline" || event.GetSmscMessageId() != "smsc-123" {
		t.Errorf("unexpected fields: %+v", event)
	}
}

func TestDecodeRejectsMissingOperatorId(t *testing.T) {
	d := validDlr()
	d.OperatorId = ""
	payload, _ := proto.Marshal(d)
	if _, err := DecodeOperatorDlr(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий operator_id")
	}
}

func TestDecodeRejectsMissingSmscMessageId(t *testing.T) {
	// В отличие от OperatorSubmitAccepted, здесь smsc_message_id ОБЯЗАТЕЛЕН
	// — DLR физически не сопоставить ни с чем без него.
	d := validDlr()
	d.SmscMessageId = ""
	payload, _ := proto.Marshal(d)
	if _, err := DecodeOperatorDlr(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий smsc_message_id")
	}
}

func TestDecodeRejectsGarbageBytes(t *testing.T) {
	if _, err := DecodeOperatorDlr([]byte{0xFF, 0xFE, 0x00}); err == nil {
		t.Fatal("ожидали ошибку unmarshal")
	}
}

func TestNormalizeStatusKnownCodes(t *testing.T) {
	cases := map[string]string{
		"DELIVRD": "DELIVERED",
		"EXPIRED": "UNDELIVERABLE",
		"DELETED": "UNDELIVERABLE",
		"UNDELIV": "UNDELIVERABLE",
		"REJECTD": "UNDELIVERABLE",
	}
	for raw, want := range cases {
		got, recognized := NormalizeStatus(raw)
		if !recognized {
			t.Errorf("%s: ожидали recognized=true", raw)
		}
		if got != want {
			t.Errorf("%s: got %s, want %s", raw, got, want)
		}
	}
}

func TestNormalizeStatusNonTerminalCodesNotRecognized(t *testing.T) {
	for _, raw := range []string{"ACCEPTD", "UNKNOWN", "SOME_OPERATOR_SPECIFIC_CODE"} {
		_, recognized := NormalizeStatus(raw)
		if recognized {
			t.Errorf("%s: не должен считаться распознанным терминальным статусом", raw)
		}
	}
}

func TestDeriveEventIDIsDeterministic(t *testing.T) {
	d1 := validDlr()
	d2 := validDlr()
	d2.ReceivedAt = d1.ReceivedAt // тот же received_at — иначе оба сгенерированы time.Now() в разные наносекунды

	id1 := DeriveEventID(d1)
	id2 := DeriveEventID(d2)
	if id1 != id2 {
		t.Fatalf("одинаковый OperatorDlr должен давать одинаковый event_id: %s != %s", id1, id2)
	}
}

func TestDeriveEventIDDiffersForDifferentDlrs(t *testing.T) {
	d1 := validDlr()
	d2 := validDlr()
	d2.SmscMessageId = "different-smsc-id"

	if DeriveEventID(d1) == DeriveEventID(d2) {
		t.Fatal("разные DLR не должны давать одинаковый event_id")
	}
}
