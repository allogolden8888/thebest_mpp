package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodePduLogRoundTrip(t *testing.T) {
	event := &eventsv1.OperatorPduLog{OperatorId: "beeline_uz", PduType: "SUBMIT_SM", SequenceNumber: 7}
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := DecodePduLog(payload)
	if err != nil {
		t.Fatalf("DecodePduLog failed: %v", err)
	}
	if decoded.GetOperatorId() != "beeline_uz" || decoded.GetPduType() != "SUBMIT_SM" || decoded.GetSequenceNumber() != 7 {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
}

func TestDecodePduLogInvalidPayload(t *testing.T) {
	if _, err := DecodePduLog([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatalf("ожидали ошибку на невалидном protobuf")
	}
}
