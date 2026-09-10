package kafkaio

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// PduLogTopic — operator.pdu.log (infra/kafka/generate_kafka_topics.py) —
// per-PDU диагностический след, публикует operator-smpp-session-manager
// (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40).
const PduLogTopic = "operator.pdu.log"

func DecodePduLog(payload []byte) (*eventsv1.OperatorPduLog, error) {
	var event eventsv1.OperatorPduLog
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal OperatorPduLog: %w", err)
	}
	return &event, nil
}
