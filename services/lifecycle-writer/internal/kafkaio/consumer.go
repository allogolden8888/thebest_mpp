// Package kafkaio — консьюмеры incoming.messages/message.lifecycle/stage.*.dlq.
package kafkaio

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func DecodeIncomingMessage(payload []byte) (*eventsv1.IncomingMessage, error) {
	var msg eventsv1.IncomingMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal IncomingMessage: %w", err)
	}
	return &msg, nil
}

func DecodeLifecycleEvent(payload []byte) (*eventsv1.MessageLifecycleEvent, error) {
	var event eventsv1.MessageLifecycleEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal MessageLifecycleEvent: %w", err)
	}
	return &event, nil
}

func DecodeDlqRecord(payload []byte) (*eventsv1.DlqRecord, error) {
	var rec eventsv1.DlqRecord
	if err := proto.Unmarshal(payload, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal DlqRecord: %w", err)
	}
	return &rec, nil
}

// DlqTopics — stage.*.dlq (data_infrastructure_spec.md §3), operator.dlr.dlq
// сознательно не входит: другая полезная нагрузка (не DlqRecord), не
// обрабатывается в этом срезе, см. README "Открытый вопрос".
var DlqTopics = []string{
	"stage.destination-resolution.dlq",
	"stage.policy.dlq",
	"stage.billing.dlq",
	"stage.routing.dlq",
	"stage.delivery.dlq",
	"stage.delivery-reconciliation.dlq",
}