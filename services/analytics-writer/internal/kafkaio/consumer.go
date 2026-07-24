package kafkaio

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func DecodeIncomingMessage(payload []byte) (*eventsv1.IncomingMessage, error) {
	var msg eventsv1.IncomingMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal IncomingMessage: %w", err)
	}
	return &msg, nil
}

func DecodeStageCompleted(payload []byte) (*commonv1.StageCompletedEvent, error) {
	var event commonv1.StageCompletedEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal StageCompletedEvent: %w", err)
	}
	return &event, nil
}

func DecodeLifecycleEvent(payload []byte) (*eventsv1.MessageLifecycleEvent, error) {
	var event eventsv1.MessageLifecycleEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal MessageLifecycleEvent: %w", err)
	}
	return &event, nil
}

// StageCompletedTopics — все stage.completed-эквиваленты: на самом деле
// один топик stage.completed (общий для всех стадий, StageName внутри
// события), не по одному на стадию.
const StageCompletedTopic = "stage.completed"
