package kafkaio

import (
	"fmt"

	commonv1 "mpp/platformcontracts/common/v1"
)

// StageTopic — маппинг StageName -> имя Kafka-топика (service_io_contracts.md
// §"топики" — stage.destination-resolution, stage.policy, stage.billing,
// stage.routing, stage.delivery, stage.delivery-reconciliation).
func StageTopic(stageName commonv1.StageName) (string, error) {
	switch stageName {
	case commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION:
		return "stage.destination-resolution", nil
	case commonv1.StageName_STAGE_NAME_POLICY:
		return "stage.policy", nil
	case commonv1.StageName_STAGE_NAME_BILLING:
		return "stage.billing", nil
	case commonv1.StageName_STAGE_NAME_ROUTING:
		return "stage.routing", nil
	case commonv1.StageName_STAGE_NAME_DELIVERY:
		return "stage.delivery", nil
	case commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION:
		return "stage.delivery-reconciliation", nil
	default:
		return "", fmt.Errorf("нет топика для StageName %v", stageName)
	}
}

// DlqTopic — соответствующий {topic}.dlq.
func DlqTopic(stageName commonv1.StageName) (string, error) {
	topic, err := StageTopic(stageName)
	if err != nil {
		return "", err
	}
	return topic + ".dlq", nil
}

// StageNameFromString переводит ExecutionState.StageName (TEXT из Runtime
// Redis exec:{message_id}.current_state) обратно в commonv1.StageName.
func StageNameFromString(s string) commonv1.StageName {
	switch s {
	case "DESTINATION_RESOLUTION":
		return commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION
	case "POLICY":
		return commonv1.StageName_STAGE_NAME_POLICY
	case "BILLING":
		return commonv1.StageName_STAGE_NAME_BILLING
	case "ROUTING":
		return commonv1.StageName_STAGE_NAME_ROUTING
	case "DELIVERY":
		return commonv1.StageName_STAGE_NAME_DELIVERY
	case "DELIVERY_RECONCILIATION":
		return commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION
	default:
		return commonv1.StageName_STAGE_NAME_UNSPECIFIED
	}
}

const StageCompletedTopic = "stage.completed"
