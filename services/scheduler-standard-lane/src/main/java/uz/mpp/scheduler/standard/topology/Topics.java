package uz.mpp.scheduler.standard.topology;

import uz.mpp.platformcontracts.common.v1.StageName;

/**
 * Маппинг StageName -&gt; имя Kafka-топика (service_io_contracts.md), тот же
 * список, что в services/scheduler-critical-sweep/internal/kafkaio/topics.go.
 */
public final class Topics {

    private Topics() {
    }

    public static final String HOLD_COMMANDS = "scheduler.standard.commands";
    public static final String EXECUTION_CONTROL = "execution.control";
    public static final String RECONCILIATION_TOPIC = "stage.delivery-reconciliation";

    public static String stageTopic(StageName stageName) {
        return switch (stageName) {
            case STAGE_NAME_DESTINATION_RESOLUTION -> "stage.destination-resolution";
            case STAGE_NAME_POLICY -> "stage.policy";
            case STAGE_NAME_BILLING -> "stage.billing";
            case STAGE_NAME_ROUTING -> "stage.routing";
            case STAGE_NAME_DELIVERY -> "stage.delivery";
            case STAGE_NAME_DELIVERY_RECONCILIATION -> "stage.delivery-reconciliation";
            default -> throw new IllegalArgumentException("нет топика для StageName " + stageName);
        };
    }

    public static StageName stageNameFromString(String s) {
        return switch (s) {
            case "DESTINATION_RESOLUTION" -> StageName.STAGE_NAME_DESTINATION_RESOLUTION;
            case "POLICY" -> StageName.STAGE_NAME_POLICY;
            case "BILLING" -> StageName.STAGE_NAME_BILLING;
            case "ROUTING" -> StageName.STAGE_NAME_ROUTING;
            case "DELIVERY" -> StageName.STAGE_NAME_DELIVERY;
            case "DELIVERY_RECONCILIATION" -> StageName.STAGE_NAME_DELIVERY_RECONCILIATION;
            default -> StageName.STAGE_NAME_UNSPECIFIED;
        };
    }
}
