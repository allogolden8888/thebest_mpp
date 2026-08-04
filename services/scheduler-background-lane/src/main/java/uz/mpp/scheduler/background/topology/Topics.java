package uz.mpp.scheduler.background.topology;

public final class Topics {
    private Topics() {
    }

    public static final String BACKGROUND_COMMANDS = "scheduler.background.commands";
    public static final String OPERATOR_DLR_UNRESOLVED = "operator.dlr.unresolved";
    public static final String NOTIFICATION_RETRY = "notification.retry";

    public static String taskTypeFromEnumName(String protoEnumName) {
        // "BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY" -> "DLR_CORRELATION_RETRY"
        return protoEnumName.replace("BACKGROUND_TASK_TYPE_", "");
    }
}
