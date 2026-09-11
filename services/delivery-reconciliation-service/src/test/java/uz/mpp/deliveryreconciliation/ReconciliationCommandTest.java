package uz.mpp.deliveryreconciliation;

import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.DeliveryReconciliationExtension;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReconciliationCommandTest {

    @Test
    void operatorIdComesFromDedicatedFieldNotQueueMessageId() {
        StageExecuteCommand command = command("beeline", "dlv-queue-id-that-is-not-an-operator");

        assertEquals("beeline", Main.operatorIdFrom(command));
    }

    @Test
    void legacyCommandWithoutOperatorIdIsRejectedInsteadOfCorruptingCase() {
        StageExecuteCommand command = command("", "dlv-legacy-queue-id");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> Main.operatorIdFrom(command));
        assertEquals("DeliveryReconciliationExtension.resolved_operator_id обязателен", error.getMessage());
    }

    private static StageExecuteCommand command(String operatorId, String queueMessageId) {
        return StageExecuteCommand.newBuilder()
            .setDeliveryReconciliation(DeliveryReconciliationExtension.newBuilder()
                .setQueueMsgId(queueMessageId)
                .setResolvedOperatorId(operatorId))
            .build();
    }
}
