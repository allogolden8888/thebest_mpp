package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;

/**
 * Фаза 11 плана закрытия API-пробелов: прямое доказательство, что
 * sandbox-сообщение не трогает ни {@link GatewayRegistry}, ни
 * {@link OperatorSubmitClient}, ни {@link SubmitIdempotencyStore}, ни
 * {@link MessageContextStore} — все четыре передаются как {@code null} в
 * {@link KafkaIo#processRecord}; если бы sandbox-ветка ошибочно дошла до
 * любого из них, вызов упал бы с {@link NullPointerException}, не с
 * connection-ошибкой (в отличие от billing-service, где то же доказательство
 * построено на недостижимом Redis URL — здесь GatewayRegistry подключается
 * ЖАДНО в конструкторе, так что тот же трюк с "заведомо недостижимым
 * адресом" пришлось бы исполнять уже в конструкторе, а не в processRecord).
 */
class KafkaIoProcessRecordTest {

    private static ConsumerRecord<String, byte[]> deliveryRecord(String stageExecutionId, boolean sandbox) {
        StageExecuteCommand command = StageExecuteCommand.newBuilder()
            .setEventId("evt-" + stageExecutionId)
            .setMessageId("m1")
            .setStageExecutionId(stageExecutionId)
            .setAttempt(1)
            .setStageName(StageName.STAGE_NAME_DELIVERY)
            .setSandbox(sandbox)
            .setDelivery(DeliveryExtension.newBuilder()
                .setRouteId("beeline_smpp_primary")
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setResolvedOperatorId("beeline")
                .build())
            .build();
        return new ConsumerRecord<>("stage.delivery", 0, 0, "m1", command.toByteArray());
    }

    @Test
    void sandboxMessageDoesNotTouchGatewayOrSubmitClientOrContextStore() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());

        KafkaIo.processRecord(
            deliveryRecord("se-sandbox-1", true),
            null, // MessageContextStore — не нужен, submit не строится
            null, // GatewayRegistry — оба операторских шлюза структурно недостижимы
            new ControlSnapshot(),
            null, // OperatorSubmitClient
            null, // SubmitIdempotencyStore
            producer);

        assertEquals(2, producer.history().size(), "sandbox обязан опубликовать и stage.completed, и синтетический delivery.status");

        ProducerRecord<String, byte[]> completedRecord = producer.history().get(0);
        assertEquals(KafkaIo.OUTPUT_TOPIC, completedRecord.topic());
        StageCompletedEvent event = StageCompletedEvent.parseFrom(completedRecord.value());
        assertEquals(Outcome.OUTCOME_SUCCEEDED, event.getOutcome(), "sandbox submit обязан выглядеть как обычный успех");
        assertTrue(event.getSandbox());
        assertTrue(event.getDelivery().getQueueMsgId().startsWith("dlv-se-sandbox-1"));

        ProducerRecord<String, byte[]> dlrRecord = producer.history().get(1);
        assertEquals(KafkaIo.DELIVERY_STATUS_TOPIC, dlrRecord.topic());
        DeliveryStatusEvent dlrEvent = DeliveryStatusEvent.parseFrom(dlrRecord.value());
        assertEquals("m1", dlrEvent.getMessageId());
        assertEquals("beeline", dlrEvent.getOperatorId());
        assertEquals("DELIVERED", dlrEvent.getNormalizedStatus());
        assertEquals("SANDBOX_SYNTHETIC", dlrEvent.getRawOperatorStatus());
    }

    @Test
    void nonSandboxMessageWithNullDependenciesThrowsInsteadOfSilentlySucceeding() {
        // Контрольный тест: доказывает, что null-зависимости в тесте выше
        // реально были бы использованы на не-sandbox пути (contextStore.fetch
        // первым) — если бы кто-то по ошибке снял sandbox-ветку, этот тест
        // немедленно упал бы с NPE вместо того, чтобы молча пройти.
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class, () -> KafkaIo.processRecord(
            deliveryRecord("se-real-1", false),
            null,
            null,
            new ControlSnapshot(),
            null,
            null,
            producer));
    }
}
