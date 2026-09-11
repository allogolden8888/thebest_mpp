package uz.mpp.deliveryreconciliation.kafkaio;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.events.v1.DlqRecord;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StageCompletedPublisherTest {

    @Test
    void publishSendsKeyedByMessageId() throws Exception {
        MockProducer<String, byte[]> mock = new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        StageCompletedPublisher publisher = new StageCompletedPublisher(mock);

        UUID messageId = UUID.randomUUID();
        StageCompletedEvent event = StageCompletedBuilder.build(messageId, UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED, Instant.now());

        publisher.publish(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("stage.completed", history.get(0).topic());
        assertEquals(messageId.toString(), history.get(0).key());
    }

    @Test
    void publishDlqPreservesOriginalCommandAndUsesMessageKey() throws Exception {
        MockProducer<String, byte[]> mock = new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        StageCompletedPublisher publisher = new StageCompletedPublisher(mock);
        StageExecuteCommand command = StageExecuteCommand.newBuilder()
            .setMessageId(UUID.randomUUID().toString())
            .setStageExecutionId(UUID.randomUUID().toString())
            .setStageName(StageName.STAGE_NAME_DELIVERY_RECONCILIATION)
            .setAttempt(2)
            .build();

        publisher.publishDlq(command, "RESOLVED_OPERATOR_ID_MISSING", "legacy command").get();

        ProducerRecord<String, byte[]> sent = mock.history().get(0);
        DlqRecord dlq = DlqRecord.parseFrom(sent.value());
        assertEquals("stage.delivery-reconciliation.dlq", sent.topic());
        assertEquals(command.getMessageId(), sent.key());
        assertEquals(command, dlq.getOriginalCommand());
        assertEquals("RESOLVED_OPERATOR_ID_MISSING", dlq.getReasonCode());
    }
}
