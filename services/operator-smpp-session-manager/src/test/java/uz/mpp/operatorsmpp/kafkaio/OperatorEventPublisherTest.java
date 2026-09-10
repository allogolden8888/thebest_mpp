package uz.mpp.operatorsmpp.kafkaio;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.events.v1.OperatorDlr;
import uz.mpp.platformcontracts.events.v1.OperatorPduLog;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;
import uz.mpp.platformcontracts.events.v1.PduDirection;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OperatorEventPublisherTest {

    private static MockProducer<String, byte[]> newMockProducer() {
        return new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    @Test
    void publishSubmitAcceptedKeyedByOperatorId() throws Exception {
        MockProducer<String, byte[]> mock = newMockProducer();
        OperatorEventPublisher publisher = new OperatorEventPublisher(mock);

        OperatorSubmitAccepted event = OperatorSubmitAccepted.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setProtocol(Protocol.PROTOCOL_SMPP)
            .build();
        publisher.publishSubmitAccepted(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("operator.submit.accepted", history.get(0).topic());
        assertEquals("beeline_uz", history.get(0).key());
        assertEquals(event, OperatorSubmitAccepted.parseFrom(history.get(0).value()));
    }

    @Test
    void publishDlrKeyedByOperatorId() throws Exception {
        MockProducer<String, byte[]> mock = newMockProducer();
        OperatorEventPublisher publisher = new OperatorEventPublisher(mock);

        OperatorDlr event = OperatorDlr.newBuilder()
            .setOperatorId("ucell_uz").setProtocol(Protocol.PROTOCOL_SMPP).setRawStatus("DELIVRD")
            .build();
        publisher.publishDlr(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("operator.dlr", history.get(0).topic());
        assertEquals("ucell_uz", history.get(0).key());
    }

    @Test
    void publishPduLogKeyedByOperatorId() throws Exception {
        MockProducer<String, byte[]> mock = newMockProducer();
        OperatorEventPublisher publisher = new OperatorEventPublisher(mock);

        OperatorPduLog event = OperatorPduLog.newBuilder()
            .setOperatorId("beeline_uz").setProtocol(Protocol.PROTOCOL_SMPP)
            .setDirection(PduDirection.PDU_DIRECTION_A2P).setPduType("SUBMIT_SM")
            .setSequenceNumber(42).setMessageId("msg-1")
            .build();
        publisher.publishPduLog(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("operator.pdu.log", history.get(0).topic());
        assertEquals("beeline_uz", history.get(0).key());
        assertEquals(event, OperatorPduLog.parseFrom(history.get(0).value()));
    }
}