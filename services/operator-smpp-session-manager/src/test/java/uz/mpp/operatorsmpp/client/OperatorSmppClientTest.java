package uz.mpp.operatorsmpp.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.operatorsmpp.codec.*;
import uz.mpp.operatorsmpp.core.PduLogEvent;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Реальный TCP round-trip: {@link OperatorSmppClient} против
 * {@link FakeSmscServer} на localhost — не мок, настоящий Netty-клиент,
 * настоящий Netty-сервер, настоящая сериализация PDU.
 */
class OperatorSmppClientTest {

    private FakeSmscServer smsc;
    private OperatorSmppClient client;

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (smsc != null) smsc.stop();
    }

    @Test
    void bindAndSubmitSmRoundTrip() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();

        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);

        Pdu bindResp = client.bind("mpp_esme", "s3cr3t", "", 2000);
        assertEquals(CommandId.BIND_TRANSCEIVER_RESP, bindResp.header().commandId());
        assertEquals(CommandStatus.ESME_ROK, bindResp.header().commandStatus());

        ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "mpp_esme",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello".getBytes());
        Pdu submitResp = client.submitSm(submitBody, 2000);

        assertEquals(CommandId.SUBMIT_SM_RESP, submitResp.header().commandId());
        assertEquals(CommandStatus.ESME_ROK, submitResp.header().commandStatus());
        assertEquals("smsc-msg-1", ((ShortMessagePduResp) submitResp.body()).messageId());
    }

    @Test
    void receivesDlrPushedByOperator() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();

        CopyOnWriteArrayList<ShortMessagePdu> receivedDlrs = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        client = new OperatorSmppClient(dlr -> {
            receivedDlrs.add(dlr);
            latch.countDown();
        });
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        byte[] dlrText = "id:1 sub:001 dlvrd:001 stat:DELIVRD".getBytes();
        ShortMessagePdu dlrBody = new ShortMessagePdu("", (byte) 1, (byte) 1, "998901234567",
            (byte) 0, (byte) 1, "mpp_esme", (byte) 0x04, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0, dlrText);
        smsc.pushDeliverSm(dlrBody, 1);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "ожидали получить DLR через dlrSink в течение 2с");
        assertEquals(1, receivedDlrs.size());
        assertArrayEquals(dlrText, receivedDlrs.get(0).shortMessage());
    }

    @Test
    void pendingResponseEntryIsClearedAfterTimeoutWithoutAnyChannelError() throws Exception {
        // CODE_REVIEW.md HIGH #2 — конгестия/тихая потеря ответа оператором (не разрыв
        // канала: exceptionCaught тут НЕ участвует) не должна оставлять запись в
        // pendingResponses навсегда. FakeSmscServer.setDropSubmitResponses имитирует
        // именно этот failure mode.
        smsc = new FakeSmscServer();
        int port = smsc.start();
        smsc.setDropSubmitResponses(true);

        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);
        assertEquals(0, client.pendingResponseCount(), "после успешного bind ничего не должно ждать ответа");

        ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "mpp_esme",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello".getBytes());

        long shortTimeoutMs = 300; // не ждём реальные 5с SUBMIT_TIMEOUT_MS gRPC-слоя
        assertThrows(TimeoutException.class, () -> client.submitSm(submitBody, shortTimeoutMs));

        assertEquals(0, client.pendingResponseCount(),
            "запись должна быть снята из pendingResponses сразу после таймаута — иначе утечка (finding #2)");
        // соединение при этом остаётся живым — таймаут не должен рвать канал (finding #4 отдельно).
        assertTrue(client.isActive());
    }

    @Test
    void submitSmEmitsPduLogEventsForRequestAndResponse() throws Exception {
        // BACKOFFICE_DESIGN_SPEC.md Экраны 38-40 — per-PDU лог: submitSm(body, timeout, messageId, stageExecutionId)
        // должен эмитить ровно 2 события (SUBMIT_SM затем SUBMIT_SM_RESP) с одинаковым sequence_number.
        smsc = new FakeSmscServer();
        int port = smsc.start();

        CopyOnWriteArrayList<PduLogEvent> pduLog = new CopyOnWriteArrayList<>();
        client = new OperatorSmppClient(null, pduLog::add);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "mpp_esme",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello".getBytes());
        client.submitSm(submitBody, 2000, "msg-42", "stage-exec-7");

        assertEquals(2, pduLog.size());
        PduLogEvent request = pduLog.get(0);
        assertEquals(PduLogEvent.DIRECTION_A2P, request.direction());
        assertEquals("SUBMIT_SM", request.pduType());
        assertEquals("msg-42", request.messageId());
        assertEquals("stage-exec-7", request.stageExecutionId());
        assertEquals("", request.status());

        PduLogEvent response = pduLog.get(1);
        assertEquals(PduLogEvent.DIRECTION_A2P, response.direction());
        assertEquals("SUBMIT_SM_RESP", response.pduType());
        assertEquals(request.sequenceNumber(), response.sequenceNumber(), "запрос и ответ должны нести один и тот же SMPP sequence_number");
        assertEquals("msg-42", response.messageId());
        assertEquals("OK", response.status());
        assertEquals("smsc-msg-1", response.smscMessageId());
    }

    @Test
    void submitSmTimeoutEmitsPduLogEventWithTimeoutStatus() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        smsc.setDropSubmitResponses(true);

        CopyOnWriteArrayList<PduLogEvent> pduLog = new CopyOnWriteArrayList<>();
        client = new OperatorSmppClient(null, pduLog::add);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "mpp_esme",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello".getBytes());
        assertThrows(TimeoutException.class, () -> client.submitSm(submitBody, 300, "msg-timeout", ""));

        assertEquals(2, pduLog.size());
        assertEquals("TIMEOUT", pduLog.get(1).status());
    }

    @Test
    void receivedDlrEmitsPduLogEventsForDeliverSmAndResp() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();

        CopyOnWriteArrayList<PduLogEvent> pduLog = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client = new OperatorSmppClient(dlr -> latch.countDown(), pduLog::add);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        byte[] dlrText = "id:dkr87lit9o7b sub:001 dlvrd:001 stat:DELIVRD".getBytes();
        ShortMessagePdu dlrBody = new ShortMessagePdu("", (byte) 1, (byte) 1, "998901234567",
            (byte) 0, (byte) 1, "mpp_esme", (byte) 0x04, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0, dlrText);
        smsc.pushDeliverSm(dlrBody, 1);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        // channelRead0 гонится с dlrSink на другом потоке (Netty event loop) — оба события
        // публикуются синхронно ДО dlrSink.accept, но polling защищает от редкой гонки в CI.
        long deadline = System.currentTimeMillis() + 2000;
        while (pduLog.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        assertEquals(2, pduLog.size());
        PduLogEvent deliver = pduLog.get(0);
        assertEquals(PduLogEvent.DIRECTION_DLR, deliver.direction());
        assertEquals("DELIVER_SM", deliver.pduType());
        assertEquals("dkr87lit9o7b", deliver.smscMessageId());
        assertEquals("DELIVRD", deliver.status());
        assertEquals("", deliver.messageId(), "message_id неизвестен на уровне DLR PDU — тот же барьер, что у OperatorDlr");

        PduLogEvent deliverResp = pduLog.get(1);
        assertEquals("DELIVER_SM_RESP", deliverResp.pduType());
        assertEquals(deliver.sequenceNumber(), deliverResp.sequenceNumber());
        assertEquals("OK", deliverResp.status());
    }

    @Test
    void bindWithMismatchedResponseSequenceStillTimesOutCorrectly() {
        // enquireLink не ждёт ответа синхронно — просто отправляется, не должно кидать исключение.
        assertDoesNotThrow(() -> {
            smsc = new FakeSmscServer();
            int port = smsc.start();
            client = new OperatorSmppClient(null);
            client.connect("127.0.0.1", port);
            client.bind("mpp_esme", "s3cr3t", "", 2000);
            client.sendEnquireLink();
            Thread.sleep(100);
            assertTrue(client.isActive());
        });
    }
}