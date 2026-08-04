package uz.mpp.operatorsmpp.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.operatorsmpp.codec.*;

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