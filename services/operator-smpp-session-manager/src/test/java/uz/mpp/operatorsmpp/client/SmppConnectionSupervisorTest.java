package uz.mpp.operatorsmpp.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.operatorsmpp.codec.CommandId;
import uz.mpp.operatorsmpp.codec.Pdu;
import uz.mpp.operatorsmpp.codec.ShortMessagePdu;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CODE_REVIEW.md CRITICAL #1 — раньше не было ни одного теста, убивающего реальное TCP
 * соединение фейкового SMSC во время работы (README прямо признавал: "нужен реальный
 * обрыв TCP-соединения, не покрыто юнит-тестом"). Этот тест держит реальный
 * {@link FakeSmscServer}, реальный {@link OperatorSmppClient} поверх него, рвёт
 * TCP-соединение НЕ останавливая сервер (симулируя сетевой блип/рестарт SMSC — а не
 * graceful close с нашей стороны), и проверяет, что {@link SmppConnectionSupervisor}
 * (а) сразу роняет readiness, (б) реально переподключается и повторно биндится в
 * пределах разумного времени, (в) поднимает readiness обратно.
 */
class SmppConnectionSupervisorTest {

    private FakeSmscServer smsc;
    private OperatorSmppClient client;
    private SmppConnectionSupervisor supervisor;

    @AfterEach
    void tearDown() {
        if (supervisor != null) supervisor.stop();
        if (client != null) client.close();
        if (smsc != null) smsc.stop();
    }

    @Test
    void reconnectsAndRebindsAfterServerSideDisconnectAndFlipsReadiness() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();

        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);
        assertTrue(client.isActive(), "клиент должен быть подключён после начального bind");

        List<Boolean> readyTransitions = new CopyOnWriteArrayList<>();
        CountDownLatch wentDown = new CountDownLatch(1);
        CountDownLatch cameBackUp = new CountDownLatch(1);

        supervisor = new SmppConnectionSupervisor(
            client,
            () -> {
                // реальный connect+bind заново — тот же путь, что и Main.connectAndBindWithRetry,
                // но без retry-петли, чтобы тест не маскировал единичный сбой повторными попытками.
                client.connect("127.0.0.1", port);
                try {
                    client.bind("mpp_esme", "s3cr3t", "", 2000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            ready -> {
                readyTransitions.add(ready);
                if (ready) {
                    cameBackUp.countDown();
                } else {
                    wentDown.countDown();
                }
            });
        supervisor.arm();

        // Обрываем TCP-соединение со стороны "оператора" — сервер продолжает слушать.
        smsc.disconnectClient();

        assertTrue(wentDown.await(2, TimeUnit.SECONDS),
            "readiness должен упасть сразу после обрыва TCP-соединения");
        assertTrue(cameBackUp.await(5, TimeUnit.SECONDS),
            "клиент должен реально переподключиться и повторно забиндиться в пределах 5с");

        assertTrue(client.isActive(), "после reconnect канал должен снова быть активным");
        assertEquals(List.of(false, true), readyTransitions,
            "readiness должен пройти ровно через 'down' затем 'up', без лишних переключений");

        // Убеждаемся, что восстановленное соединение реально рабочее, не только isActive():
        // полноценный submit_sm round-trip против того же FakeSmscServer.
        ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "mpp_esme",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello after reconnect".getBytes());
        Pdu submitResp = client.submitSm(submitBody, 2000);
        assertEquals(CommandId.SUBMIT_SM_RESP, submitResp.header().commandId());
    }
}
