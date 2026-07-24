package uz.mpp.partnersmpp.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.codec.*;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Реальный TCP round-trip против {@link PartnerSmppServer} на localhost —
 * не мок, настоящий Netty-сервер, настоящий сокет-клиент, настоящая
 * сериализация через {@link PduCodec}. Не требует Docker/Kafka/Redis —
 * SMPP-протокольный слой самодостаточен для этого теста.
 */
class PartnerSmppServerIntegrationTest {

    private PartnerSmppServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private PartnerSmppServer startServer(double tps, CopyOnWriteArrayList<IncomingMessage> sink) throws InterruptedException {
        StaticAuthenticator auth = new StaticAuthenticator(Map.of(
            "click_uz_main", new StaticAuthenticator.Credential("s3cr3t", "click_uz", "click_uz_main")
        ));
        server = new PartnerSmppServer(auth, sink::add, tps);
        return server;
    }

    private static void writePdu(DataOutputStream out, Pdu pdu) throws IOException {
        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(pdu, buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        out.write(bytes);
        out.flush();
    }

    private static Pdu readPdu(DataInputStream in) throws IOException {
        int commandLength = in.readInt();
        byte[] rest = new byte[commandLength - 4];
        in.readFully(rest);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(commandLength);
        buf.writeBytes(rest);
        return PduCodec.decode(buf);
    }

    @Test
    void fullBindSubmitUnbindFlow() throws Exception {
        CopyOnWriteArrayList<IncomingMessage> sink = new CopyOnWriteArrayList<>();
        int port = startServer(100, sink).start(0);

        try (Socket socket = new Socket("127.0.0.1", port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // bind_transceiver
            writePdu(out, Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, 1,
                new BindTransceiver("click_uz_main", "s3cr3t", "", (byte) 0x34, (byte) 0, (byte) 0, "")));
            Pdu bindResp = readPdu(in);
            assertEquals(CommandId.BIND_TRANSCEIVER_RESP, bindResp.header().commandId());
            assertEquals(CommandStatus.ESME_ROK, bindResp.header().commandStatus());
            assertEquals(1, bindResp.header().sequenceNumber(), "resp должен echo'ить sequence_number запроса");

            // submit_sm
            writePdu(out, Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, 2,
                new ShortMessagePdu("", (byte) 0, (byte) 1, "click_uz_main",
                    (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
                    (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello".getBytes())));
            Pdu submitResp = readPdu(in);
            assertEquals(CommandId.SUBMIT_SM_RESP, submitResp.header().commandId());
            assertEquals(CommandStatus.ESME_ROK, submitResp.header().commandStatus());
            assertEquals(2, submitResp.header().sequenceNumber());

            assertEquals(1, sink.size(), "IncomingMessage должен быть опубликован в sink ровно один раз");
            assertEquals("998901234567", sink.get(0).getSms().getMsisdn());
            assertEquals("hello", sink.get(0).getSms().getBody());
            assertEquals("click_uz", sink.get(0).getPartnerId());

            // enquire_link
            writePdu(out, Pdu.headerOnly(CommandId.ENQUIRE_LINK, CommandStatus.ESME_ROK, 3));
            Pdu enquireResp = readPdu(in);
            assertEquals(CommandId.ENQUIRE_LINK_RESP, enquireResp.header().commandId());

            // unbind
            writePdu(out, Pdu.headerOnly(CommandId.UNBIND, CommandStatus.ESME_ROK, 4));
            Pdu unbindResp = readPdu(in);
            assertEquals(CommandId.UNBIND_RESP, unbindResp.header().commandId());

            // сервер должен закрыть соединение после unbind
            assertEquals(-1, in.read(), "ожидали закрытие соединения после unbind_resp");
        }
    }

    @Test
    void bindWithWrongPasswordIsRejectedAndConnectionClosed() throws Exception {
        int port = startServer(100, new CopyOnWriteArrayList<>()).start(0);

        try (Socket socket = new Socket("127.0.0.1", port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            writePdu(out, Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, 1,
                new BindTransceiver("click_uz_main", "wrong-password", "", (byte) 0x34, (byte) 0, (byte) 0, "")));
            Pdu resp = readPdu(in);
            assertEquals(CommandStatus.ESME_RINVPASWD, resp.header().commandStatus());
            assertEquals(-1, in.read(), "соединение должно закрыться после неудачного bind");
        }
    }

    @Test
    void submitSmBeforeBindIsRejected() throws Exception {
        int port = startServer(100, new CopyOnWriteArrayList<>()).start(0);

        try (Socket socket = new Socket("127.0.0.1", port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            writePdu(out, Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, 1,
                new ShortMessagePdu("", (byte) 0, (byte) 1, "x", (byte) 0, (byte) 1, "998901234567",
                    (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, "x".getBytes())));
            Pdu resp = readPdu(in);
            assertEquals(CommandId.SUBMIT_SM_RESP, resp.header().commandId());
            assertEquals(CommandStatus.ESME_RINVBNDSTS, resp.header().commandStatus());
        }
    }

    @Test
    void rateLimitThrottlesExcessSubmits() throws Exception {
        CopyOnWriteArrayList<IncomingMessage> sink = new CopyOnWriteArrayList<>();
        int port = startServer(1, sink).start(0); // capacity=1, refill=1/sec — только 1 немедленный submit

        try (Socket socket = new Socket("127.0.0.1", port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            writePdu(out, Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, 1,
                new BindTransceiver("click_uz_main", "s3cr3t", "", (byte) 0x34, (byte) 0, (byte) 0, "")));
            readPdu(in);

            ShortMessagePdu submitBody = new ShortMessagePdu("", (byte) 0, (byte) 1, "x", (byte) 0, (byte) 1,
                "998901234567", (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, "x".getBytes());

            writePdu(out, Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, 2, submitBody));
            Pdu first = readPdu(in);
            assertEquals(CommandStatus.ESME_ROK, first.header().commandStatus());

            writePdu(out, Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, 3, submitBody));
            Pdu second = readPdu(in);
            assertEquals(CommandStatus.ESME_RTHROTTLED, second.header().commandStatus(),
                "второй submit_sm сразу после первого должен быть throttled при capacity=1");

            assertEquals(1, sink.size(), "throttled submit не должен попасть в incoming sink");
        }
    }
}