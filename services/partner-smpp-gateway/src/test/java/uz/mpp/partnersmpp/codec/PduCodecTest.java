package uz.mpp.partnersmpp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PduCodecTest {

    @Test
    void bindTransceiverRoundTrips() {
        BindTransceiver body = new BindTransceiver("click_uz_main", "s3cr3t", "", (byte) 0x34, (byte) 0, (byte) 0, "");
        Pdu original = Pdu.withBody(CommandId.BIND_TRANSCEIVER, CommandStatus.ESME_ROK, 1, body);

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);

        assertEquals(CommandId.BIND_TRANSCEIVER, decoded.header().commandId());
        assertEquals(1, decoded.header().sequenceNumber());
        BindTransceiver decodedBody = (BindTransceiver) decoded.body();
        assertEquals("click_uz_main", decodedBody.systemId());
        assertEquals("s3cr3t", decodedBody.password());
        assertEquals((byte) 0x34, decodedBody.interfaceVersion());
    }

    @Test
    void bindTransceiverRespRoundTrips() {
        Pdu original = Pdu.withBody(CommandId.BIND_TRANSCEIVER_RESP, CommandStatus.ESME_ROK, 1, new BindTransceiverResp("click_uz_main"));

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);

        assertEquals(CommandId.BIND_TRANSCEIVER_RESP, decoded.header().commandId());
        assertEquals("click_uz_main", ((BindTransceiverResp) decoded.body()).systemId());
    }

    @Test
    void submitSmRoundTripsWithBinaryShortMessage() {
        byte[] message = "Hello, 998901234567!".getBytes(StandardCharsets.US_ASCII);
        ShortMessagePdu body = new ShortMessagePdu(
            "", (byte) 0, (byte) 1, "click_uz",
            (byte) 0, (byte) 1, "998901234567",
            (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0,
            message
        );
        Pdu original = Pdu.withBody(CommandId.SUBMIT_SM, CommandStatus.ESME_ROK, 42, body);

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);

        assertEquals(42, decoded.header().sequenceNumber());
        ShortMessagePdu decodedBody = (ShortMessagePdu) decoded.body();
        assertEquals("998901234567", decodedBody.destinationAddr());
        assertEquals("click_uz", decodedBody.sourceAddr());
        assertArrayEquals(message, decodedBody.shortMessage());
        assertEquals((byte) 1, decodedBody.registeredDelivery());
    }

    @Test
    void submitSmRespRoundTrips() {
        Pdu original = Pdu.withBody(CommandId.SUBMIT_SM_RESP, CommandStatus.ESME_ROK, 42, new ShortMessagePduResp("smsc-msg-1"));

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);

        assertEquals("smsc-msg-1", ((ShortMessagePduResp) decoded.body()).messageId());
    }

    @Test
    void deliverSmRoundTripsSameShapeAsSubmitSm() {
        byte[] dlrText = "id:1 sub:001 dlvrd:001 stat:DELIVRD".getBytes(StandardCharsets.US_ASCII);
        ShortMessagePdu body = new ShortMessagePdu(
            "", (byte) 1, (byte) 1, "998901234567",
            (byte) 0, (byte) 1, "click_uz",
            (byte) 0x04 /* SMSC delivery receipt */, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0,
            dlrText
        );
        Pdu original = Pdu.withBody(CommandId.DELIVER_SM, CommandStatus.ESME_ROK, 7, body);

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);

        ShortMessagePdu decodedBody = (ShortMessagePdu) decoded.body();
        assertArrayEquals(dlrText, decodedBody.shortMessage());
        assertEquals((byte) 0x04, decodedBody.esmClass());
    }

    @Test
    void enquireLinkHasEmptyBodyAndCorrectFrameLength() {
        Pdu original = Pdu.headerOnly(CommandId.ENQUIRE_LINK, CommandStatus.ESME_ROK, 5);

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);

        assertEquals(16, buf.readableBytes(), "enquire_link — только 16-байтовый header, пустое тело");

        Pdu decoded = PduCodec.decode(buf);
        assertEquals(CommandId.ENQUIRE_LINK, decoded.header().commandId());
        assertNull(decoded.body());
    }

    @Test
    void unbindAndGenericNackRoundTrip() {
        for (int commandId : new int[]{CommandId.UNBIND, CommandId.UNBIND_RESP, CommandId.GENERIC_NACK, CommandId.ENQUIRE_LINK_RESP}) {
            Pdu original = Pdu.headerOnly(commandId, CommandStatus.ESME_ROK, 9);
            ByteBuf buf = Unpooled.buffer();
            PduCodec.encode(original, buf);
            Pdu decoded = PduCodec.decode(buf);
            assertEquals(commandId, decoded.header().commandId());
            assertNull(decoded.body());
        }
    }

    @Test
    void commandLengthFieldMatchesActualFrameSize() {
        Pdu original = Pdu.withBody(CommandId.BIND_TRANSCEIVER_RESP, CommandStatus.ESME_ROK, 1, new BindTransceiverResp("sys"));

        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);

        int commandLength = buf.getInt(0);
        assertEquals(buf.readableBytes(), commandLength, "command_length должен совпадать с реальным размером фрейма");
    }

    @Test
    void genericNackCarriesErrorStatus() {
        Pdu original = Pdu.headerOnly(CommandId.GENERIC_NACK, CommandStatus.ESME_RINVCMDID, 3);
        ByteBuf buf = Unpooled.buffer();
        PduCodec.encode(original, buf);
        Pdu decoded = PduCodec.decode(buf);
        assertEquals(CommandStatus.ESME_RINVCMDID, decoded.header().commandStatus());
    }
}