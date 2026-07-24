package uz.mpp.partnersmpp.core;

import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.codec.ShortMessagePdu;
import uz.mpp.platformcontracts.common.v1.Channel;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncomingMessageBuilderTest {

    @Test
    void buildsIncomingMessageFromSubmitSm() {
        ShortMessagePdu pdu = new ShortMessagePdu("", (byte) 0, (byte) 1, "click_uz_main",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 1, (byte) 0, (byte) 0, (byte) 0, "hello world".getBytes());

        Instant ttl = Instant.now().plus(1, ChronoUnit.DAYS);
        IncomingMessage msg = IncomingMessageBuilder.build(pdu, "click_uz", "click_uz_main", ttl);

        assertTrue(msg.getMessageId() != null && !msg.getMessageId().isEmpty());
        assertTrue(msg.getTraceId() != null && !msg.getTraceId().isEmpty());
        assertEquals(Channel.CHANNEL_SMS, msg.getChannel());
        assertEquals("click_uz", msg.getPartnerId());
        assertEquals("click_uz_main", msg.getApplicationId());
        assertEquals("998901234567", msg.getSms().getMsisdn());
        assertEquals("click_uz_main", msg.getSms().getSender());
        assertEquals("hello world", msg.getSms().getBody());
        assertEquals(0, msg.getSms().getSegmentCount(), "segment_count не должен заполняться здесь — считает Pipeline Engine");
    }

    @Test
    void messageIdsAreUniquePerCall() {
        ShortMessagePdu pdu = new ShortMessagePdu("", (byte) 0, (byte) 1, "click_uz",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0, "x".getBytes());
        Instant ttl = Instant.now().plus(1, ChronoUnit.DAYS);

        IncomingMessage a = IncomingMessageBuilder.build(pdu, "click_uz", "app", ttl);
        IncomingMessage b = IncomingMessageBuilder.build(pdu, "click_uz", "app", ttl);
        assertNotEquals(a.getMessageId(), b.getMessageId());
    }

    @Test
    void mapsDataCodingToEncoding() {
        ShortMessagePdu ucs2 = new ShortMessagePdu("", (byte) 0, (byte) 1, "click_uz",
            (byte) 0, (byte) 1, "998901234567", (byte) 0, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 8, (byte) 0, "x".getBytes());
        IncomingMessage msg = IncomingMessageBuilder.build(ucs2, "click_uz", "app", Instant.now());
        assertEquals("UCS2", msg.getSms().getEncoding());
        assertFalse(msg.getSms().getEncoding().equals("GSM7"));
    }
}