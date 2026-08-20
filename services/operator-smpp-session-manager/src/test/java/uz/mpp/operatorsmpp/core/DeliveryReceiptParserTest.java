package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeliveryReceiptParserTest {

    /** Ровно тот текст, что реально приходил от тестового SMSC в этой сессии. */
    private static final String REAL_RECEIPT =
        "id:AEED5D53 sub:001 dlvrd:001 submit date:260805230945 done date:260805230946 stat:DELIVRD err:0 Text:report";

    @Test
    void extractsIdFromRealReceipt() {
        assertEquals("AEED5D53", DeliveryReceiptParser.extractSmscMessageId(REAL_RECEIPT));
    }

    @Test
    void extractsStatusFromRealReceipt() {
        assertEquals("DELIVRD", DeliveryReceiptParser.extractStatus(REAL_RECEIPT));
    }

    @Test
    void missingIdReturnsEmptyNotNull() {
        assertEquals("", DeliveryReceiptParser.extractSmscMessageId("stat:EXPIRED err:1"));
    }

    @Test
    void nullAndEmptyAreSafe() {
        assertEquals("", DeliveryReceiptParser.extractSmscMessageId(null));
        assertEquals("", DeliveryReceiptParser.extractSmscMessageId(""));
    }

    /**
     * Префикс обязан стоять на границе слова — иначе "id:" совпал бы
     * внутри "msgid:" и вернул бы чужое значение как smsc_message_id
     * (тихая порча ключа корреляции, худший вид бага здесь).
     */
    @Test
    void doesNotMatchPrefixInsideAnotherToken() {
        assertEquals("REAL1", DeliveryReceiptParser.extractSmscMessageId("msgid:WRONG id:REAL1 stat:DELIVRD"));
    }

    /** Операторы шлют разный регистр — Appendix B не фиксирует его жёстко. */
    @Test
    void isCaseInsensitiveOnFieldNames() {
        assertEquals("ABC123", DeliveryReceiptParser.extractSmscMessageId("ID:ABC123 STAT:DELIVRD"));
        assertEquals("DELIVRD", DeliveryReceiptParser.extractStatus("ID:ABC123 STAT:DELIVRD"));
    }

    /**
     * Text: в конце может содержать что угодно, включая то, что выглядит
     * как поле — разбор не должен от этого зависеть.
     */
    @Test
    void trailingTextWithFieldLikeContentDoesNotConfuseParser() {
        String receipt = "id:XYZ789 stat:UNDELIV err:8 Text:failed id:NOTTHISONE";
        assertEquals("XYZ789", DeliveryReceiptParser.extractSmscMessageId(receipt));
        assertEquals("UNDELIV", DeliveryReceiptParser.extractStatus(receipt));
    }

    @Test
    void fieldAtVeryStartOfStringIsFound() {
        assertEquals("FIRST", DeliveryReceiptParser.extractSmscMessageId("id:FIRST sub:001"));
    }
}
