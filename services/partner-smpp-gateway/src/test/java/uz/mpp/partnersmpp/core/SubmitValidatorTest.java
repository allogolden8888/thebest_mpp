package uz.mpp.partnersmpp.core;

import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.codec.ShortMessagePdu;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubmitValidatorTest {

    private static ShortMessagePdu pdu(String destAddr, byte[] shortMessage) {
        return new ShortMessagePdu("", (byte) 0, (byte) 1, "click_uz",
            (byte) 0, (byte) 1, destAddr, (byte) 0, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0, shortMessage);
    }

    @Test
    void acceptsValidSubmit() {
        var result = SubmitValidator.validate(pdu("998901234567", "hello".getBytes()));
        assertTrue(result.valid());
    }

    @Test
    void rejectsEmptyDestinationAddr() {
        var result = SubmitValidator.validate(pdu("", "hello".getBytes()));
        assertFalse(result.valid());
    }

    @Test
    void rejectsTooLongDestinationAddr() {
        var result = SubmitValidator.validate(pdu("9989012345671234567890", "hi".getBytes()));
        assertFalse(result.valid());
    }

    @Test
    void rejectsOversizedShortMessage() {
        byte[] tooLong = new byte[255];
        var result = SubmitValidator.validate(pdu("998901234567", tooLong));
        assertFalse(result.valid());
    }
}