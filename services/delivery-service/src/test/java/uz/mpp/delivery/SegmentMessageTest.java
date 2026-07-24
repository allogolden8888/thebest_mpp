package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import uz.mpp.delivery.SegmentMessage.Segment;

class SegmentMessageTest {

    @Test
    void asciiUnder160IsOneGsm7Segment() {
        List<Segment> segments = SegmentMessage.segment("Your OTP code is 123456.", "GSM7");
        assertEquals(1, segments.size());
        assertEquals("GSM7", segments.get(0).encoding());
        assertEquals("Your OTP code is 123456.", new String(segments.get(0).content(), StandardCharsets.UTF_8));
    }

    @Test
    void ascii161CharsSplitsIntoTwoConcatenatedGsm7Segments() {
        String body = "a".repeat(161);
        List<Segment> segments = SegmentMessage.segment(body, "GSM7");
        assertEquals(2, segments.size());
        assertEquals(153, segments.get(0).content().length);
        assertEquals(8, segments.get(1).content().length);
    }

    @Test
    void ascii306CharsNeedsExactlyTwoSegments() {
        String body = "a".repeat(306);
        List<Segment> segments = SegmentMessage.segment(body, "GSM7");
        assertEquals(2, segments.size());
    }

    @Test
    void ascii307CharsNeedsThreeSegments() {
        String body = "a".repeat(307);
        List<Segment> segments = SegmentMessage.segment(body, "GSM7");
        assertEquals(3, segments.size());
    }

    @Test
    void extendedTableCharCostsTwoSeptets() {
        // 159 обычных + "€" (2 септета) = 161 септет > 160.
        String body = "a".repeat(159) + "€";
        List<Segment> segments = SegmentMessage.segment(body, "GSM7");
        assertEquals(2, segments.size());
    }

    @Test
    void cyrillicBodyProducesUtf16BeBytesPerUcs2Segment() {
        String body = "Спасибо";
        List<Segment> segments = SegmentMessage.segment(body, "UCS2");
        assertEquals(1, segments.size());
        assertEquals("UCS2", segments.get(0).encoding());
        assertEquals(body, new String(segments.get(0).content(), StandardCharsets.UTF_16BE));
    }

    @Test
    void ucs2Under70IsOneSegment() {
        String body = "я".repeat(35);
        List<Segment> segments = SegmentMessage.segment(body, "UCS2");
        assertEquals(1, segments.size());
    }

    @Test
    void ucs2Exactly71UnitsSplitsIntoTwoConcatenatedSegments() {
        String body = "я".repeat(71);
        List<Segment> segments = SegmentMessage.segment(body, "UCS2");
        assertEquals(2, segments.size());
        assertEquals(67 * 2, segments.get(0).content().length, "67 UTF-16BE code unit = 134 байта");
        assertEquals(8, segments.get(1).content().length, "4 оставшихся code unit = 8 байт");
    }

    @Test
    void mismatchedEncodingHintFallsBackToUcs2NotCorruption() {
        // msgctx говорит GSM7, но тело реально содержит не-GSM7 символ —
        // fail-safe откат на UCS-2 для этого вызова, не искажённая отправка.
        List<Segment> segments = SegmentMessage.segment("Спасибо", "GSM7");
        assertEquals("UCS2", segments.get(0).encoding());
        assertTrue(segments.get(0).content().length > 0);
    }

    @Test
    void isGsm7CompatibleDetectsCyrillicAsIncompatible() {
        assertTrue(SegmentMessage.isGsm7Compatible("Hello world"));
        assertTrue(!SegmentMessage.isGsm7Compatible("Спасибо"));
    }

    @Test
    void emptyBodyProducesSingleEmptySegment() {
        List<Segment> segmentsGsm7 = SegmentMessage.segment("", "GSM7");
        assertEquals(1, segmentsGsm7.size());
        assertEquals(0, segmentsGsm7.get(0).content().length);

        List<Segment> segmentsUcs2 = SegmentMessage.segment("", "UCS2");
        assertEquals(1, segmentsUcs2.size());
        assertEquals(0, segmentsUcs2.get(0).content().length);
    }
}
