package uz.mpp.delivery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code segment_message} (service_internal_methods.md §1.8) — splits body
 * into actual PDU-ready segments, not just a count (that count was already
 * computed once by partner-rest-receiver's {@code segmentation.rs} and cached
 * in {@code msgctx.segment_count} — this is a second, independent computation
 * because that service only needed a number, this one needs real byte
 * content per segment). Same GSM 03.38 / 3GPP TS 23.038 rules (GSM-7: 160
 * septets single / 153 per concatenated segment; UCS-2: 70 code units single
 * / 67 per concatenated segment), ported from
 * {@code partner-rest-receiver/src/segmentation.rs} to Java.
 *
 * <p>Known, documented scope reduction: for GSM-7, {@link Segment#content}
 * carries UTF-8 text bytes, not 7-bit-packed septets — real SMPP {@code
 * submit_sm} with {@code data_coding=0} requires packed septets in the PDU,
 * which is a protocol-layer concern of Operator SMPP Session Manager (not
 * implemented in this repository, owned by Sub-agent 1), not this service.
 * For UCS-2, {@link Segment#content} is real, correct UTF-16BE bytes — the
 * standard wire format for SMPP {@code data_coding=8}, no further transform
 * needed downstream.
 */
public final class SegmentMessage {

    private SegmentMessage() {
    }

    public record Segment(int segmentId, byte[] content, String encoding) {
    }

    private static final String GSM7_BASIC =
        "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ"
            + "ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡"
            + "ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿"
            + "abcdefghijklmnopqrstuvwxyzäöñüà";
    private static final String GSM7_EXTENDED = "|^€{}[]~\\";

    private static int septetsFor(String body) {
        int total = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (GSM7_BASIC.indexOf(c) >= 0) {
                total += 1;
            } else if (GSM7_EXTENDED.indexOf(c) >= 0) {
                total += 2;
            } else {
                return -1; // не GSM-7 — вызывающая сторона должна была передать encoding=UCS2
            }
        }
        return total;
    }

    public static boolean isGsm7Compatible(String body) {
        return septetsFor(body) >= 0;
    }

    /**
     * {@code encoding} — то, что уже решил partner-rest-receiver и сохранил в
     * {@code msgctx} ("GSM7"/"UCS2"), не пересчитывается заново — сервис
     * доверяет upstream-решению, только режет на сегменты в соответствии с ним.
     */
    public static List<Segment> segment(String body, String encoding) {
        if ("UCS2".equals(encoding)) {
            return segmentUcs2(body);
        }
        return segmentGsm7(body);
    }

    private static List<Segment> segmentGsm7(String body) {
        int totalSeptets = septetsFor(body);
        if (totalSeptets < 0) {
            // Найдено на границе сервисов: msgctx.encoding говорит GSM7, но
            // реальное тело содержит символ вне GSM-7 alphabet (рассинхрон
            // между тем, что решил partner-rest-receiver, и тем, что реально
            // хранится) — fail-safe откат на UCS-2 для ЭТОГО вызова, не паника
            // и не искажённая отправка.
            return segmentUcs2(body);
        }
        int limit = totalSeptets <= 160 ? 160 : 153;
        List<Segment> segments = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int currentSeptets = 0;
        int segmentId = 1;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            int weight = GSM7_EXTENDED.indexOf(c) >= 0 ? 2 : 1;
            if (currentSeptets + weight > limit) {
                segments.add(new Segment(segmentId++, current.toString().getBytes(StandardCharsets.UTF_8), "GSM7"));
                current = new StringBuilder();
                currentSeptets = 0;
            }
            current.append(c);
            currentSeptets += weight;
        }
        segments.add(new Segment(segmentId, current.toString().getBytes(StandardCharsets.UTF_8), "GSM7"));
        return segments;
    }

    private static List<Segment> segmentUcs2(String body) {
        // body.length() в Java — число UTF-16 code unit'ов (суррогатная пара
        // считается как 2), ровно то, что нужно для UCS-2 сегментации —
        // не нужен отдельный подсчёт, в отличие от Rust-версии (там
        // `chars().count()` даёт code point'ы, там понадобился `encode_utf16`).
        int totalUnits = body.length();
        int limit = totalUnits <= 70 ? 70 : 67;
        List<Segment> segments = new ArrayList<>();
        int segmentId = 1;
        for (int start = 0; start < body.length(); start += limit) {
            int end = Math.min(start + limit, body.length());
            String chunk = body.substring(start, end);
            segments.add(new Segment(segmentId++, chunk.getBytes(StandardCharsets.UTF_16BE), "UCS2"));
        }
        if (segments.isEmpty()) {
            segments.add(new Segment(1, new byte[0], "UCS2"));
        }
        return segments;
    }
}
