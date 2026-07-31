package uz.mpp.deliveryreconciliation.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * CODE_REVIEW.md #11 — EvidenceCodec — encode/decode должны round-trip'иться
 * без потерь и decode должен деградировать к дефолтам {@link Evidence#empty()}
 * на пустом/частичном/повреждённом JSON вместо исключения (persist_case и
 * sweepDeadlines не должны падать на строке, которую сами когда-то записали
 * в промежуточном формате, либо на "{}" из create()).
 */
class EvidenceCodecTest {

    @Test
    void encodeThenDecodeRoundTripsAllFields() {
        Evidence evidence = Evidence.empty()
            .withSubmitAccepted()
            .withDeliveryStatus(Evidence.DeliveryOutcome.SUCCESS)
            .withQuerySm(Evidence.QuerySmOutcome.CONFIRMED_DELIVERED);

        String json = EvidenceCodec.encode(evidence);
        Evidence decoded = EvidenceCodec.decode(json);

        assertEquals(evidence, decoded);
    }

    @Test
    void decodeOfDefaultDbValueYieldsEmptyEvidence() {
        assertEquals(Evidence.empty(), EvidenceCodec.decode("{}"));
    }

    @Test
    void decodeOfNullYieldsEmptyEvidence() {
        assertEquals(Evidence.empty(), EvidenceCodec.decode(null));
    }

    @Test
    void decodeIsTolerantOfPartialJson() {
        // ReconciliationStoreTest.persistEvidenceUpdatesEvidenceColumn пишет
        // именно такую усечённую запись напрямую — decode не должен на ней падать.
        Evidence decoded = EvidenceCodec.decode("{\"submit_accepted\":true}");

        assertEquals(Evidence.empty().withSubmitAccepted(), decoded);
    }

    @Test
    void decodeIsTolerantOfUnknownEnumValue() {
        Evidence decoded = EvidenceCodec.decode("{\"delivery_status\":\"SOME_FUTURE_VALUE\"}");

        assertFalse(decoded.submitAcceptedObserved());
        assertEquals(Evidence.DeliveryOutcome.NONE, decoded.deliveryStatusObserved());
    }
}
