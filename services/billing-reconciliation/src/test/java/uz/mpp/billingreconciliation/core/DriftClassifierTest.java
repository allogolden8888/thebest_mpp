package uz.mpp.billingreconciliation.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DriftClassifierTest {

    @Test
    void zeroDriftIsNone() {
        DriftReport report = new DriftReport("acc-1", 100_000, 100_000);
        assertEquals(0, report.driftMinorUnits());
        assertEquals(DriftClassifier.Severity.NONE, DriftClassifier.classify(report));
    }

    @Test
    void smallDriftIsLow() {
        DriftReport report = new DriftReport("acc-1", 100_050, 100_000);
        assertEquals(DriftClassifier.Severity.LOW, DriftClassifier.classify(report));
    }

    @Test
    void mediumDriftClassifiesAsMedium() {
        DriftReport report = new DriftReport("acc-1", 105_000, 100_000);
        assertEquals(DriftClassifier.Severity.MEDIUM, DriftClassifier.classify(report));
    }

    @Test
    void largeDriftClassifiesAsHigh() {
        DriftReport report = new DriftReport("acc-1", 2_100_000, 100_000);
        assertEquals(DriftClassifier.Severity.HIGH, DriftClassifier.classify(report));
    }

    @Test
    void driftIsSignedButSeverityUsesAbsoluteValue() {
        DriftReport negative = new DriftReport("acc-1", 90_000, 100_000);
        assertEquals(-10_000, negative.driftMinorUnits());
        assertEquals(DriftClassifier.Severity.HIGH, DriftClassifier.classify(negative));
    }

    @Test
    void onlyHighSeverityTriggersFreeze() {
        assertTrue(DriftClassifier.shouldFreeze(DriftClassifier.Severity.HIGH));
        assertFalse(DriftClassifier.shouldFreeze(DriftClassifier.Severity.MEDIUM));
        assertFalse(DriftClassifier.shouldFreeze(DriftClassifier.Severity.LOW));
        assertFalse(DriftClassifier.shouldFreeze(DriftClassifier.Severity.NONE));
    }
}
