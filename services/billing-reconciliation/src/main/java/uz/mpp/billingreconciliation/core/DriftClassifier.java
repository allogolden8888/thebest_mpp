package uz.mpp.billingreconciliation.core;

/**
 * classify_drift (service_internal_methods.md §5.3): DriftReport -&gt;
 * Severity. Пороги в минорных единицах (тийин) — **не задокументированы ни
 * в одном источнике этой сессии**, рабочее предположение, см. README
 * "Открытый вопрос".
 */
public final class DriftClassifier {

    private DriftClassifier() {
    }

    public enum Severity { NONE, LOW, MEDIUM, HIGH }

    private static final long LOW_THRESHOLD = 100; // 1 UZS
    private static final long MEDIUM_THRESHOLD = 10_000; // 100 UZS

    public static Severity classify(DriftReport report) {
        long abs = report.absDriftMinorUnits();
        if (abs == 0) {
            return Severity.NONE;
        }
        if (abs < LOW_THRESHOLD) {
            return Severity.LOW;
        }
        if (abs < MEDIUM_THRESHOLD) {
            return Severity.MEDIUM;
        }
        return Severity.HIGH;
    }

    /** trigger_freeze — фризить только при Severity выше порога. */
    public static boolean shouldFreeze(Severity severity) {
        return severity == Severity.HIGH;
    }
}
