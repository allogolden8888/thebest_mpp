package uz.mpp.scheduler.standard.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlSnapshotTest {

    @Test
    void defaultsToActiveWithNoRecords() {
        ControlSnapshot snapshot = new ControlSnapshot();
        assertFalse(snapshot.isPaused("BILLING"));
        assertTrue(snapshot.isActive("BILLING"));
        assertEquals(1.0, snapshot.rampRate("BILLING"));
    }

    @Test
    void globalPausedBlocksEveryStage() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("GLOBAL", "", ControlSnapshot.State.PAUSED, 0.0, 0.0);
        assertTrue(snapshot.isPaused("BILLING"));
        assertTrue(snapshot.isPaused("ROUTING"));
    }

    @Test
    void stageScopedPauseOnlyBlocksThatStage() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("STAGE", "BILLING", ControlSnapshot.State.PAUSED, 0.0, 0.0);
        assertTrue(snapshot.isPaused("BILLING"));
        assertFalse(snapshot.isPaused("ROUTING"));
    }

    @Test
    void rampRateTakesMinOfGlobalAndStage() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("GLOBAL", "", ControlSnapshot.State.DEGRADED, 0.5, 0.5);
        snapshot.apply("STAGE", "BILLING", ControlSnapshot.State.DEGRADED, 0.25, 0.25);
        assertEquals(0.25, snapshot.rampRate("BILLING"));
    }

    @Test
    void laterApplyOverwritesEarlierForSameKey() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("STAGE", "BILLING", ControlSnapshot.State.PAUSED, 0.0, 0.0);
        snapshot.apply("STAGE", "BILLING", ControlSnapshot.State.ACTIVE, 1.0, 1.0);
        assertFalse(snapshot.isPaused("BILLING"));
    }

    // Регрессии на CODE_REVIEW.md Critical #2 — раньше isPaused(stageName)/
    // rampRate(stageName) (GLOBAL/STAGE only) были единственным overload'ом,
    // PARTNER/PARTNER_STAGE/OPERATOR_ROUTE записи в byKey никогда не читались
    // обратно на release-стороне, несмотря на то, что store-update сторона уже
    // умела все 5 scope из hld.md §8.1.

    @Test
    void partnerScopedPauseBlocksOnlyThatPartnersOwnScopeCheck() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("PARTNER", "partner-42", ControlSnapshot.State.PAUSED, 0.0, 0.0);

        assertTrue(snapshot.isPaused("BILLING", "PARTNER", "partner-42"),
            "held item с собственным scope=PARTNER/scopeId=partner-42 должен видеть свою же паузу");
        assertFalse(snapshot.isPaused("BILLING", "PARTNER", "partner-99"),
            "другой partner_id не должен быть затронут чужой PARTNER-паузой");
        assertFalse(snapshot.isPaused("BILLING"),
            "GLOBAL/STAGE-only overload (без scope) не должен видеть PARTNER-специфичную паузу");
    }

    @Test
    void operatorRouteScopedPauseBlocksOnlyThatRoute() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("OPERATOR_ROUTE", "route-beeline-1", ControlSnapshot.State.PAUSED, 0.0, 0.0);

        assertTrue(snapshot.isPaused("DELIVERY", "OPERATOR_ROUTE", "route-beeline-1"));
        assertFalse(snapshot.isPaused("DELIVERY", "OPERATOR_ROUTE", "route-ucell-1"));
    }

    @Test
    void partnerStagePauseDerivedFromPartnerAndStageBlocksRelease() {
        // Конвенция scope_id для PARTNER_STAGE — "{partner_id}:{stage}" (та же,
        // что billing-reconciliation's ExecutionControlClient) — held item со
        // scope=PARTNER должен дополнительно проверяться против производного
        // PARTNER_STAGE-ключа, даже если сам item не PARTNER_STAGE-scoped.
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("PARTNER_STAGE", "partner-42:BILLING", ControlSnapshot.State.PAUSED, 0.0, 0.0);

        assertTrue(snapshot.isPaused("BILLING", "PARTNER", "partner-42"),
            "PARTNER item на стадии BILLING должен видеть производную PARTNER_STAGE-паузу partner-42:BILLING");
        assertFalse(snapshot.isPaused("ROUTING", "PARTNER", "partner-42"),
            "тот же partner на другой стадии не затронут — PARTNER_STAGE специфичен к стадии");
    }

    @Test
    void rampRateForScopedItemTakesMinAcrossGlobalStageAndOwnScope() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("GLOBAL", "", ControlSnapshot.State.DEGRADED, 0.5, 0.5);
        snapshot.apply("PARTNER", "partner-42", ControlSnapshot.State.DEGRADED, 0.1, 0.1);

        assertEquals(0.1, snapshot.rampRate("BILLING", "PARTNER", "partner-42"),
            "min по всем применимым scope (hld.md §8.1) — самый строгий (PARTNER=0.1) должен победить GLOBAL=0.5");
        assertEquals(0.5, snapshot.rampRate("BILLING", "PARTNER", "partner-99"),
            "не затронутый PARTNER-паузой partner видит только GLOBAL=0.5");
    }

    @Test
    void isActiveOverloadWithScopeMirrorsIsPaused() {
        ControlSnapshot snapshot = new ControlSnapshot();
        snapshot.apply("PARTNER", "partner-42", ControlSnapshot.State.PAUSED, 0.0, 0.0);
        assertFalse(snapshot.isActive("BILLING", "PARTNER", "partner-42"));
        assertTrue(snapshot.isActive("BILLING", "PARTNER", "partner-99"));
    }
}
