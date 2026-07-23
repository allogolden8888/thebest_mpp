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
}
