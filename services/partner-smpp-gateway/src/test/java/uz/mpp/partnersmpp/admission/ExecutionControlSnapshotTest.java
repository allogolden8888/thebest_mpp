package uz.mpp.partnersmpp.admission;

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ExecutionControlScope;
import uz.mpp.platformcontracts.common.v1.ExecutionControlState;
import uz.mpp.platformcontracts.events.v1.ExecutionControlRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionControlSnapshotTest {

    @Test
    void startupAndReplayWithoutGlobalSentinelStayFailClosed() {
        ExecutionControlSnapshot snapshot = new ExecutionControlSnapshot();
        SnapshotAdmissionGate gate = new SnapshotAdmissionGate(snapshot);

        assertFalse(snapshot.isReady());
        assertFalse(gate.admit("click_uz"));

        snapshot.install(new ExecutionControlSnapshot.Replay());
        assertFalse(snapshot.isReady());
        assertFalse(gate.admit("click_uz"));
    }

    @Test
    void fullReplayWithGlobalActiveOpensAdmission() {
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0, null)
        );

        assertTrue(snapshot.isReady());
        assertTrue(new SnapshotAdmissionGate(snapshot).admit("click_uz"));
    }

    @Test
    void globalPauseRejectsEveryPartner() {
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0, null)
        );
        SnapshotAdmissionGate gate = new SnapshotAdmissionGate(snapshot);

        assertFalse(gate.admit("click_uz"));
        assertFalse(gate.admit("other"));
    }

    @Test
    void partnerPauseIsScopedAndTombstoneRestoresGlobalState() {
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0, null),
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "click_uz",
                ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0, null)
        );
        SnapshotAdmissionGate gate = new SnapshotAdmissionGate(snapshot);

        assertFalse(gate.admit("click_uz"));
        assertTrue(gate.admit("other"));

        snapshot.applyLive(key(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "click_uz"), null);
        assertTrue(gate.admit("click_uz"));
    }

    @Test
    void expiredPartnerOverrideIsIgnoredButExpiredGlobalFailsClosed() {
        Timestamp expired = Timestamp.newBuilder().setSeconds(100).build();
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0, null),
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "click_uz",
                ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0, expired)
        );

        assertEquals(1.0, snapshot.effectiveRateAt("click_uz", Instant.ofEpochSecond(101)).orElseThrow());

        snapshot.applyLive(
            key(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, ""),
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0, expired).toByteArray()
        );
        assertTrue(snapshot.effectiveRateAt("click_uz", Instant.ofEpochSecond(101)).isEmpty());
        assertFalse(snapshot.isReady());
    }

    @Test
    void partialAdmissionRateIsEnforced() {
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_DEGRADED, 0.25, null)
        );
        SnapshotAdmissionGate gate = new SnapshotAdmissionGate(snapshot);

        long admitted = 0;
        for (long sample = 0; sample < 10_000; sample++) {
            if (gate.admitWithSample("click_uz", sample)) {
                admitted++;
            }
        }
        assertTrue(admitted >= 2_300 && admitted <= 2_700,
            "expected approximately 25%, got " + admitted + "/10000");
    }

    @Test
    void malformedOrMismatchedRecordsCannotReplaceSnapshot() {
        ExecutionControlSnapshot snapshot = snapshotWith(
            record(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
                ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0, null)
        );

        ExecutionControlRecord nanRate = record(
            ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, Double.NaN, null
        );
        assertThrows(IllegalArgumentException.class,
            () -> snapshot.applyLive(key(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, ""), nanRate.toByteArray()));

        ExecutionControlRecord partner = record(
            ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "click_uz",
            ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0, null
        );
        assertThrows(IllegalArgumentException.class,
            () -> snapshot.applyLive(key(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "other"), partner.toByteArray()));

        assertTrue(new SnapshotAdmissionGate(snapshot).admit("click_uz"));
    }

    private static ExecutionControlSnapshot snapshotWith(ExecutionControlRecord... records) {
        ExecutionControlSnapshot snapshot = new ExecutionControlSnapshot();
        ExecutionControlSnapshot.Replay replay = new ExecutionControlSnapshot.Replay();
        for (ExecutionControlRecord record : records) {
            ExecutionControlScope scope = record.getScope();
            replay.apply(key(scope, record.getScopeId()), record.toByteArray());
        }
        snapshot.install(replay);
        return snapshot;
    }

    private static ExecutionControlRecord record(
        ExecutionControlScope scope,
        String scopeId,
        ExecutionControlState state,
        double admissionRate,
        Timestamp expiresAt
    ) {
        ExecutionControlRecord.Builder builder = ExecutionControlRecord.newBuilder()
            .setScope(scope)
            .setScopeId(scopeId)
            .setState(state)
            .setAdmissionRate(admissionRate);
        if (expiresAt != null) {
            builder.setExpiresAt(expiresAt);
        }
        return builder.build();
    }

    private static byte[] key(ExecutionControlScope scope, String scopeId) {
        return (scope.name() + ":" + scopeId).getBytes(StandardCharsets.UTF_8);
    }
}
