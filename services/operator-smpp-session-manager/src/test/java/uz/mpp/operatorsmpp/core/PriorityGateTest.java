package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PriorityGateTest {

    @Test
    void permitsQueryWhenNoSubmitLoad() {
        PriorityGate gate = new PriorityGate(5);
        assertEquals(PriorityGate.QueryDecision.PERMIT, gate.checkQuerySm());
    }

    @Test
    void defersQueryWhenSubmitLoadAtThreshold() {
        PriorityGate gate = new PriorityGate(2);
        gate.submitStarted();
        gate.submitStarted();
        assertEquals(PriorityGate.QueryDecision.DEFER, gate.checkQuerySm());
    }

    @Test
    void permitsAgainAfterSubmitsFinish() {
        PriorityGate gate = new PriorityGate(1);
        gate.submitStarted();
        assertEquals(PriorityGate.QueryDecision.DEFER, gate.checkQuerySm());
        gate.submitFinished();
        assertEquals(PriorityGate.QueryDecision.PERMIT, gate.checkQuerySm());
    }

    @Test
    void submitFinishedNeverGoesNegative() {
        PriorityGate gate = new PriorityGate(1);
        gate.submitFinished();
        gate.submitFinished();
        assertEquals(PriorityGate.QueryDecision.PERMIT, gate.checkQuerySm());
    }
}