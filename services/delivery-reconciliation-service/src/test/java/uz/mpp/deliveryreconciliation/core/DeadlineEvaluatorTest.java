package uz.mpp.deliveryreconciliation.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeadlineEvaluatorTest {

    @Test
    void continuesBeforeDeadline() {
        Instant now = Instant.now();
        assertEquals(OutcomeResolver.DeadlineState.CONTINUE, DeadlineEvaluator.evaluate(now, now.plus(1, ChronoUnit.HOURS)));
    }

    @Test
    void expiresAfterDeadline() {
        Instant now = Instant.now();
        assertEquals(OutcomeResolver.DeadlineState.EXPIRED, DeadlineEvaluator.evaluate(now, now.minus(1, ChronoUnit.SECONDS)));
    }
}