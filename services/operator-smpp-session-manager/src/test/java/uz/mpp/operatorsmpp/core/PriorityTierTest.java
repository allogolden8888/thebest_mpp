package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PriorityTierTest {

    @Test
    void priorityFlag3IsHigh() {
        assertEquals(PriorityTier.HIGH, PriorityTier.forPriorityFlag(3));
    }

    @Test
    void priorityFlag1And2AreMedium() {
        assertEquals(PriorityTier.MEDIUM, PriorityTier.forPriorityFlag(1));
        assertEquals(PriorityTier.MEDIUM, PriorityTier.forPriorityFlag(2));
    }

    @Test
    void priorityFlag0IsLow() {
        assertEquals(PriorityTier.LOW, PriorityTier.forPriorityFlag(0));
    }

    @Test
    void outOfRangePriorityFlagFallsBackToLow() {
        assertEquals(PriorityTier.LOW, PriorityTier.forPriorityFlag(4));
        assertEquals(PriorityTier.LOW, PriorityTier.forPriorityFlag(-1));
        assertEquals(PriorityTier.LOW, PriorityTier.forPriorityFlag(999));
    }
}
