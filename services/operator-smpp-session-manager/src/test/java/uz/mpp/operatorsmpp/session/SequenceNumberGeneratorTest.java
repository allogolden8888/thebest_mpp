package uz.mpp.operatorsmpp.session;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SequenceNumberGeneratorTest {

    @Test
    void startsAtOneAndIncrements() {
        SequenceNumberGenerator gen = new SequenceNumberGenerator();
        assertEquals(1, gen.next());
        assertEquals(2, gen.next());
        assertEquals(3, gen.next());
    }

    @Test
    void wrapsAfterMax() throws Exception {
        SequenceNumberGenerator gen = new SequenceNumberGenerator();
        var field = SequenceNumberGenerator.class.getDeclaredField("current");
        field.setAccessible(true);
        var atomic = (java.util.concurrent.atomic.AtomicInteger) field.get(gen);
        atomic.set(0x7FFFFFFF);
        assertEquals(1, gen.next(), "должен обернуться на 1 после 0x7FFFFFFF, не переполниться в отрицательное");
    }
}