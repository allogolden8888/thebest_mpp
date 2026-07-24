package uz.mpp.operatorsmpp.session;

import java.util.concurrent.atomic.AtomicInteger;

/** SMPP 3.4 §3.2: sequence_number — 1..0x7FFFFFFF, per-session monotonic (wraps). */
public final class SequenceNumberGenerator {
    private static final int MAX = 0x7FFFFFFF;
    private final AtomicInteger current = new AtomicInteger(0);

    public int next() {
        return current.updateAndGet(v -> v >= MAX ? 1 : v + 1);
    }
}
