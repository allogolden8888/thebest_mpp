package uz.mpp.partnersmpp.admission;

import java.util.concurrent.atomic.AtomicLong;

/** Applies the local execution-control snapshot on the submit_sm hot path. */
public final class SnapshotAdmissionGate implements AdmissionGate {

    private static final long SAMPLE_BUCKETS = 1_000_000L;

    private final ExecutionControlSnapshot snapshot;
    private final AtomicLong sequence = new AtomicLong();

    public SnapshotAdmissionGate(ExecutionControlSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public boolean admit(String partnerId) {
        return admitWithSample(partnerId, sequence.getAndIncrement());
    }

    boolean admitWithSample(String partnerId, long sample) {
        var rate = snapshot.effectiveRate(partnerId);
        if (rate.isEmpty() || rate.orElseThrow() <= 0.0) {
            return false;
        }
        if (rate.orElseThrow() >= 1.0) {
            return true;
        }

        long mixed = mix64((((long) partnerId.hashCode()) << 32) ^ sample);
        long bucket = Long.remainderUnsigned(mixed, SAMPLE_BUCKETS);
        return bucket < (long) (rate.orElseThrow() * SAMPLE_BUCKETS);
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
