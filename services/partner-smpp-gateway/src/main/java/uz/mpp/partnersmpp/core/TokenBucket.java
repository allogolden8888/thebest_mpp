package uz.mpp.partnersmpp.core;

/**
 * check_rate_limit (service_internal_methods.md §1.2): локальный in-memory
 * token bucket per partner/application, с периодической синхронизацией
 * агрегированного счётчика в Runtime Redis (~1с, sync_rate_limit_counters)
 * — не Redis-запрос на каждый submit_sm (services_specifictaion.md §2.2).
 */
public final class TokenBucket {
    private final double capacity;
    private final double refillPerSecond;
    private double tokens;
    private long lastRefillEpochMs;

    public TokenBucket(double capacity, double refillPerSecond, long nowEpochMs) {
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.tokens = capacity;
        this.lastRefillEpochMs = nowEpochMs;
    }

    private void refill(long nowEpochMs) {
        long elapsedMs = nowEpochMs - lastRefillEpochMs;
        if (elapsedMs <= 0) {
            return;
        }
        tokens = Math.min(capacity, tokens + (elapsedMs / 1000.0) * refillPerSecond);
        lastRefillEpochMs = nowEpochMs;
    }

    public synchronized boolean tryAcquire(long nowEpochMs) {
        refill(nowEpochMs);
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }
}