package uz.mpp.scheduler.standard.core;

/**
 * apply_token_bucket (service_internal_methods.md §2.2): Permit | Deny по
 * scope в пределах лимита. Простой token bucket — ёмкость и скорость
 * пополнения, без внешнего состояния (per-scope инстанс держит вызывающая
 * сторона, {@link uz.mpp.scheduler.standard.core.ControlSnapshot} не хранит
 * bucket'ы — они per-instance, не разделяются между репликами Standard
 * Lane, что допустимо: единица тут — controlled ramp-up скорость, не точный
 * глобальный лимит, см. `hld.md` §8 "допустимо кратковременное превышение").
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
        double added = (elapsedMs / 1000.0) * refillPerSecond;
        tokens = Math.min(capacity, tokens + added);
        lastRefillEpochMs = nowEpochMs;
    }

    /** Пытается снять {@code count} токенов; Permit=true, Deny=false. */
    public boolean tryAcquire(int count, long nowEpochMs) {
        refill(nowEpochMs);
        if (tokens >= count) {
            tokens -= count;
            return true;
        }
        return false;
    }

    /** Сколько permits доступно прямо сейчас, без списания (для батч-расчёта). */
    public int available(long nowEpochMs) {
        refill(nowEpochMs);
        return (int) Math.floor(tokens);
    }
}
