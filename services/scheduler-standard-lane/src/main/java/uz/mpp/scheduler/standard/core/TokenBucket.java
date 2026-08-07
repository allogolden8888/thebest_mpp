package uz.mpp.scheduler.standard.core;

/**
 * apply_token_bucket (service_internal_methods.md §2.2): Permit | Deny по
 * scope в пределах лимита. Простой in-JVM token bucket — ёмкость и скорость
 * пополнения, без внешнего состояния.
 *
 * <p><b>Больше не используется по одному per-Kafka-Streams-партиции
 * инстансу в проде</b> (CODE_REVIEW.md HIGH #5 — см. {@link RedisTokenBucket}
 * за реальным фиксом, применяемым в {@code HoldCommandProcessor}). Этот
 * класс остаётся как есть — для юнит-тестов {@link ReleaseBatchSelector} без
 * живого Redis (тот же принцип, что и остальная сессия: pure-логика
 * тестируется без сети) — реализует {@link RateLimiter}, тот же контракт,
 * что и Redis-версия.
 */
public final class TokenBucket implements RateLimiter {

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

    /**
     * {@link RateLimiter#acquireUpTo} — здесь честная реализация через
     * available()+tryAcquire() безопасна (в отличие от Redis-версии, где
     * это была бы гонка): экземпляр этого класса не разделяется между
     * потоками/партициями, вызывающая сторона (Kafka Streams
     * single-threaded per-task processing) гарантирует отсутствие
     * конкурентного доступа.
     */
    @Override
    public int acquireUpTo(int requested, long nowEpochMs) {
        int granted = Math.min(requested, available(nowEpochMs));
        if (granted > 0) {
            tryAcquire(granted, nowEpochMs);
        }
        return granted;
    }
}
