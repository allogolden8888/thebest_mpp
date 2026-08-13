package uz.mpp.operatorsmpp.core;

/**
 * check_rate_limit (service_internal_methods.md §1.2): локальный in-memory
 * token bucket per partner/application, с периодической синхронизацией
 * агрегированного счётчика в Runtime Redis (~1с, sync_rate_limit_counters)
 * — не Redis-запрос на каждый submit_sm (services_specifictaion.md §2.2).
 */
public final class TokenBucket {

    /**
     * Реальная находка (нагрузочный прогон + прямое измерение на реальном
     * SMSC): раньше capacity бакета == самому rate — процесс, какое-то
     * время не отправлявший ничего, копил токенов ровно на 1 секунду
     * трафика и мог потратить их практически мгновенно, разом (в пределах
     * любого 1-секундного окна возможно до {@code rate + capacity}
     * сообщений — с capacity=rate это давало до 2x номинального TPS).
     * SMSC реально видел burst до ~440/200мс при номинальных ~65/200мс
     * (CV=1.05-1.06 на двух независимых прогонах). Тот же паттерн, уже
     * исправленный сегодня в {@code partner-rest-receiver/src/rate_limit.rs}
     * через {@code MESSAGE_BURST_HEADROOM_FACTOR}. Ограничивает разрешённый
     * мгновенный избыток над номиналом 15% от rate — refillPerSecond НЕ
     * уменьшается, средняя пропускная способность в долгосрочном периоде
     * остаётся точно rate, меняется только максимально допустимый
     * мгновенный всплеск.
     */
    public static final double BURST_HEADROOM_FACTOR = 0.15;

    private final double capacity;
    private final double refillPerSecond;
    private double tokens;
    private long lastRefillEpochMs;

    /**
     * Полная ёмкость (capacity == refillPerSecond), БЕЗ урезания под
     * headroom — для случаев, где такая семантика "полного потолка"
     * осознанно сохранена (например {@code ceilBucket} пейсера, см.
     * PacerCore — это верхний предел всего туннеля, не bucket отдельного
     * приоритетного tier'а, который реально копит и потом отдаёт всплеск).
     */
    public TokenBucket(double capacity, double refillPerSecond, long nowEpochMs) {
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.tokens = capacity;
        this.lastRefillEpochMs = nowEpochMs;
    }

    /**
     * См. {@link #BURST_HEADROOM_FACTOR}. Минимум 1.0 — иначе для низкого
     * rate (например 3 TPS у lowBucket при малом TPS_LIMIT) capacity вышла
     * бы меньше одного полного токена, и bucket не смог бы пропустить НИ
     * ОДНОГО сообщения никогда.
     */
    public static TokenBucket withBurstHeadroom(double refillPerSecond, long nowEpochMs) {
        double capacity = Math.max(refillPerSecond * BURST_HEADROOM_FACTOR, 1.0);
        return new TokenBucket(capacity, refillPerSecond, nowEpochMs);
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