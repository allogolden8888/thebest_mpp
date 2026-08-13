package uz.mpp.operatorsmpp.core;

import java.util.EnumMap;
import java.util.Map;

/**
 * Priority-tier scheduler (dynamic-seeking-russell.md "Priority-tier
 * scheduler") — чистая two-phase HTB-style (Hierarchical Token Bucket)
 * dispatch-логика туннеля к оператору. Никаких потоков/очередей/gRPC/сети —
 * только голые числа (queue depth per tier, available permits) и
 * {@link TokenBucket}-инстансы, поэтому тестируется без реального
 * {@code ArrayBlockingQueue}/{@code Semaphore}/времени. Реальная
 * диспетчеризация (см. {@code Main.java}) вызывает {@link #decide} раз в
 * тик (20мс), получает план "сколько элементов какого tier'а дёрнуть из
 * очереди в этот тик" и сама уже делает {@code queue.poll()}/{@code
 * executor.submit()} по этому плану.
 *
 * <p><b>Phase 1 (guaranteed)</b>, порядок HIGH→MEDIUM→LOW: каждый tier
 * допускается до своего собственного гарантированного bucket'а (rate =
 * {@code TPS_LIMIT × {0.70, 0.20, 0.10}}) + permits. {@code ceilBucket} не
 * является тут гейтом (тройная доля в сумме даёт ровно 1.0 — по построению
 * фаза 1 не может превысить общий потолок только за счёт собственных
 * bucket'ов), но КАЖДОЕ фактическое admission в фазе 1 всё равно списывает
 * токен и из {@code ceilBucket} (без проверки результата) — иначе
 * {@code ceilBucket} в фазе 2 не отражал бы, сколько уже реально
 * отправлено в этот тик через фазу 1, и заимствование могло бы добавить
 * ЕЩЁ один полный {@code TPS_LIMIT} поверх — ровно тот же burst-паттерн,
 * который весь этот пейсер должен устранить, только на уровне всего
 * туннеля вместо одного tier'а.
 *
 * <p><b>Phase 2 (borrowing)</b>, тот же порядок приоритета: то, что
 * осталось невостребованным в {@code ceilBucket} после фазы 1 (то есть
 * реальный неиспользованный запас относительно {@code TPS_LIMIT}),
 * предлагается tier'ам, у которых всё ещё есть очередь — в порядке
 * приоритета, так что HIGH может превысить свои 70% при простое
 * MEDIUM/LOW, а простаивающий HIGH отдаёт запас вниз, тоже в порядке
 * приоритета (MEDIUM раньше LOW).
 */
public final class PacerCore {

    public static final double HIGH_SHARE = 0.70;
    public static final double MEDIUM_SHARE = 0.20;
    public static final double LOW_SHARE = 0.10;

    private final TokenBucket ceilBucket;
    private final Map<PriorityTier, TokenBucket> tierBuckets;

    public PacerCore(TokenBucket ceilBucket, TokenBucket highBucket, TokenBucket mediumBucket, TokenBucket lowBucket) {
        this.ceilBucket = ceilBucket;
        this.tierBuckets = new EnumMap<>(PriorityTier.class);
        this.tierBuckets.put(PriorityTier.HIGH, highBucket);
        this.tierBuckets.put(PriorityTier.MEDIUM, mediumBucket);
        this.tierBuckets.put(PriorityTier.LOW, lowBucket);
    }

    /**
     * @param queueDepth       сколько элементов сейчас ждёт в очереди каждого tier'а
     *                         (демонстрирует "спрос" — реальные очереди наружи, здесь только счётчик)
     * @param availablePermits сколько permits свободно у {@code Semaphore(MAX_CONCURRENT_SUBMITS)}
     *                         на начало этого тика
     * @param nowEpochMs       текущее время (для refill token bucket'ов)
     * @return план — сколько элементов каждого tier'а допустить к диспетчеризации в этот тик
     */
    public AdmitPlan decide(Map<PriorityTier, Integer> queueDepth, int availablePermits, long nowEpochMs) {
        Map<PriorityTier, Integer> remaining = new EnumMap<>(PriorityTier.class);
        Map<PriorityTier, Integer> admitted = new EnumMap<>(PriorityTier.class);
        for (PriorityTier tier : PriorityTier.values()) {
            remaining.put(tier, Math.max(0, queueDepth.getOrDefault(tier, 0)));
            admitted.put(tier, 0);
        }

        int[] permits = {availablePermits};

        for (PriorityTier tier : PriorityTier.values()) {
            admitGuaranteed(tier, remaining, admitted, permits, nowEpochMs);
        }
        for (PriorityTier tier : PriorityTier.values()) {
            admitBorrowed(tier, remaining, admitted, permits, nowEpochMs);
        }

        return new AdmitPlan(admitted);
    }

    private void admitGuaranteed(PriorityTier tier, Map<PriorityTier, Integer> remaining,
                                  Map<PriorityTier, Integer> admitted, int[] permits, long nowEpochMs) {
        TokenBucket tierBucket = tierBuckets.get(tier);
        int demand = remaining.get(tier);
        int count = 0;
        while (count < demand && permits[0] > 0 && tierBucket.tryAcquire(nowEpochMs)) {
            count++;
            permits[0]--;
            // Не гейтим фазу 1 по ceilBucket (гарантия не должна зависеть от
            // того, сколько уже позаимствовано фазой 2 в предыдущих тиках),
            // но синхронизируем его состояние с фактическим расходом — см.
            // javadoc класса.
            ceilBucket.tryAcquire(nowEpochMs);
        }
        remaining.put(tier, demand - count);
        admitted.merge(tier, count, Integer::sum);
    }

    private void admitBorrowed(PriorityTier tier, Map<PriorityTier, Integer> remaining,
                                Map<PriorityTier, Integer> admitted, int[] permits, long nowEpochMs) {
        int demand = remaining.get(tier);
        int count = 0;
        while (count < demand && permits[0] > 0 && ceilBucket.tryAcquire(nowEpochMs)) {
            count++;
            permits[0]--;
        }
        remaining.put(tier, demand - count);
        admitted.merge(tier, count, Integer::sum);
    }

    /** Сколько элементов каждого tier'а допущено к диспетчеризации в этот тик. */
    public record AdmitPlan(Map<PriorityTier, Integer> admittedPerTier) {
        public int forTier(PriorityTier tier) {
            return admittedPerTier.getOrDefault(tier, 0);
        }

        public int total() {
            return admittedPerTier.values().stream().mapToInt(Integer::intValue).sum();
        }
    }
}
