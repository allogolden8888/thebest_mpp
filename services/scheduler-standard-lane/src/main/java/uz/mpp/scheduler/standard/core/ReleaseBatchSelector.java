package uz.mpp.scheduler.standard.core;

import java.util.ArrayList;
import java.util.List;

/**
 * release_backlog_batch (service_internal_methods.md §2.2): scope, текущий
 * ramp-шаг -> ReleasedItems[]. Оркестрирует apply_fair_scheduling +
 * apply_token_bucket поверх held-записей одного stage.
 */
public final class ReleaseBatchSelector {

    private ReleaseBatchSelector() {
    }

    /**
     * @param heldItems  все текущие held-записи для данного stageName (уже
     *                   отфильтрованы вызывающей стороной по scope)
     * @param rampRate   compose_effective_rate для scope, 0..1 — доля от
     *                   heldItems.size(), которую в принципе можно
     *                   отпустить в этом тике (controlled ramp-up, не
     *                   отпускать весь backlog разом после выхода из PAUSED)
     * @param rateLimiter token bucket того же scope (in-JVM {@link TokenBucket}
     *                   в тестах, {@link RedisTokenBucket} в проде — см.
     *                   CODE_REVIEW.md HIGH #5 и package doc в {@link RateLimiter})
     *                   — независимый от rampRate дополнительный лимит на batch
     * @param nowEpochMs текущее время для token bucket
     */
    public static List<HeldItem> selectBatch(List<HeldItem> heldItems, double rampRate, RateLimiter rateLimiter, long nowEpochMs) {
        if (heldItems.isEmpty() || rampRate <= 0.0) {
            return List.of();
        }

        int rampBudget = (int) Math.ceil(heldItems.size() * Math.min(1.0, rampRate));

        // CODE_REVIEW.md HIGH #5: один атомарный acquireUpTo — не отдельные
        // available()+tryAcquire() — как только rateLimiter стал реально
        // разделяемым состоянием (RedisTokenBucket, общий на все партиции/
        // реплики), check-then-act здесь сам был бы новым TOCTOU.
        int granted = rateLimiter.acquireUpTo(rampBudget, nowEpochMs);
        if (granted == 0) {
            return List.of();
        }

        List<HeldItem> ordered = FairScheduler.applyFairScheduling(heldItems);
        return new ArrayList<>(ordered.subList(0, Math.min(granted, ordered.size())));
    }
}
