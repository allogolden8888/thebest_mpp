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
     * @param bucket     token bucket того же scope — независимый от
     *                   rampRate дополнительный лимит на batch
     * @param nowEpochMs текущее время для token bucket
     */
    public static List<HeldItem> selectBatch(List<HeldItem> heldItems, double rampRate, TokenBucket bucket, long nowEpochMs) {
        if (heldItems.isEmpty() || rampRate <= 0.0) {
            return List.of();
        }

        int rampBudget = (int) Math.ceil(heldItems.size() * Math.min(1.0, rampRate));
        int tokenBudget = bucket.available(nowEpochMs);
        int batchSize = Math.max(0, Math.min(rampBudget, tokenBudget));
        if (batchSize == 0) {
            return List.of();
        }

        List<HeldItem> ordered = FairScheduler.applyFairScheduling(heldItems);
        List<HeldItem> batch = new ArrayList<>(ordered.subList(0, Math.min(batchSize, ordered.size())));

        if (!bucket.tryAcquire(batch.size(), nowEpochMs)) {
            // Не должно произойти — available() уже проверил budget, но
            // защитный путь на случай гонки с другим вызовом того же bucket.
            return List.of();
        }
        return batch;
    }
}
