package uz.mpp.scheduler.standard.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * apply_fair_scheduling (service_internal_methods.md §2.2): несколько
 * партнёров могут иметь held-записи под одним и тем же scope (например,
 * GLOBAL hold затрагивает много партнёров одновременно) — round-robin по
 * {@link HeldItem#fairnessKey()}, чтобы один партнёр с большим backlog не
 * монополизировал batch release, вытесняя остальных.
 */
public final class FairScheduler {

    private FairScheduler() {
    }

    /**
     * Возвращает элементы candidates в round-robin порядке по fairnessKey,
     * сохраняя относительный порядок внутри каждой группы (FIFO по
     * held_at внутри группы, если candidates уже отсортированы по held_at).
     */
    public static List<HeldItem> applyFairScheduling(List<HeldItem> candidates) {
        Map<String, Deque<HeldItem>> byKey = new LinkedHashMap<>();
        for (HeldItem item : candidates) {
            byKey.computeIfAbsent(item.fairnessKey(), k -> new ArrayDeque<>()).add(item);
        }

        List<HeldItem> ordered = new ArrayList<>(candidates.size());
        boolean progress = true;
        while (progress) {
            progress = false;
            for (Deque<HeldItem> queue : byKey.values()) {
                HeldItem next = queue.poll();
                if (next != null) {
                    ordered.add(next);
                    progress = true;
                }
            }
        }
        return ordered;
    }
}
