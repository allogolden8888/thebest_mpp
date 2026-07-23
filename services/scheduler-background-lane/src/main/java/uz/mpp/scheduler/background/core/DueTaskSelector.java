package uz.mpp.scheduler.background.core;

import java.util.ArrayList;
import java.util.List;

/**
 * tick_delay_queue (service_internal_methods.md §2.3): внутренний таймер ->
 * DueTasks[]. Чистая функция — отбирает задачи, чей due_at уже наступил,
 * из уже загруженного списка (вызывающая сторона отвечает за итерацию
 * state store).
 */
public final class DueTaskSelector {

    private DueTaskSelector() {
    }

    public static List<BackgroundTask> selectDue(List<BackgroundTask> all, long nowEpochMs) {
        List<BackgroundTask> due = new ArrayList<>();
        for (BackgroundTask task : all) {
            if (task.isDue(nowEpochMs)) {
                due.add(task);
            }
        }
        return due;
    }
}
