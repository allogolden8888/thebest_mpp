package uz.mpp.scheduler.background.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DueTaskSelectorTest {

    private static BackgroundTask task(String id, long dueAt) {
        return new BackgroundTask("DLR_CORRELATION_RETRY", id, 1, dueAt, 0, "operator.dlr.unresolved");
    }

    @Test
    void selectsOnlyTasksWhoseDueAtHasPassed() {
        List<BackgroundTask> all = List.of(task("t1", 100), task("t2", 200), task("t3", 50));
        List<BackgroundTask> due = DueTaskSelector.selectDue(all, 150);
        assertEquals(2, due.size());
        assertTrue(due.stream().anyMatch(t -> t.sourceEventId().equals("t1")));
        assertTrue(due.stream().anyMatch(t -> t.sourceEventId().equals("t3")));
    }

    @Test
    void emptyWhenNothingDueYet() {
        List<BackgroundTask> all = List.of(task("t1", 1000));
        assertEquals(List.of(), DueTaskSelector.selectDue(all, 500));
    }

    @Test
    void taskDueExactlyAtNowCounts() {
        List<BackgroundTask> all = List.of(task("t1", 1000));
        assertEquals(1, DueTaskSelector.selectDue(all, 1000).size());
    }
}
