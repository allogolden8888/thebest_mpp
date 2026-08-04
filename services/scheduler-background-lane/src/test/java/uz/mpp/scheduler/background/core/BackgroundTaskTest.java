package uz.mpp.scheduler.background.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackgroundTaskTest {

    @Test
    void isDueTrueWhenDueAtInPast() {
        BackgroundTask task = new BackgroundTask("NOTIFICATION_RETRY", "evt-1", 1, 100, 0, "notification.retry");
        assertTrue(task.isDue(200));
        assertFalse(task.isDue(50));
    }

    @Test
    void isExpiredFalseWhenDeadlineZero() {
        BackgroundTask task = new BackgroundTask("NOTIFICATION_RETRY", "evt-1", 1, 100, 0, "notification.retry");
        assertFalse(task.isExpired(Long.MAX_VALUE), "deadline=0 означает 'нет дедлайна', не 'уже истёк'");
    }

    @Test
    void isExpiredTrueAfterDeadline() {
        BackgroundTask task = new BackgroundTask("NOTIFICATION_RETRY", "evt-1", 1, 100, 500, "notification.retry");
        assertFalse(task.isExpired(400));
        assertTrue(task.isExpired(500));
        assertTrue(task.isExpired(600));
    }
}
