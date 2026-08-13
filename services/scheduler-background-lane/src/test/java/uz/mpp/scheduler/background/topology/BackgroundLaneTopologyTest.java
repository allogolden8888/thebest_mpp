package uz.mpp.scheduler.background.topology;

import com.google.protobuf.Timestamp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.BackgroundTaskType;
import uz.mpp.platformcontracts.events.v1.NotificationRetryTask;
import uz.mpp.platformcontracts.events.v1.SchedulerBackgroundTask;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackgroundLaneTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> input;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-background-lane");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/test-state-" + System.nanoTime());

        driver = new TopologyTestDriver(BackgroundLaneTopology.build(), props);
        input = driver.createInputTopic(Topics.BACKGROUND_COMMANDS, Serdes.String().serializer(), Serdes.ByteArray().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static SchedulerBackgroundTask task(BackgroundTaskType type, String sourceEventId, long dueAtEpochSec, String targetTopic) {
        return SchedulerBackgroundTask.newBuilder()
            .setTaskType(type)
            .setSourceEventId(sourceEventId)
            .setAttempt(1)
            .setDueAt(Timestamp.newBuilder().setSeconds(dueAtEpochSec).build())
            .setTargetTopic(targetTopic)
            .build();
    }

    /** STAGE_RETRY несёт message_id, не source_event_id — см. BackgroundTask.sourceEventId javadoc. */
    private static SchedulerBackgroundTask stageRetryTask(String messageId, int attempt, long dueAtEpochSec) {
        return SchedulerBackgroundTask.newBuilder()
            .setTaskType(BackgroundTaskType.BACKGROUND_TASK_TYPE_STAGE_RETRY)
            .setMessageId(messageId)
            .setAttempt(attempt)
            .setDueAt(Timestamp.newBuilder().setSeconds(dueAtEpochSec).build())
            .setTargetTopic(Topics.PIPELINE_RETRY_TRIGGERS)
            .build();
    }

    @Test
    void dlrRetryTaskDispatchedOnceDue() {
        long now = Instant.now().getEpochSecond();
        input.pipeInput("evt-1", task(BackgroundTaskType.BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY, "evt-1", now - 10, Topics.OPERATOR_DLR_UNRESOLVED).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> output = driver.createOutputTopic(
            Topics.OPERATOR_DLR_UNRESOLVED, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertFalse(output.isEmpty(), "просроченная DLR retry задача должна быть диспатчнута");

        SchedulerBackgroundTask dispatched = parse(output.readValue());
        assertEquals("evt-1", dispatched.getSourceEventId());
        assertEquals(2, dispatched.getAttempt(), "attempt должен увеличиться на диспатче");
    }

    @Test
    void notificationRetryTaskDispatchedToNotificationTopic() {
        long now = Instant.now().getEpochSecond();
        input.pipeInput("evt-2", task(BackgroundTaskType.BACKGROUND_TASK_TYPE_NOTIFICATION_RETRY, "evt-2", now - 10, Topics.NOTIFICATION_RETRY).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> output = driver.createOutputTopic(
            Topics.NOTIFICATION_RETRY, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertFalse(output.isEmpty());

        NotificationRetryTask retry = parseNotification(output.readValue());
        assertEquals("evt-2", retry.getLifecycleEventId());
        assertEquals(2, retry.getAttempt());
    }

    @Test
    void notYetDueTaskIsNotDispatched() {
        long now = Instant.now().getEpochSecond();
        input.pipeInput("evt-3", task(BackgroundTaskType.BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY, "evt-3", now + 3600, Topics.OPERATOR_DLR_UNRESOLVED).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> output = driver.createOutputTopic(
            Topics.OPERATOR_DLR_UNRESOLVED, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertTrue(output.isEmpty(), "задача с будущим due_at не должна диспатчиться сейчас");
    }

    @Test
    void taskIsRemovedFromStoreAfterDispatch() {
        long now = Instant.now().getEpochSecond();
        input.pipeInput("evt-4", task(BackgroundTaskType.BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY, "evt-4", now - 1, Topics.OPERATOR_DLR_UNRESOLVED).toByteArray());
        driver.advanceWallClockTime(Duration.ofSeconds(1));

        var store = driver.<String, uz.mpp.scheduler.background.core.BackgroundTask>getKeyValueStore(BackgroundCommandProcessor.STORE_NAME);
        assertEquals(null, store.get("evt-4"), "задача должна быть удалена из store после диспатча");
    }

    @Test
    void stageRetryTaskDispatchedToPipelineRetryTriggersWithAttemptUnchanged() {
        // В отличие от DLR/notification republish (attempt+1 — счётчик
        // попыток РАЗБУДИТЬ саму фоновую задачу), для STAGE_RETRY attempt
        // уже увеличен Pipeline Engine'ом ДО постановки задачи (см.
        // DispatchBuilder.buildStageRetryTrigger javadoc) — здесь он
        // остаётся тем же, не удваивается.
        long now = Instant.now().getEpochSecond();
        input.pipeInput("m1", stageRetryTask("m1", 2, now - 10).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> output = driver.createOutputTopic(
            Topics.PIPELINE_RETRY_TRIGGERS, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertFalse(output.isEmpty(), "просроченная STAGE_RETRY задача должна быть диспатчнута");

        SchedulerBackgroundTask dispatched = parse(output.readValue());
        assertEquals("m1", dispatched.getMessageId());
        assertEquals(2, dispatched.getAttempt(), "attempt НЕ должен увеличиться повторно на диспатче STAGE_RETRY");
    }

    @Test
    void stageRetryTaskNotYetDueIsNotDispatched() {
        long now = Instant.now().getEpochSecond();
        input.pipeInput("m2", stageRetryTask("m2", 1, now + 3600).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> output = driver.createOutputTopic(
            Topics.PIPELINE_RETRY_TRIGGERS, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertTrue(output.isEmpty(), "задача с будущим due_at не должна диспатчиться сейчас");
    }

    private static SchedulerBackgroundTask parse(byte[] bytes) {
        try {
            return SchedulerBackgroundTask.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static NotificationRetryTask parseNotification(byte[] bytes) {
        try {
            return NotificationRetryTask.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
