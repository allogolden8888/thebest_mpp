package uz.mpp.scheduler.standard.topology;

import com.google.protobuf.Timestamp;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.events.v1.ExecutionControlRecord;
import uz.mpp.platformcontracts.events.v1.SchedulerHoldCommand;
import uz.mpp.scheduler.standard.core.HeldItem;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TopologyTestDriver — реальная прогонка топологии Kafka Streams (не мок),
 * без живого брокера. Аналог TestContainers/live Kafka для Streams-
 * приложений, официальный инструмент Kafka Streams для offline-тестирования
 * топологии целиком, включая state stores и punctuator'ы (через
 * advanceWallClockTime).
 */
class StandardLaneTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> holdInput;
    private TestInputTopic<String, byte[]> controlInput;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-standard-lane");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/test-state-" + System.nanoTime());

        driver = new TopologyTestDriver(StandardLaneTopology.build(), props);
        holdInput = driver.createInputTopic(Topics.HOLD_COMMANDS, Serdes.String().serializer(), Serdes.ByteArray().serializer());
        controlInput = driver.createInputTopic(Topics.EXECUTION_CONTROL, Serdes.String().serializer(), Serdes.ByteArray().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static SchedulerHoldCommand holdCommand(String messageId, String stageExecId, StageName stageName) {
        return SchedulerHoldCommand.newBuilder()
            .setMessageId(messageId)
            .setStageExecutionId(stageExecId)
            .setScope(uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_STAGE)
            .setScopeId(stageName.name().replace("STAGE_NAME_", ""))
            .setStageName(stageName)
            .setHeldAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
            .build();
    }

    private static ExecutionControlRecord controlRecord(
        uz.mpp.platformcontracts.common.v1.ExecutionControlScope scope, String scopeId,
        uz.mpp.platformcontracts.common.v1.ExecutionControlState state, double admissionRate) {
        return ExecutionControlRecord.newBuilder()
            .setScope(scope)
            .setScopeId(scopeId)
            .setState(state)
            .setAdmissionRate(admissionRate)
            .setDispatchRate(admissionRate)
            .setVersion(1)
            .build();
    }

    @Test
    void heldItemIsReleasedOnceControlIsActive() {
        controlInput.pipeInput("GLOBAL:", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0
        ).toByteArray());

        holdInput.pipeInput("msg-1", holdCommand("msg-1", "exec-1", StageName.STAGE_NAME_BILLING).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> billingOutput = driver.createOutputTopic(
            "stage.billing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());

        assertFalse(billingOutput.isEmpty(), "ожидали released StageExecuteCommand на stage.billing");
        StageExecuteCommand released = parseCommand(billingOutput.readValue());
        assertEquals("exec-1", released.getStageExecutionId());
        assertEquals("msg-1", released.getMessageId());

        assertNull(holdsStore().get("exec-1"), "held item должен быть удалён из store после release");
    }

    @Test
    void heldItemStaysHeldWhileStagePaused() {
        controlInput.pipeInput("STAGE:BILLING", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_STAGE, "BILLING",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0
        ).toByteArray());

        holdInput.pipeInput("msg-2", holdCommand("msg-2", "exec-2", StageName.STAGE_NAME_BILLING).toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> billingOutput = driver.createOutputTopic(
            "stage.billing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());

        assertTrue(billingOutput.isEmpty(), "PAUSED-стадия не должна получать release");
        assertNotNull(holdsStore().get("exec-2"), "held item должен остаться в store, пока стадия PAUSED");
    }

    @Test
    void globalPauseBlocksReleaseRegardlessOfStageState() {
        controlInput.pipeInput("GLOBAL:", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0
        ).toByteArray());

        holdInput.pipeInput("msg-3", holdCommand("msg-3", "exec-3", StageName.STAGE_NAME_ROUTING).toByteArray());
        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> routingOutput = driver.createOutputTopic(
            "stage.routing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertTrue(routingOutput.isEmpty(), "GLOBAL PAUSED должен блокировать любую стадию");
    }

    // Регрессии на CODE_REVIEW.md находки, реальный TopologyTestDriver
    // end-to-end (не юнит на голых классах) — доказывает, что фикс реально
    // подключён в топологию, не только корректен изолированно.

    @Test
    void unspecifiedControlStateIsLoggedAndSkippedNotCrashing() {
        // Critical #1: раньше ExecutionControlState.UNSPECIFIED заставлял
        // ControlSnapshot.State.valueOf(...) бросить необработанный
        // IllegalArgumentException на GlobalStreamThread — здесь просто
        // проверяем, что pipeInput не бросает (driver.close() в @AfterEach
        // тоже не должен бы упасть) и что последующие записи всё ещё
        // нормально обрабатываются — сервис не "падает" после этой записи.
        controlInput.pipeInput("PARTNER:partner-1", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "partner-1",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_UNSPECIFIED, 0.0
        ).toByteArray());

        // Сервис должен продолжать нормально работать после UNSPECIFIED-записи.
        controlInput.pipeInput("GLOBAL:", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0
        ).toByteArray());
        holdInput.pipeInput("msg-unspecified", holdCommand("msg-unspecified", "exec-unspecified", StageName.STAGE_NAME_BILLING).toByteArray());
        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> billingOutput = driver.createOutputTopic(
            "stage.billing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertFalse(billingOutput.isEmpty(), "топология должна продолжать работать после UNSPECIFIED control record, не падать");
    }

    @Test
    void unknownStageNameHoldCommandIsRejectedNotStored() {
        // High #3 (poison-pill): SchedulerHoldCommand с STAGE_NAME_UNSPECIFIED
        // не должен попасть в standard-holds-store вообще — раньше он бы
        // застревал там навсегда и валил releaseTick на каждом тике.
        SchedulerHoldCommand poison = SchedulerHoldCommand.newBuilder()
            .setMessageId("msg-poison")
            .setStageExecutionId("exec-poison")
            .setScope(uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_STAGE)
            .setScopeId("UNSPECIFIED")
            .setStageName(StageName.STAGE_NAME_UNSPECIFIED)
            .setHeldAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
            .build();
        holdInput.pipeInput("msg-poison", poison.toByteArray());

        assertNull(holdsStore().get("exec-poison"), "STAGE_NAME_UNSPECIFIED hold command не должен попасть в store");

        // releaseTick не должен упасть на последующих тиках.
        driver.advanceWallClockTime(Duration.ofSeconds(1));
        driver.advanceWallClockTime(Duration.ofSeconds(1));
    }

    @Test
    void partnerScopedPauseBlocksReleaseEvenWhenGlobalAndStageAreActive() {
        // Critical #2 end-to-end: раньше ControlSnapshot только читал
        // GLOBAL/STAGE на release-стороне — PARTNER-запись в byKey существовала,
        // но никогда не блокировала releaseTick. Теперь held item несёт
        // собственный scope/scope_id и releaseTick обязан их проверить.
        controlInput.pipeInput("GLOBAL:", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0
        ).toByteArray());
        controlInput.pipeInput("PARTNER:partner-42", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER, "partner-42",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, 0.0
        ).toByteArray());

        SchedulerHoldCommand partnerHold = SchedulerHoldCommand.newBuilder()
            .setMessageId("msg-partner")
            .setStageExecutionId("exec-partner")
            .setScope(uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER)
            .setScopeId("partner-42")
            .setStageName(StageName.STAGE_NAME_BILLING)
            .setHeldAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
            .build();
        holdInput.pipeInput("msg-partner", partnerHold.toByteArray());
        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> billingOutput = driver.createOutputTopic(
            "stage.billing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertTrue(billingOutput.isEmpty(), "PARTNER-scoped PAUSED должен блокировать release этого held item'а, даже если GLOBAL/STAGE ACTIVE");
        assertNotNull(holdsStore().get("exec-partner"), "held item должен остаться в store, пока его собственный scope PAUSED");
    }

    @Test
    void releaseOrderFollowsHeldAtTimeNotStageExecutionIdKeyOrder() {
        // High #4: RocksDB-backed store итерирует в KEY order (keyed по
        // stageExecutionId), не по held_at — раньше release order был
        // произвольным относительно времени hold'а. Здесь stageExecutionId
        // сортируется в ОБРАТНОМ порядке относительно held_at, чтобы явно
        // отличить "по ключу" от "по времени": если фикс работает, "exec-b"
        // (held раньше, более поздний по алфавиту ключ) освобождается ПЕРЕД
        // "exec-a" (held позже, более ранний по алфавиту ключ).
        controlInput.pipeInput("GLOBAL:", controlRecord(
            uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_GLOBAL, "",
            uz.mpp.platformcontracts.common.v1.ExecutionControlState.EXECUTION_CONTROL_STATE_ACTIVE, 1.0
        ).toByteArray());

        long now = Instant.now().getEpochSecond();
        SchedulerHoldCommand earlyHeldLaterKey = SchedulerHoldCommand.newBuilder()
            .setMessageId("msg-b").setStageExecutionId("exec-b")
            .setScope(uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_STAGE)
            .setScopeId("BILLING").setStageName(StageName.STAGE_NAME_BILLING)
            .setHeldAt(Timestamp.newBuilder().setSeconds(now).build()) // held раньше
            .build();
        SchedulerHoldCommand lateHeldEarlierKey = SchedulerHoldCommand.newBuilder()
            .setMessageId("msg-a").setStageExecutionId("exec-a")
            .setScope(uz.mpp.platformcontracts.common.v1.ExecutionControlScope.EXECUTION_CONTROL_SCOPE_STAGE)
            .setScopeId("BILLING").setStageName(StageName.STAGE_NAME_BILLING)
            .setHeldAt(Timestamp.newBuilder().setSeconds(now + 100).build()) // held позже
            .build();
        holdInput.pipeInput("msg-b", earlyHeldLaterKey.toByteArray());
        holdInput.pipeInput("msg-a", lateHeldEarlierKey.toByteArray());

        driver.advanceWallClockTime(Duration.ofSeconds(1));

        TestOutputTopic<String, byte[]> billingOutput = driver.createOutputTopic(
            "stage.billing", Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertFalse(billingOutput.isEmpty(), "хотя бы один held item должен быть освобождён в этом тике");
        StageExecuteCommand firstReleased = parseCommand(billingOutput.readValue());
        assertEquals("exec-b", firstReleased.getStageExecutionId(),
            "held раньше (exec-b) должен освобождаться первым, несмотря на то, что его ключ идёт ПОСЛЕ exec-a в алфавитном/RocksDB key order");
    }

    private KeyValueStore<String, HeldItem> holdsStore() {
        return driver.getKeyValueStore(HoldCommandProcessor.STORE_NAME);
    }

    private static StageExecuteCommand parseCommand(byte[] bytes) {
        try {
            return StageExecuteCommand.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
