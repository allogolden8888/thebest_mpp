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
