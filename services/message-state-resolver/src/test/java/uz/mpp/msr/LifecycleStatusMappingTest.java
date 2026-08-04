package uz.mpp.msr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.MessageLifecycleStatus;

/**
 * {@link LifecycleStatusMapping#fromProto} — новая для restore_from_changelog
 * (KafkaIo.restoreFromChangelog) обратная сторона существующего toProto.
 * Round-trip через все реальные значения — прямая проверка, что
 * restore-путь реконструирует тот же {@link LifecycleStatus}, что был
 * записан в message-state.changelog изначально.
 */
class LifecycleStatusMappingTest {

    @Test
    void everyRealStatusRoundTripsThroughProto() {
        for (LifecycleStatus status : LifecycleStatus.values()) {
            MessageLifecycleStatus proto = LifecycleStatusMapping.toProto(status);
            assertEquals(status, LifecycleStatusMapping.fromProto(proto), "round-trip должен вернуть тот же статус для " + status);
        }
    }

    @Test
    void unspecifiedAndUnrecognizedMapToNull() {
        assertNull(LifecycleStatusMapping.fromProto(MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_UNSPECIFIED));
        assertNull(LifecycleStatusMapping.fromProto(MessageLifecycleStatus.UNRECOGNIZED));
    }
}
