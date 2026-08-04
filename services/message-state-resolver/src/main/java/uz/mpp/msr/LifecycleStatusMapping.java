package uz.mpp.msr;

import uz.mpp.platformcontracts.common.v1.MessageLifecycleStatus;

/** {@link LifecycleStatus} (внутренний, ровно повторяет Python-версию по
 * именам) <-> {@link MessageLifecycleStatus} (protobuf, wire-формат). */
public final class LifecycleStatusMapping {

    private LifecycleStatusMapping() {
    }

    public static MessageLifecycleStatus toProto(LifecycleStatus status) {
        return switch (status) {
            case SUBMITTED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_SUBMITTED;
            case DELIVERED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_DELIVERED;
            case UNDELIVERABLE -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_UNDELIVERABLE;
            case DELIVERY_UNRESOLVED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_DELIVERY_UNRESOLVED;
            case LATE_DELIVERY_CONFIRMED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_LATE_DELIVERY_CONFIRMED;
            case REJECTED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_REJECTED;
            case FAILED -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_FAILED;
            case SYSTEM_UNAVAILABLE -> MessageLifecycleStatus.MESSAGE_LIFECYCLE_STATUS_SYSTEM_UNAVAILABLE;
        };
    }

    /**
     * Обратное направление — нужно для {@code restore_from_changelog}
     * (KafkaIo.restoreFromChangelog): реконструирует {@link LifecycleState}
     * из ранее записанного {@code MessageLifecycleEvent} в
     * {@code message-state.changelog}. {@code null} для
     * {@code UNSPECIFIED}/{@code UNRECOGNIZED} — такая запись в changelog не
     * должна была появиться (writer этого же сервиса никогда не пишет
     * UNSPECIFIED), но decode-независимая защита лучше NPE/MatchException
     * при чтении чужого/повреждённого changelog-топика.
     */
    public static LifecycleStatus fromProto(MessageLifecycleStatus proto) {
        return switch (proto) {
            case MESSAGE_LIFECYCLE_STATUS_SUBMITTED -> LifecycleStatus.SUBMITTED;
            case MESSAGE_LIFECYCLE_STATUS_DELIVERED -> LifecycleStatus.DELIVERED;
            case MESSAGE_LIFECYCLE_STATUS_UNDELIVERABLE -> LifecycleStatus.UNDELIVERABLE;
            case MESSAGE_LIFECYCLE_STATUS_DELIVERY_UNRESOLVED -> LifecycleStatus.DELIVERY_UNRESOLVED;
            case MESSAGE_LIFECYCLE_STATUS_LATE_DELIVERY_CONFIRMED -> LifecycleStatus.LATE_DELIVERY_CONFIRMED;
            case MESSAGE_LIFECYCLE_STATUS_REJECTED -> LifecycleStatus.REJECTED;
            case MESSAGE_LIFECYCLE_STATUS_FAILED -> LifecycleStatus.FAILED;
            case MESSAGE_LIFECYCLE_STATUS_SYSTEM_UNAVAILABLE -> LifecycleStatus.SYSTEM_UNAVAILABLE;
            case MESSAGE_LIFECYCLE_STATUS_UNSPECIFIED, UNRECOGNIZED -> null;
        };
    }
}
