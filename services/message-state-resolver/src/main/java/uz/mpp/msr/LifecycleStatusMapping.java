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
}
