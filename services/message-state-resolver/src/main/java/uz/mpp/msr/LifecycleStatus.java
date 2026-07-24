package uz.mpp.msr;

/** Зеркало {@code MessageLifecycleStatus} (platform-contracts/common/enums.proto). */
public enum LifecycleStatus {
    SUBMITTED,
    DELIVERED,
    UNDELIVERABLE,
    DELIVERY_UNRESOLVED,
    LATE_DELIVERY_CONFIRMED,
    REJECTED,
    FAILED,
    SYSTEM_UNAVAILABLE
}
