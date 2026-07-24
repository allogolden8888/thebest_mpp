package uz.mpp.billingreconciliation.core;

/** compute_drift (service_internal_methods.md §5.3): баланс Billing Redis vs баланс из PostgreSQL ledger. */
public record DriftReport(String accountId, long redisBalanceMinorUnits, long ledgerBalanceMinorUnits) {

    public long driftMinorUnits() {
        return redisBalanceMinorUnits - ledgerBalanceMinorUnits;
    }

    public long absDriftMinorUnits() {
        return Math.abs(driftMinorUnits());
    }
}