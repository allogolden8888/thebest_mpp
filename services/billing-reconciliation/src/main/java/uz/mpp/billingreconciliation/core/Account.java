package uz.mpp.billingreconciliation.core;

import java.util.Set;

/** Account — перенос 1:1 state_machines/billing_account_state.py::Account. */
public record Account(long balanceMinorUnits, AccountState state, long epoch, Set<String> processedChargeIds) {

    public Account withBalance(long newBalance) {
        return new Account(newBalance, state, epoch, processedChargeIds);
    }

    public Account withState(AccountState newState) {
        return new Account(balanceMinorUnits, newState, epoch, processedChargeIds);
    }

    public Account withEpoch(long newEpoch) {
        return new Account(balanceMinorUnits, state, newEpoch, processedChargeIds);
    }
}