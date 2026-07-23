package uz.mpp.billing;

import org.junit.jupiter.api.Test;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.billing.BillingAccountState.AccountState;
import uz.mpp.billing.BillingAccountState.ChargeOutcome;
import uz.mpp.billing.BillingAccountState.ChargeResult;
import uz.mpp.billing.BillingAccountState.StaleEpochException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uz.mpp.billing.BillingAccountState.applyCharge;
import static uz.mpp.billing.BillingAccountState.freeze;
import static uz.mpp.billing.BillingAccountState.unfreeze;

/** Тот же набор тестов, что {@code state_machines/billing_account_state.py} — одноимённо, для сверки 1:1. */
class BillingAccountStateTest {

    private static Account fresh() {
        return Account.fresh(100_000);
    }

    @Test
    void normalChargeApplies() {
        ChargeResult result = applyCharge(fresh(), "charge-1", 1_000, 0);
        assertEquals(ChargeOutcome.APPLIED, result.outcome());
        assertEquals(99_000, result.account().balance());
    }

    @Test
    void duplicateChargeIdIsIdempotent() {
        ChargeResult r1 = applyCharge(fresh(), "charge-1", 1_000, 0);
        ChargeResult r2 = applyCharge(r1.account(), "charge-1", 1_000, 0);
        assertEquals(ChargeOutcome.APPLIED, r1.outcome());
        assertEquals(ChargeOutcome.ALREADY_PROCESSED, r2.outcome());
        assertEquals(99_000, r2.account().balance(), "списано только один раз");
    }

    @Test
    void chargeRejectedWhileFrozen() {
        Account frozen = freeze(fresh());
        ChargeResult result = applyCharge(frozen, "charge-1", 1_000, 1);
        assertEquals(ChargeOutcome.ACCOUNT_FROZEN, result.outcome());
        assertEquals(100_000, result.account().balance());
    }

    @Test
    void inFlightChargeRejectedByStaleEpochRace() {
        // Ключевое свойство fencing: charge, прочитавший epoch=0 до freeze,
        // но применяющийся после (epoch уже 1), обязан быть отклонён.
        Account account = fresh();
        long epochSeenByInflightCharge = account.epoch();

        Account frozen = freeze(account);
        assertEquals(1, frozen.epoch());

        ChargeResult result = applyCharge(frozen, "in-flight-charge", 5_000, epochSeenByInflightCharge);
        assertEquals(ChargeOutcome.STALE_EPOCH, result.outcome());
        assertEquals(100_000, result.account().balance(), "деньги не должны были списаться со свежезамороженного счёта");
    }

    @Test
    void freezeIsIdempotentDoesNotDoubleBumpEpoch() {
        Account frozen = freeze(fresh());
        assertEquals(1, frozen.epoch());
        Account frozenAgain = freeze(frozen);
        assertEquals(1, frozenAgain.epoch(), "повторный freeze не должен двигать epoch дальше");
    }

    @Test
    void fencedUnfreezeSuccessPath() {
        Account frozen = freeze(fresh());
        assertEquals(AccountState.FROZEN, frozen.state());
        assertEquals(1, frozen.epoch());

        Account unfrozen = unfreeze(frozen, 97_500, 1);
        assertEquals(AccountState.ACTIVE, unfrozen.state());
        assertEquals(97_500, unfrozen.balance());
        assertEquals(2, unfrozen.epoch());
    }

    @Test
    void fencedUnfreezeRejectsIfEpochMovedDuringRecompute() {
        Account frozen = freeze(fresh());
        assertEquals(1, frozen.epoch());

        // Кто-то другой успел разморозить и заморозить счёт заново, пока мы считали.
        Account reactivated = unfreeze(frozen, 99_000, 1); // epoch -> 2, ACTIVE
        Account refrozen = freeze(reactivated); // epoch -> 3, FROZEN снова

        assertThrows(StaleEpochException.class, () -> unfreeze(refrozen, 123_456, 1));
    }

    @Test
    void chargeAfterSuccessfulUnfreezeUsesNewEpoch() {
        Account frozen = freeze(fresh());
        Account unfrozen = unfreeze(frozen, 97_500, 1);
        assertEquals(2, unfrozen.epoch());
        ChargeResult result = applyCharge(unfrozen, "charge-after-recovery", 500, 2);
        assertEquals(ChargeOutcome.APPLIED, result.outcome());
        assertEquals(97_000, result.account().balance());
    }
}
