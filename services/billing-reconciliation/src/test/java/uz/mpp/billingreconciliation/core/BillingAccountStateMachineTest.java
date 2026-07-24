package uz.mpp.billingreconciliation.core;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static uz.mpp.billingreconciliation.core.BillingAccountStateMachine.*;

/**
 * Перенос 1:1 тестов state_machines/billing_account_state.py (те же имена,
 * для трассируемости) — доказывает, что порт на Java сохраняет то же
 * поведение fencing по account_epoch.
 */
class BillingAccountStateMachineTest {

    private static Account fresh(long balance) {
        return new Account(balance, AccountState.ACTIVE, 0, Set.of());
    }

    @Test
    void testNormalChargeApplies() {
        Account acc = fresh(100_000);
        var result = applyCharge(acc, "charge-1", 1_000, 0);
        assertEquals(ChargeResult.APPLIED, result.result());
        assertEquals(99_000, result.account().balanceMinorUnits());
    }

    @Test
    void testDuplicateChargeIdIsIdempotent() {
        Account acc = fresh(100_000);
        var r1 = applyCharge(acc, "charge-1", 1_000, 0);
        var r2 = applyCharge(r1.account(), "charge-1", 1_000, 0);
        assertEquals(ChargeResult.APPLIED, r1.result());
        assertEquals(ChargeResult.ALREADY_PROCESSED, r2.result());
        assertEquals(99_000, r2.account().balanceMinorUnits(), "списано только один раз");
    }

    @Test
    void testChargeRejectedWhileFrozen() {
        Account acc = freeze(fresh(100_000));
        var result = applyCharge(acc, "charge-1", 1_000, 1);
        assertEquals(ChargeResult.ACCOUNT_FROZEN, result.result());
        assertEquals(100_000, result.account().balanceMinorUnits(), "не изменился");
    }

    @Test
    void testInFlightChargeRejectedByStaleEpochRace() {
        // Ключевое свойство fencing: charge, который успел прочитать epoch=0
        // ДО freeze, но пытается применить операцию ПОСЛЕ freeze (epoch уже 1),
        // обязан быть отклонён — а не списать деньги с только что замороженного счёта.
        Account acc = fresh(100_000); // epoch=0
        long epochSeenByInflightCharge = acc.epoch(); // 0, "прочитано" до freeze

        acc = freeze(acc); // epoch теперь 1, счёт заморожен
        assertEquals(1, acc.epoch());

        var result = applyCharge(acc, "in-flight-charge", 5_000, epochSeenByInflightCharge);
        assertEquals(ChargeResult.STALE_EPOCH, result.result());
        assertEquals(100_000, result.account().balanceMinorUnits(), "деньги не должны были списаться со свежезамороженного счёта");
    }

    @Test
    void testFreezeIsIdempotentDoesNotDoubleBumpEpoch() {
        Account acc = freeze(fresh(100_000));
        assertEquals(1, acc.epoch());
        acc = freeze(acc);
        assertEquals(1, acc.epoch(), "повторный freeze не должен двигать epoch дальше");
    }

    @Test
    void testFencedUnfreezeSuccessPath() {
        Account acc = freeze(fresh(100_000));
        assertEquals(AccountState.FROZEN, acc.state());
        assertEquals(1, acc.epoch());

        long recomputed = 97_500;
        acc = unfreeze(acc, recomputed, 1);
        assertEquals(AccountState.ACTIVE, acc.state());
        assertEquals(recomputed, acc.balanceMinorUnits());
        assertEquals(2, acc.epoch());
    }

    @Test
    void testFencedUnfreezeRejectsIfEpochMovedDuringRecompute() {
        Account acc = freeze(fresh(100_000));
        assertEquals(1, acc.epoch());

        // Кто-то другой успел разморозить и заморозить счёт заново, пока мы считали.
        acc = unfreeze(acc, 99_000, 1); // epoch -> 2, ACTIVE
        acc = freeze(acc); // epoch -> 3, FROZEN снова

        Account finalAcc = acc;
        assertThrows(IllegalStateException.class, () -> unfreeze(finalAcc, 123_456, 1),
            "должно было отклонить unfreeze с устаревшим expected_epoch");
    }

    @Test
    void testChargeAfterSuccessfulUnfreezeUsesNewEpoch() {
        Account acc = freeze(fresh(100_000));
        acc = unfreeze(acc, 97_500, 1);
        assertEquals(2, acc.epoch());
        var result = applyCharge(acc, "charge-after-recovery", 500, 2);
        assertEquals(ChargeResult.APPLIED, result.result());
        assertEquals(97_000, result.account().balanceMinorUnits());
    }
}