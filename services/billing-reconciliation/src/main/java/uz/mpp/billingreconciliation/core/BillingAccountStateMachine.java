package uz.mpp.billingreconciliation.core;

import java.util.HashSet;
import java.util.Set;

/**
 * Перенос 1:1 state_machines/billing_account_state.py — ACTIVE &lt;-&gt; FROZEN
 * с fencing по account_epoch (HLD §15.3/§15.5). Production-реализация
 * атомарности — Billing Redis Lua/Redis Function (development_plan.md 4.2,
 * владеет Главный агент, потребители — pipeline-engine/billing-service);
 * этот класс — тот же алгоритм, перенесённый как исполняемая Java-модель
 * для Billing Reconciliation (trigger_freeze/apply_fenced_cas/trigger_unfreeze,
 * service_internal_methods.md §5.3), не сама атомарная Redis-операция.
 */
public final class BillingAccountStateMachine {

    private BillingAccountStateMachine() {
    }

    public enum ChargeResult { APPLIED, ALREADY_PROCESSED, ACCOUNT_FROZEN, STALE_EPOCH }

    /** Идемпотентно: заморозка уже замороженного счёта не двигает epoch дальше. */
    public static Account freeze(Account account) {
        if (account.state() == AccountState.FROZEN) {
            return account;
        }
        return account.withState(AccountState.FROZEN).withEpoch(account.epoch() + 1);
    }

    /**
     * Fenced CAS: unfreeze применяется только если epoch не уехал дальше,
     * пока считался recomputedBalance (иначе кто-то другой уже разморозил/
     * заморозил счёт заново, и наш recompute устарел).
     */
    public static Account unfreeze(Account account, long recomputedBalance, long expectedEpoch) {
        if (account.state() == AccountState.ACTIVE) {
            return account; // уже разморожен
        }
        if (account.epoch() != expectedEpoch) {
            throw new IllegalStateException(
                "Fenced CAS отклонён: epoch уехал с " + expectedEpoch + " на " + account.epoch()
                    + " пока считался recomputed_balance — recovery должен пересчитать заново");
        }
        return new Account(recomputedBalance, AccountState.ACTIVE, account.epoch() + 1, account.processedChargeIds());
    }

    /**
     * Соответствует Lua-скрипту apply_atomic_charge (HLD §15.2): все три
     * проверки — account_state, account_epoch, charge_id dedup — атомарны
     * в реальном Redis Function, здесь смоделированы как одна функция.
     * Billing Reconciliation сам не вызывает этот метод в проде (это путь
     * Billing Service) — перенесён вместе с freeze/unfreeze, потому что
     * тестирует то же самое свойство fencing (test_in_flight_charge_rejected_
     * by_stale_epoch_race), которое recovery (apply_fenced_cas) обязан не
     * сломать.
     */
    public static ChargeResultWithAccount applyCharge(Account account, String chargeId, long amount, long expectedEpoch) {
        if (account.processedChargeIds().contains(chargeId)) {
            return new ChargeResultWithAccount(account, ChargeResult.ALREADY_PROCESSED);
        }
        if (account.epoch() != expectedEpoch) {
            return new ChargeResultWithAccount(account, ChargeResult.STALE_EPOCH);
        }
        if (account.state() == AccountState.FROZEN) {
            return new ChargeResultWithAccount(account, ChargeResult.ACCOUNT_FROZEN);
        }
        Set<String> newProcessed = new HashSet<>(account.processedChargeIds());
        newProcessed.add(chargeId);
        Account newAccount = new Account(account.balanceMinorUnits() - amount, account.state(), account.epoch(), newProcessed);
        return new ChargeResultWithAccount(newAccount, ChargeResult.APPLIED);
    }

    public record ChargeResultWithAccount(Account account, ChargeResult result) {
    }
}