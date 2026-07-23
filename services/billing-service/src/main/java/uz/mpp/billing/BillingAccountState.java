package uz.mpp.billing;

import java.util.HashSet;
import java.util.Set;

/**
 * Порт {@code state_machines/billing_account_state.py} на Java — тот же
 * алгоритм 1:1: двухсостояниевая машина ACTIVE/FROZEN с fencing по
 * {@code account_epoch}, идемпотентность по {@code charge_id}. Ключевое
 * свойство (fencing защищает от списания "в полёте" во время freeze)
 * доказано тестами в Python-версии и повторно доказано здесь в
 * {@code BillingAccountStateTest} — не предполагается, что порт корректен
 * просто потому, что похож на оригинал.
 *
 * <p>Production-реализация — Billing Redis Lua/Redis Function
 * ({@code service_internal_methods.md} §1.6, {@code apply_atomic_charge}),
 * не этот класс — здесь модель, которую Lua-скрипт обязан воспроизводить
 * атомарно (development_plan.md 4.2, ещё не написан).
 */
public final class BillingAccountState {

    private BillingAccountState() {
    }

    public enum AccountState {
        ACTIVE, FROZEN
    }

    public enum ChargeOutcome {
        APPLIED, ALREADY_PROCESSED, ACCOUNT_FROZEN, STALE_EPOCH
    }

    public record Account(long balance, AccountState state, long epoch, Set<String> processedChargeIds) {
        public static Account fresh(long balance) {
            return new Account(balance, AccountState.ACTIVE, 0, Set.of());
        }
    }

    public record ChargeResult(Account account, ChargeOutcome outcome) {
    }

    /** Идемпотентно: заморозка уже замороженного счёта не двигает epoch дальше. */
    public static Account freeze(Account account) {
        if (account.state() == AccountState.FROZEN) {
            return account;
        }
        return new Account(account.balance(), AccountState.FROZEN, account.epoch() + 1, account.processedChargeIds());
    }

    /**
     * Fenced CAS: unfreeze применяется только если epoch не уехал дальше,
     * пока считался recomputedBalance (иначе кто-то другой уже разморозил/
     * заморозил счёт заново, и recompute устарел).
     */
    public static Account unfreeze(Account account, long recomputedBalance, long expectedEpoch) {
        if (account.state() == AccountState.ACTIVE) {
            return account;
        }
        if (account.epoch() != expectedEpoch) {
            throw new StaleEpochException(
                "Fenced CAS отклонён: epoch уехал с %d на %d пока считался recomputedBalance — recovery должен пересчитать заново"
                    .formatted(expectedEpoch, account.epoch()));
        }
        return new Account(recomputedBalance, AccountState.ACTIVE, account.epoch() + 1, account.processedChargeIds());
    }

    public static final class StaleEpochException extends RuntimeException {
        public StaleEpochException(String message) {
            super(message);
        }
    }

    /**
     * Соответствует Lua-скрипту {@code apply_atomic_charge}: все три
     * проверки — account_state, account_epoch, charge_id dedup — атомарны
     * в реальном Redis Function, здесь смоделированы как один метод.
     */
    public static ChargeResult applyCharge(Account account, String chargeId, long amount, long expectedEpoch) {
        if (account.processedChargeIds().contains(chargeId)) {
            return new ChargeResult(account, ChargeOutcome.ALREADY_PROCESSED);
        }
        if (account.epoch() != expectedEpoch) {
            return new ChargeResult(account, ChargeOutcome.STALE_EPOCH);
        }
        if (account.state() == AccountState.FROZEN) {
            return new ChargeResult(account, ChargeOutcome.ACCOUNT_FROZEN);
        }
        Set<String> updatedIds = new HashSet<>(account.processedChargeIds());
        updatedIds.add(chargeId);
        Account updated = new Account(account.balance() - amount, account.state(), account.epoch(), Set.copyOf(updatedIds));
        return new ChargeResult(updated, ChargeOutcome.APPLIED);
    }
}
