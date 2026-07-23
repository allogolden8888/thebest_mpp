"""
Billing account_state — ACTIVE <-> FROZEN с fencing по account_epoch.

Формализует то, что в HLD §15.3/§15.5 было описано текстом (Redis Lua-скрипт,
fenced CAS recovery), но не как явную модель с доказанным свойством:
операция списания, "зависшая в полёте" во время freeze, обязана быть
отклонена по несовпадению epoch — это единственная причина существования
account_epoch, стоит явно доказать, что она работает, а не считать очевидным.

Production-реализация — Billing Redis Lua/Redis Function
(services_specifictaion.md §2.6), не этот файл.
"""

from dataclasses import dataclass, replace
from enum import Enum, auto


class AccountState(Enum):
    ACTIVE = auto()
    FROZEN = auto()


class ChargeResult(Enum):
    APPLIED = auto()
    ALREADY_PROCESSED = auto()
    ACCOUNT_FROZEN = auto()
    STALE_EPOCH = auto()


@dataclass(frozen=True)
class Account:
    balance: int  # minor units
    state: AccountState
    epoch: int
    processed_charge_ids: frozenset[str]


def freeze(account: Account) -> Account:
    """Идемпотентно: заморозка уже замороженного счёта не двигает epoch дальше."""
    if account.state == AccountState.FROZEN:
        return account
    return replace(account, state=AccountState.FROZEN, epoch=account.epoch + 1)


def unfreeze(account: Account, recomputed_balance: int, expected_epoch: int) -> Account:
    """Fenced CAS: unfreeze применяется только если epoch не уехал дальше,
    пока считался recomputed_balance (иначе кто-то другой уже разморозил/
    заморозил счёт заново, и наш recompute устарел)."""
    if account.state == AccountState.ACTIVE:
        return account  # уже разморожен
    if account.epoch != expected_epoch:
        raise ValueError(
            f"Fenced CAS отклонён: epoch уехал с {expected_epoch} на {account.epoch} "
            f"пока считался recomputed_balance — recovery должен пересчитать заново"
        )
    return replace(
        account, state=AccountState.ACTIVE, epoch=account.epoch + 1, balance=recomputed_balance
    )


def apply_charge(account: Account, charge_id: str, amount: int, expected_epoch: int) -> tuple[Account, ChargeResult]:
    """Соответствует Lua-скрипту apply_atomic_charge (HLD §15.2): все три
    проверки — account_state, account_epoch, charge_id dedup — атомарны
    в реальном Redis Function, здесь смоделированы как одна функция."""
    if charge_id in account.processed_charge_ids:
        return account, ChargeResult.ALREADY_PROCESSED
    if account.epoch != expected_epoch:
        return account, ChargeResult.STALE_EPOCH
    if account.state == AccountState.FROZEN:
        return account, ChargeResult.ACCOUNT_FROZEN
    new_account = replace(
        account,
        balance=account.balance - amount,
        processed_charge_ids=account.processed_charge_ids | {charge_id},
    )
    return new_account, ChargeResult.APPLIED


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------

def _fresh(balance: int = 100_000) -> Account:
    return Account(balance=balance, state=AccountState.ACTIVE, epoch=0, processed_charge_ids=frozenset())


def test_normal_charge_applies():
    acc = _fresh()
    acc, result = apply_charge(acc, "charge-1", 1_000, expected_epoch=0)
    assert result == ChargeResult.APPLIED
    assert acc.balance == 99_000


def test_duplicate_charge_id_is_idempotent():
    acc = _fresh()
    acc, r1 = apply_charge(acc, "charge-1", 1_000, expected_epoch=0)
    acc, r2 = apply_charge(acc, "charge-1", 1_000, expected_epoch=0)
    assert r1 == ChargeResult.APPLIED
    assert r2 == ChargeResult.ALREADY_PROCESSED
    assert acc.balance == 99_000  # списано только один раз


def test_charge_rejected_while_frozen():
    acc = freeze(_fresh())
    acc, result = apply_charge(acc, "charge-1", 1_000, expected_epoch=1)
    assert result == ChargeResult.ACCOUNT_FROZEN
    assert acc.balance == 100_000  # не изменился


def test_in_flight_charge_rejected_by_stale_epoch_race():
    """Ключевое свойство fencing: charge, который успел прочитать epoch=0
    ДО freeze, но пытается применить операцию ПОСЛЕ freeze (epoch уже 1),
    обязан быть отклонён — а не списать деньги с только что замороженного
    счёта."""
    acc = _fresh()  # epoch=0
    epoch_seen_by_inflight_charge = acc.epoch  # 0, "прочитано" до freeze

    acc = freeze(acc)  # epoch теперь 1, счёт заморожен
    assert acc.epoch == 1

    # Операция, стартовавшая при старом epoch, приходит и пытается закоммититься.
    acc, result = apply_charge(acc, "in-flight-charge", 5_000, expected_epoch=epoch_seen_by_inflight_charge)
    assert result == ChargeResult.STALE_EPOCH
    assert acc.balance == 100_000, "деньги не должны были списаться со свежезамороженного счёта"


def test_freeze_is_idempotent_does_not_double_bump_epoch():
    acc = freeze(_fresh())
    assert acc.epoch == 1
    acc = freeze(acc)
    assert acc.epoch == 1, "повторный freeze не должен двигать epoch дальше"


def test_fenced_unfreeze_success_path():
    acc = freeze(_fresh(balance=100_000))
    assert acc.state == AccountState.FROZEN and acc.epoch == 1

    # Reconciliation считает recomputed_balance по ledger, ожидая epoch=1.
    recomputed = 97_500
    acc = unfreeze(acc, recomputed_balance=recomputed, expected_epoch=1)
    assert acc.state == AccountState.ACTIVE
    assert acc.balance == recomputed
    assert acc.epoch == 2


def test_fenced_unfreeze_rejects_if_epoch_moved_during_recompute():
    """Если пока Reconciliation считал recomputed_balance, счёт успели
    разморозить/заморозить кем-то ещё (epoch уехал), unfreeze должен
    провалиться, а не тихо применить устаревший баланс поверх новых данных."""
    acc = freeze(_fresh())
    assert acc.epoch == 1

    # Кто-то другой успел разморозить и заморозить счёт заново, пока мы считали.
    acc = unfreeze(acc, recomputed_balance=99_000, expected_epoch=1)  # epoch -> 2, ACTIVE
    acc = freeze(acc)  # epoch -> 3, FROZEN снова

    try:
        unfreeze(acc, recomputed_balance=123_456, expected_epoch=1)  # наш старый expected_epoch
        assert False, "должно было отклонить unfreeze с устаревшим expected_epoch"
    except ValueError:
        pass


def test_charge_after_successful_unfreeze_uses_new_epoch():
    acc = freeze(_fresh())
    acc = unfreeze(acc, recomputed_balance=97_500, expected_epoch=1)
    assert acc.epoch == 2
    acc, result = apply_charge(acc, "charge-after-recovery", 500, expected_epoch=2)
    assert result == ChargeResult.APPLIED
    assert acc.balance == 97_000


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
