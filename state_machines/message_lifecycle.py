"""
Message State Resolver — валидация переходов message.lifecycle.

Это исполняемая проверка таблицы допустимых переходов, не production-код
(Message State Resolver — Java/Kafka Streams, service_internal_methods.md §2.4).
Логика здесь должна быть перенесена 1:1 в `validate_transition` на Java —
таблица переходов и есть контракт, тесты ниже доказывают, что таблица
самосогласована и покрывает случаи, явно упомянутые в HLD §10.

Значения статусов соответствуют MessageLifecycleStatus
(platform-contracts/common/enums.proto).
"""

from dataclasses import dataclass
from enum import Enum, auto


class Status(Enum):
    SUBMITTED = auto()
    DELIVERED = auto()
    UNDELIVERABLE = auto()
    DELIVERY_UNRESOLVED = auto()
    LATE_DELIVERY_CONFIRMED = auto()
    REJECTED = auto()
    FAILED = auto()
    SYSTEM_UNAVAILABLE = auto()


class Verdict(Enum):
    VALID = auto()
    REGRESSION = auto()
    DUPLICATE = auto()


# Терминальные статусы — не принимают дальнейших переходов, КРОМЕ явно
# перечисленных исключений в ALLOWED_FROM_TERMINAL ниже.
TERMINAL = {
    Status.DELIVERED,
    Status.UNDELIVERABLE,
    Status.REJECTED,
    Status.FAILED,
    Status.SYSTEM_UNAVAILABLE,
    Status.DELIVERY_UNRESOLVED,  # терминален, кроме одного исключения
    Status.LATE_DELIVERY_CONFIRMED,
}

# Допустимые записи для сообщения, у которого ещё нет статуса (первое
# событие в его жизни). Не всегда SUBMITTED — Reconciliation может дать
# первый статус напрямую (SUBMISSION_OUTCOME_UNKNOWN никогда не проходил
# через чистый SUBMITTED).
VALID_ENTRY_POINTS = {
    Status.SUBMITTED,
    Status.REJECTED,
    Status.FAILED,
    Status.SYSTEM_UNAVAILABLE,
    Status.DELIVERED,
    Status.UNDELIVERABLE,
    Status.DELIVERY_UNRESOLVED,
}

# Обычные (не из терминального состояния) переходы.
ALLOWED_TRANSITIONS = {
    Status.SUBMITTED: {Status.DELIVERED, Status.UNDELIVERABLE},
}

# Единственное исключение из "терминальное состояние = конец": поздний DLR
# после DELIVERY_UNRESOLVED (HLD §10). Явно НЕ включает DELIVERED/UNDELIVERABLE
# — решение зафиксировано осознанно (см. state_machines.md): поздний DLR,
# противоречащий уже вынесенному DELIVERED/UNDELIVERABLE, считается аномалией
# и логируется, но не применяется как новая lifecycle-версия.
ALLOWED_FROM_TERMINAL = {
    Status.DELIVERY_UNRESOLVED: {Status.LATE_DELIVERY_CONFIRMED},
}


@dataclass
class LifecycleState:
    status: Status | None
    lifecycle_version: int
    last_applied_event_id: str | None


def validate_transition(
    current: LifecycleState, candidate_status: Status, event_id: str
) -> Verdict:
    if event_id == current.last_applied_event_id:
        return Verdict.DUPLICATE

    if current.status is None:
        return Verdict.VALID if candidate_status in VALID_ENTRY_POINTS else Verdict.REGRESSION

    if current.status in TERMINAL:
        allowed = ALLOWED_FROM_TERMINAL.get(current.status, set())
        return Verdict.VALID if candidate_status in allowed else Verdict.REGRESSION

    allowed = ALLOWED_TRANSITIONS.get(current.status, set())
    return Verdict.VALID if candidate_status in allowed else Verdict.REGRESSION


def apply(current: LifecycleState, candidate_status: Status, event_id: str) -> LifecycleState:
    verdict = validate_transition(current, candidate_status, event_id)
    if verdict == Verdict.DUPLICATE:
        return current  # без изменений, но не ошибка
    if verdict == Verdict.REGRESSION:
        raise ValueError(
            f"Недопустимый переход: {current.status} -> {candidate_status} "
            f"(event_id={event_id})"
        )
    return LifecycleState(
        status=candidate_status,
        lifecycle_version=current.lifecycle_version + 1,
        last_applied_event_id=event_id,
    )


# ---------------------------------------------------------------------------
# Тесты — реально запускаются, не просто описание.
# ---------------------------------------------------------------------------

def _fresh() -> LifecycleState:
    return LifecycleState(status=None, lifecycle_version=0, last_applied_event_id=None)


def test_all_entry_points_valid():
    for status in VALID_ENTRY_POINTS:
        s = apply(_fresh(), status, "e1")
        assert s.status == status
        assert s.lifecycle_version == 1


def test_submitted_to_delivered_valid():
    s = apply(_fresh(), Status.SUBMITTED, "e1")
    s = apply(s, Status.DELIVERED, "e2")
    assert s.status == Status.DELIVERED
    assert s.lifecycle_version == 2


def test_submitted_to_undeliverable_valid():
    s = apply(_fresh(), Status.SUBMITTED, "e1")
    s = apply(s, Status.UNDELIVERABLE, "e2")
    assert s.status == Status.UNDELIVERABLE


def test_delivered_to_undeliverable_illegal():
    """Явный пример недопустимого перехода из HLD §10."""
    s = apply(_fresh(), Status.SUBMITTED, "e1")
    s = apply(s, Status.DELIVERED, "e2")
    try:
        apply(s, Status.UNDELIVERABLE, "e3")
        assert False, "должно было отклонить переход DELIVERED -> UNDELIVERABLE"
    except ValueError:
        pass


def test_undeliverable_to_delivered_illegal_by_symmetry():
    s = apply(_fresh(), Status.SUBMITTED, "e1")
    s = apply(s, Status.UNDELIVERABLE, "e2")
    try:
        apply(s, Status.DELIVERED, "e3")
        assert False, "должно было отклонить переход UNDELIVERABLE -> DELIVERED"
    except ValueError:
        pass


def test_delivery_unresolved_to_late_confirmed_valid_exception():
    s = apply(_fresh(), Status.DELIVERY_UNRESOLVED, "e1")
    s = apply(s, Status.LATE_DELIVERY_CONFIRMED, "e2")
    assert s.status == Status.LATE_DELIVERY_CONFIRMED
    assert s.lifecycle_version == 2


def test_delivery_unresolved_to_anything_else_illegal():
    for bad_target in Status:
        if bad_target in (Status.DELIVERY_UNRESOLVED, Status.LATE_DELIVERY_CONFIRMED):
            continue
        s = apply(_fresh(), Status.DELIVERY_UNRESOLVED, "e1")
        try:
            apply(s, bad_target, "e2")
            assert False, f"должно было отклонить DELIVERY_UNRESOLVED -> {bad_target}"
        except ValueError:
            pass


def _reach(status: Status) -> LifecycleState:
    """Достигает status легитимным путём — не все терминальные статусы
    достижимы как entry point (например LATE_DELIVERY_CONFIRMED — только
    через DELIVERY_UNRESOLVED, это и есть предмет проверки ниже)."""
    if status in VALID_ENTRY_POINTS:
        return apply(_fresh(), status, "e1")
    if status == Status.LATE_DELIVERY_CONFIRMED:
        s = apply(_fresh(), Status.DELIVERY_UNRESOLVED, "e1")
        return apply(s, Status.LATE_DELIVERY_CONFIRMED, "e2")
    raise AssertionError(f"нет известного пути до {status} — тест нужно дополнить")


def test_all_fully_terminal_states_reject_everything():
    """Каждый полностью терминальный статус (без исключений) должен
    отклонять абсолютно любой следующий переход — независимо от того,
    каким путём до него дошли."""
    fully_terminal = TERMINAL - set(ALLOWED_FROM_TERMINAL.keys())
    assert fully_terminal, "список полностью терминальных статусов не должен быть пустым"
    for term_status in fully_terminal:
        for any_target in Status:
            s = _reach(term_status)
            try:
                apply(s, any_target, "e99")
                assert False, f"должно было отклонить {term_status} -> {any_target}"
            except ValueError:
                pass


def test_duplicate_event_id_is_noop_not_error():
    s = apply(_fresh(), Status.SUBMITTED, "e1")
    s2 = apply(s, Status.DELIVERED, "e1")  # тот же event_id, что уже применён
    assert s2.status == Status.SUBMITTED  # не применилось
    assert s2.lifecycle_version == 1


def test_reconciliation_can_enter_directly_without_submitted():
    """SUBMISSION_OUTCOME_UNKNOWN никогда не был SUBMITTED — Reconciliation
    даёт первый статус напрямую (HLD §13)."""
    for status in (Status.DELIVERED, Status.UNDELIVERABLE, Status.DELIVERY_UNRESOLVED):
        s = apply(_fresh(), status, "e1")
        assert s.status == status


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
