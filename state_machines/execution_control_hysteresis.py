"""
Execution Control — гистерезис ACTIVE/DEGRADED/PAUSED.

Исполняемая симуляция логики из HLD §8.3, которая раньше была только
параметрами (enter/exit thresholds, confirmation window, min_state_duration)
без формального алгоритма перехода. Здесь — конкретный алгоритм и прогон
против синтетических временных рядов метрики, доказывающий:

1. Шум вокруг порога НЕ вызывает flapping (min_state_duration + confirmation
   window гасят одиночные всплески).
2. Устойчивая деградация переводит в DEGRADED, затем в PAUSED.
3. Выход из PAUSED идёт только через DEGRADED, не сразу в ACTIVE (HLD §8.3).
4. ACTIVE может уйти сразу в PAUSED при резком тяжёлом сбое, минуя DEGRADED
   (асимметрия: путь вниз быстрый при катастрофе, путь вверх — всегда
   через DEGRADED, осторожный).

Production-реализация — Execution Control Service (Go,
services_specifictaion.md §4.1); этот файл — валидация алгоритма, не сам
сервис.
"""

from dataclasses import dataclass, field
from enum import Enum, auto


class State(Enum):
    ACTIVE = auto()
    DEGRADED = auto()
    PAUSED = auto()


@dataclass
class Thresholds:
    enter_degraded: float
    exit_degraded: float
    enter_paused: float
    exit_paused: float
    enter_confirmation_window_ticks: int  # сколько подряд тиков метрика должна держаться, чтобы применить переход
    exit_confirmation_window_ticks: int
    min_state_duration_ticks: int


@dataclass
class ControlLoop:
    thresholds: Thresholds
    state: State = State.ACTIVE
    ticks_in_state: int = 0
    candidate: State | None = None
    candidate_streak: int = 0
    history: list[tuple[int, float, State]] = field(default_factory=list)

    def _suggest(self, metric: float) -> State:
        t = self.thresholds
        if self.state == State.ACTIVE:
            if metric >= t.enter_paused:
                return State.PAUSED  # катастрофа — можно сразу в PAUSED, минуя DEGRADED
            if metric >= t.enter_degraded:
                return State.DEGRADED
            return State.ACTIVE
        if self.state == State.DEGRADED:
            if metric >= t.enter_paused:
                return State.PAUSED
            if metric <= t.exit_degraded:
                return State.ACTIVE
            return State.DEGRADED
        if self.state == State.PAUSED:
            if metric <= t.exit_paused:
                return State.DEGRADED  # выход всегда через DEGRADED, не сразу ACTIVE
            return State.PAUSED
        raise AssertionError("unreachable")

    def tick(self, tick_no: int, metric: float) -> State:
        self.ticks_in_state += 1
        suggested = self._suggest(metric)

        if suggested == self.state:
            self.candidate = None
            self.candidate_streak = 0
            self.history.append((tick_no, metric, self.state))
            return self.state

        # Предлагается смена состояния — считаем streak.
        if suggested == self.candidate:
            self.candidate_streak += 1
        else:
            self.candidate = suggested
            self.candidate_streak = 1

        required_window = (
            self.thresholds.exit_confirmation_window_ticks
            if _is_recovery(self.state, suggested)
            else self.thresholds.enter_confirmation_window_ticks
        )

        if (
            self.candidate_streak >= required_window
            and self.ticks_in_state >= self.thresholds.min_state_duration_ticks
        ):
            self.state = suggested
            self.ticks_in_state = 0
            self.candidate = None
            self.candidate_streak = 0

        self.history.append((tick_no, metric, self.state))
        return self.state


def _is_recovery(current: State, suggested: State) -> bool:
    order = {State.ACTIVE: 0, State.DEGRADED: 1, State.PAUSED: 2}
    return order[suggested] < order[current]


# ---------------------------------------------------------------------------
# Тесты — на синтетических временных рядах.
# ---------------------------------------------------------------------------

STANDARD = Thresholds(
    enter_degraded=0.5,
    exit_degraded=0.3,
    enter_paused=0.8,
    exit_paused=0.5,
    enter_confirmation_window_ticks=3,
    exit_confirmation_window_ticks=5,
    min_state_duration_ticks=5,
)


def test_noise_around_threshold_does_not_flap():
    """Метрика колеблется вокруг enter_degraded (0.5), то чуть выше, то чуть
    ниже, ни разу не задерживаясь достаточно долго. Ожидание: остаётся ACTIVE,
    не осциллирует."""
    loop = ControlLoop(thresholds=STANDARD)
    noisy_series = [0.45, 0.55, 0.48, 0.52, 0.49, 0.51, 0.47, 0.53, 0.50, 0.46] * 3
    states = [loop.tick(i, m) for i, m in enumerate(noisy_series)]
    transitions = sum(1 for a, b in zip(states, states[1:]) if a != b)
    assert transitions == 0, f"ожидали 0 переходов на шуме, получили {transitions}: {states}"
    assert loop.state == State.ACTIVE


def test_sustained_degradation_enters_degraded_then_paused():
    loop = ControlLoop(thresholds=STANDARD)
    # 3 тика подряд >= 0.5 -> DEGRADED (enter_confirmation_window=3),
    # затем держим на 0.6 (между exit_degraded и enter_paused) — остаётся DEGRADED,
    # затем устойчиво >= 0.8 -> PAUSED.
    series = [0.6] * 10 + [0.9] * 10
    states = [loop.tick(i, m) for i, m in enumerate(series)]
    assert State.DEGRADED in states
    assert states[-1] == State.PAUSED
    # DEGRADED должен наступить раньше PAUSED, не одновременно/в обход.
    first_degraded = states.index(State.DEGRADED)
    first_paused = states.index(State.PAUSED)
    assert first_degraded < first_paused


def test_recovery_from_paused_goes_through_degraded_not_straight_to_active():
    loop = ControlLoop(thresholds=STANDARD)
    # Загоняем в PAUSED.
    for i, m in enumerate([0.6] * 5 + [0.9] * 5):
        loop.tick(i, m)
    assert loop.state == State.PAUSED

    # Метрика падает сразу в зону ACTIVE (0.1) — но восстановление обязано
    # пройти через DEGRADED, даже если метрика уже "здоровая".
    tick_no = 10
    recovered_state = None
    for _ in range(20):
        recovered_state = loop.tick(tick_no, 0.1)
        tick_no += 1
        if recovered_state != State.PAUSED:
            break
    assert recovered_state == State.DEGRADED, (
        f"ожидали, что выход из PAUSED приземлится в DEGRADED, получили {recovered_state}"
    )


def test_active_can_jump_straight_to_paused_on_catastrophic_spike():
    loop = ControlLoop(thresholds=STANDARD)
    assert loop.state == State.ACTIVE
    states = [loop.tick(i, 0.95) for i in range(10)]
    assert State.PAUSED in states
    assert State.DEGRADED not in states, "катастрофа должна была уйти в PAUSED напрямую, минуя DEGRADED"


def test_min_state_duration_blocks_immediate_re_transition():
    """Даже если метрика мгновенно и устойчиво ухудшается сразу после входа
    в состояние, min_state_duration не даёт немедленно уйти дальше —
    это дополнительный анти-флаппинг барьер поверх confirmation window."""
    short_min_duration = Thresholds(
        enter_degraded=0.5,
        exit_degraded=0.3,
        enter_paused=0.8,
        exit_paused=0.5,
        enter_confirmation_window_ticks=1,  # специально ослаблено, чтобы изолировать эффект min_state_duration
        exit_confirmation_window_ticks=1,
        min_state_duration_ticks=4,
    )
    loop = ControlLoop(thresholds=short_min_duration)
    # Тик 0: 0.6 -> предложение DEGRADED, confirmation_window=1 удовлетворён сразу,
    # но ticks_in_state (для ACTIVE) тоже должен быть >= min_state_duration.
    # Поскольку это самый первый тик, ticks_in_state=1 < 4 — переход блокируется.
    s0 = loop.tick(0, 0.6)
    assert s0 == State.ACTIVE, "первый же тик не должен мгновенно переключать состояние"
    # После накопления min_state_duration тиков в ACTIVE переход должен пройти.
    states = [loop.tick(i, 0.6) for i in range(1, 6)]
    assert State.DEGRADED in states


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = 0
    for t in tests:
        t()
        passed += 1
        print(f"PASS  {t.__name__}")
    print(f"\n{passed}/{len(tests)} тестов прошли")
