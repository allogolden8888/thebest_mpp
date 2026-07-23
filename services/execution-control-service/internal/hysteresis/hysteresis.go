// Package hysteresis — перенос 1:1 с state_machines/execution_control_hysteresis.py
// (5/5 тестов, HLD §8.3). Алгоритм там уже спроектирован и провалидирован на
// синтетических временных рядах — здесь только перевод на Go, без изменения
// логики. Инварианты (см. исходный докстринг):
//
//  1. Шум вокруг порога не вызывает flapping (min_state_duration +
//     confirmation window гасят одиночные всплески).
//  2. Устойчивая деградация переводит ACTIVE -> DEGRADED -> PAUSED.
//  3. Выход из PAUSED идёт только через DEGRADED, не сразу в ACTIVE.
//  4. ACTIVE может уйти сразу в PAUSED при резком тяжёлом сбое, минуя
//     DEGRADED (путь вниз быстрый при катастрофе, путь вверх — всегда
//     через DEGRADED, осторожный).
package hysteresis

// State — состояние scope в контроле выполнения.
type State int

const (
	StateActive State = iota
	StateDegraded
	StatePaused
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "ACTIVE"
	case StateDegraded:
		return "DEGRADED"
	case StatePaused:
		return "PAUSED"
	default:
		return "UNKNOWN"
	}
}

// Thresholds — пороги входа/выхода и окна подтверждения, как в Python-версии.
type Thresholds struct {
	EnterDegraded                float64
	ExitDegraded                 float64
	EnterPaused                  float64
	ExitPaused                   float64
	EnterConfirmationWindowTicks int
	ExitConfirmationWindowTicks  int
	MinStateDurationTicks        int
}

// HistoryEntry — один тик истории (tick_no, metric, state).
type HistoryEntry struct {
	TickNo int
	Metric float64
	State  State
}

// ControlLoop — держатель состояния гистерезиса для одного scope.
type ControlLoop struct {
	Thresholds      Thresholds
	State           State
	TicksInState    int
	candidate       *State
	CandidateStreak int
	History         []HistoryEntry
}

// NewControlLoop создаёт control loop в состоянии ACTIVE — как
// ControlLoop(thresholds=...) в Python (state по умолчанию State.ACTIVE).
func NewControlLoop(t Thresholds) *ControlLoop {
	return &ControlLoop{Thresholds: t, State: StateActive}
}

// Candidate — текущий кандидат на переход, nil если его нет (эквивалент
// Python candidate: State | None).
func (c *ControlLoop) Candidate() (State, bool) {
	if c.candidate == nil {
		return 0, false
	}
	return *c.candidate, true
}

func (c *ControlLoop) suggest(metric float64) State {
	t := c.Thresholds
	switch c.State {
	case StateActive:
		if metric >= t.EnterPaused {
			return StatePaused // катастрофа — можно сразу в PAUSED, минуя DEGRADED
		}
		if metric >= t.EnterDegraded {
			return StateDegraded
		}
		return StateActive
	case StateDegraded:
		if metric >= t.EnterPaused {
			return StatePaused
		}
		if metric <= t.ExitDegraded {
			return StateActive
		}
		return StateDegraded
	case StatePaused:
		if metric <= t.ExitPaused {
			return StateDegraded // выход всегда через DEGRADED, не сразу ACTIVE
		}
		return StatePaused
	default:
		panic("unreachable state")
	}
}

// scopeOrder — порядок состояний для определения направления перехода
// (isRecovery в Python).
var scopeOrder = map[State]int{StateActive: 0, StateDegraded: 1, StatePaused: 2}

func isRecovery(current, suggested State) bool {
	return scopeOrder[suggested] < scopeOrder[current]
}

// Tick подаёт очередное значение метрики и возвращает подтверждённое (или
// неизменное) состояние — прямой перенос ControlLoop.tick из Python.
func (c *ControlLoop) Tick(tickNo int, metric float64) State {
	c.TicksInState++
	suggested := c.suggest(metric)

	if suggested == c.State {
		c.candidate = nil
		c.CandidateStreak = 0
		c.History = append(c.History, HistoryEntry{tickNo, metric, c.State})
		return c.State
	}

	if c.candidate != nil && *c.candidate == suggested {
		c.CandidateStreak++
	} else {
		s := suggested
		c.candidate = &s
		c.CandidateStreak = 1
	}

	requiredWindow := c.Thresholds.EnterConfirmationWindowTicks
	if isRecovery(c.State, suggested) {
		requiredWindow = c.Thresholds.ExitConfirmationWindowTicks
	}

	if c.CandidateStreak >= requiredWindow && c.TicksInState >= c.Thresholds.MinStateDurationTicks {
		c.State = suggested
		c.TicksInState = 0
		c.candidate = nil
		c.CandidateStreak = 0
	}

	c.History = append(c.History, HistoryEntry{tickNo, metric, c.State})
	return c.State
}
