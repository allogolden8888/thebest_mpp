package hysteresis

import "testing"

// standardThresholds — те же значения, что STANDARD в
// state_machines/execution_control_hysteresis.py.
var standardThresholds = Thresholds{
	EnterDegraded:                0.5,
	ExitDegraded:                 0.3,
	EnterPaused:                  0.8,
	ExitPaused:                   0.5,
	EnterConfirmationWindowTicks: 3,
	ExitConfirmationWindowTicks:  5,
	MinStateDurationTicks:        5,
}

// TestNoiseAroundThresholdDoesNotFlap — метрика колеблется вокруг
// enter_degraded (0.5), то чуть выше, то чуть ниже, ни разу не задерживаясь
// достаточно долго. Ожидание: остаётся ACTIVE, не осциллирует.
func TestNoiseAroundThresholdDoesNotFlap(t *testing.T) {
	loop := NewControlLoop(standardThresholds)
	noisySeries := repeatFloats([]float64{0.45, 0.55, 0.48, 0.52, 0.49, 0.51, 0.47, 0.53, 0.50, 0.46}, 3)

	states := make([]State, len(noisySeries))
	for i, m := range noisySeries {
		states[i] = loop.Tick(i, m)
	}

	transitions := 0
	for i := 1; i < len(states); i++ {
		if states[i] != states[i-1] {
			transitions++
		}
	}
	if transitions != 0 {
		t.Fatalf("ожидали 0 переходов на шуме, получили %d: %v", transitions, states)
	}
	if loop.State != StateActive {
		t.Fatalf("ожидали ACTIVE, получили %v", loop.State)
	}
}

// TestSustainedDegradationEntersDegradedThenPaused — 3 тика подряд >= 0.5 ->
// DEGRADED (enter_confirmation_window=3), затем держим на 0.6 (между
// exit_degraded и enter_paused) — остаётся DEGRADED, затем устойчиво
// >= 0.8 -> PAUSED.
func TestSustainedDegradationEntersDegradedThenPaused(t *testing.T) {
	loop := NewControlLoop(standardThresholds)
	series := append(repeatFloats([]float64{0.6}, 10), repeatFloats([]float64{0.9}, 10)...)

	states := make([]State, len(series))
	for i, m := range series {
		states[i] = loop.Tick(i, m)
	}

	firstDegraded := indexOf(states, StateDegraded)
	firstPaused := indexOf(states, StatePaused)
	if firstDegraded == -1 {
		t.Fatalf("ожидали, что DEGRADED встретится в истории состояний: %v", states)
	}
	if firstPaused == -1 {
		t.Fatalf("ожидали, что PAUSED встретится в истории состояний: %v", states)
	}
	if states[len(states)-1] != StatePaused {
		t.Fatalf("ожидали финальное состояние PAUSED, получили %v", states[len(states)-1])
	}
	if !(firstDegraded < firstPaused) {
		t.Fatalf("DEGRADED должен наступить раньше PAUSED, не одновременно/в обход: degraded=%d paused=%d", firstDegraded, firstPaused)
	}
}

// TestRecoveryFromPausedGoesThroughDegradedNotStraightToActive — восстановление
// обязано пройти через DEGRADED, даже если метрика уже "здоровая".
func TestRecoveryFromPausedGoesThroughDegradedNotStraightToActive(t *testing.T) {
	loop := NewControlLoop(standardThresholds)
	series := append(repeatFloats([]float64{0.6}, 5), repeatFloats([]float64{0.9}, 5)...)
	for i, m := range series {
		loop.Tick(i, m)
	}
	if loop.State != StatePaused {
		t.Fatalf("ожидали PAUSED перед проверкой восстановления, получили %v", loop.State)
	}

	tickNo := 10
	var recovered State
	for i := 0; i < 20; i++ {
		recovered = loop.Tick(tickNo, 0.1)
		tickNo++
		if recovered != StatePaused {
			break
		}
	}
	if recovered != StateDegraded {
		t.Fatalf("ожидали, что выход из PAUSED приземлится в DEGRADED, получили %v", recovered)
	}
}

// TestActiveCanJumpStraightToPausedOnCatastrophicSpike — катастрофа должна
// уйти в PAUSED напрямую, минуя DEGRADED.
func TestActiveCanJumpStraightToPausedOnCatastrophicSpike(t *testing.T) {
	loop := NewControlLoop(standardThresholds)
	if loop.State != StateActive {
		t.Fatalf("начальное состояние должно быть ACTIVE")
	}

	states := make([]State, 10)
	for i := 0; i < 10; i++ {
		states[i] = loop.Tick(i, 0.95)
	}

	if indexOf(states, StatePaused) == -1 {
		t.Fatalf("ожидали PAUSED в истории состояний: %v", states)
	}
	if indexOf(states, StateDegraded) != -1 {
		t.Fatalf("катастрофа должна была уйти в PAUSED напрямую, минуя DEGRADED: %v", states)
	}
}

// TestMinStateDurationBlocksImmediateReTransition — даже если метрика
// мгновенно и устойчиво ухудшается сразу после входа в состояние,
// min_state_duration не даёт немедленно уйти дальше — дополнительный
// анти-флаппинг барьер поверх confirmation window.
func TestMinStateDurationBlocksImmediateReTransition(t *testing.T) {
	shortMinDuration := Thresholds{
		EnterDegraded:                0.5,
		ExitDegraded:                 0.3,
		EnterPaused:                  0.8,
		ExitPaused:                   0.5,
		EnterConfirmationWindowTicks: 1, // ослаблено, чтобы изолировать эффект min_state_duration
		ExitConfirmationWindowTicks:  1,
		MinStateDurationTicks:        4,
	}
	loop := NewControlLoop(shortMinDuration)

	// Тик 0: 0.6 -> предложение DEGRADED, confirmation_window=1 удовлетворён
	// сразу, но ticks_in_state (для ACTIVE) тоже должен быть >= min_state_duration.
	// Первый тик: ticks_in_state=1 < 4 — переход блокируется.
	s0 := loop.Tick(0, 0.6)
	if s0 != StateActive {
		t.Fatalf("первый же тик не должен мгновенно переключать состояние, получили %v", s0)
	}

	states := make([]State, 5)
	for i := 1; i <= 5; i++ {
		states[i-1] = loop.Tick(i, 0.6)
	}
	if indexOf(states, StateDegraded) == -1 {
		t.Fatalf("ожидали, что после накопления min_state_duration переход в DEGRADED пройдёт: %v", states)
	}
}

func repeatFloats(series []float64, times int) []float64 {
	out := make([]float64, 0, len(series)*times)
	for i := 0; i < times; i++ {
		out = append(out, series...)
	}
	return out
}

func indexOf(states []State, target State) int {
	for i, s := range states {
		if s == target {
			return i
		}
	}
	return -1
}
