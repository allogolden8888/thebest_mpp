package hysteresis

import "testing"

func TestComputeRampStepAdvancesOneStepAtATime(t *testing.T) {
	cases := []struct {
		current  float64
		expected float64
	}{
		{0.0, 0.05},
		{0.05, 0.10},
		{0.10, 0.25},
		{0.25, 0.50},
		{0.50, 1.00},
		{1.00, 1.00}, // уже на максимуме — дальше некуда
	}
	for _, c := range cases {
		got := ComputeRampStep(StateActive, c.current)
		if got != c.expected {
			t.Fatalf("ComputeRampStep(ACTIVE, %v) = %v, ожидали %v", c.current, got, c.expected)
		}
	}
}

func TestComputeRampStepDoesNotAdvanceOutsideActive(t *testing.T) {
	for _, s := range []State{StateDegraded, StatePaused} {
		got := ComputeRampStep(s, 0.10)
		if got != 0.10 {
			t.Fatalf("ramp-up не должен продвигаться в состоянии %v, получили %v вместо 0.10", s, got)
		}
	}
}
