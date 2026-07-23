package hysteresis

// RampSteps — фиксированная последовательность controlled ramp-up после
// PAUSED/DEGRADED (service_internal_methods.md §3.1 `compute_ramp_step`:
// "0→5→10→25→50→100%"), в долях от номинальной пропускной способности.
var RampSteps = []float64{0.0, 0.05, 0.10, 0.25, 0.50, 1.00}

// ComputeRampStep возвращает следующий шаг ramp-up после currentRate.
// Если состояние не ACTIVE (ramp-up идёт только при подтверждённом
// восстановлении), возвращает текущую ставку без изменений — вызывающая
// сторона не должна повышать rate, пока scope не вышел из DEGRADED/PAUSED.
// Если currentRate уже на последнем шаге (1.0) или выше, дальше повышать
// некуда — возвращается 1.0.
func ComputeRampStep(state State, currentRate float64) float64 {
	if state != StateActive {
		return currentRate
	}
	for _, step := range RampSteps {
		if step > currentRate {
			return step
		}
	}
	return RampSteps[len(RampSteps)-1]
}
