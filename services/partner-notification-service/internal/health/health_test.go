package health

import "testing"

func TestReadyRequiresBaseLatch(t *testing.T) {
	state := &State{}
	state.SetDependencyCheck(func() bool { return true })
	if state.Ready() {
		t.Fatal("state must not be ready before startup latch")
	}
}

func TestReadyTracksLiveDependency(t *testing.T) {
	state := &State{}
	state.SetReady(true)
	healthy := false
	state.SetDependencyCheck(func() bool { return healthy })
	if state.Ready() {
		t.Fatal("unhealthy config mirror must make readiness false")
	}
	healthy = true
	if !state.Ready() {
		t.Fatal("healthy dependency must restore readiness")
	}
}

func TestReadyWithoutDependencyUsesBaseLatch(t *testing.T) {
	state := &State{}
	state.SetReady(true)
	if !state.Ready() {
		t.Fatal("explicit file mode has no live config dependency")
	}
}
