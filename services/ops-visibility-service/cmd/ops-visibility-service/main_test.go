package main

import (
	"testing"
	"time"

	"mpp/ops-visibility-service/internal/services"
)

func TestEnvReturnsFallbackWhenUnset(t *testing.T) {
	t.Setenv("OPS_TEST_UNSET_VAR", "")
	if got := env("OPS_TEST_UNSET_VAR", "fallback"); got != "fallback" {
		t.Errorf("env() = %q, want %q", got, "fallback")
	}
}

func TestEnvReturnsValueWhenSet(t *testing.T) {
	t.Setenv("OPS_TEST_VAR", "value")
	if got := env("OPS_TEST_VAR", "fallback"); got != "value" {
		t.Errorf("env() = %q, want %q", got, "value")
	}
}

func TestEnvDurationParsesValidDuration(t *testing.T) {
	t.Setenv("OPS_TEST_DURATION", "45s")
	if got := envDuration("OPS_TEST_DURATION", time.Minute); got != 45*time.Second {
		t.Errorf("envDuration() = %v, want 45s", got)
	}
}

func TestEnvDurationFallsBackOnInvalidValue(t *testing.T) {
	t.Setenv("OPS_TEST_DURATION_BAD", "not-a-duration")
	if got := envDuration("OPS_TEST_DURATION_BAD", 20*time.Second); got != 20*time.Second {
		t.Errorf("envDuration() = %v, want fallback 20s", got)
	}
}

func TestEnvIntParsesValidInt(t *testing.T) {
	t.Setenv("OPS_TEST_INT", "12")
	if got := envInt("OPS_TEST_INT", 8); got != 12 {
		t.Errorf("envInt() = %d, want 12", got)
	}
}

func TestEnvIntFallsBackOnInvalidValue(t *testing.T) {
	t.Setenv("OPS_TEST_INT_BAD", "not-an-int")
	if got := envInt("OPS_TEST_INT_BAD", 8); got != 8 {
		t.Errorf("envInt() = %d, want fallback 8", got)
	}
}

func TestSplitCSVTrimsAndDropsEmpty(t *testing.T) {
	got := splitCSV(" foo , bar,,baz ")
	want := []string{"foo", "bar", "baz"}
	if len(got) != len(want) {
		t.Fatalf("splitCSV() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("splitCSV()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestResolveServiceListDefaultsToServicesList(t *testing.T) {
	t.Setenv("EXTRA_SERVICES", "")
	t.Setenv("SERVICES_OVERRIDE", "")

	got := resolveServiceList()
	if len(got) != len(services.List) {
		t.Fatalf("resolveServiceList() len = %d, want %d", len(got), len(services.List))
	}
}

func TestResolveServiceListAppendsExtraServices(t *testing.T) {
	t.Setenv("EXTRA_SERVICES", "test-only-service-a,test-only-service-b")
	t.Setenv("SERVICES_OVERRIDE", "")

	got := resolveServiceList()
	want := len(services.List) + 2
	if len(got) != want {
		t.Fatalf("resolveServiceList() len = %d, want %d", len(got), want)
	}
	found := map[string]bool{}
	for _, s := range got {
		found[s] = true
	}
	if !found["test-only-service-a"] || !found["test-only-service-b"] {
		t.Errorf("EXTRA_SERVICES не были добавлены: %v", got)
	}
}

func TestResolveServiceListOverrideReplacesDefaultList(t *testing.T) {
	t.Setenv("SERVICES_OVERRIDE", "only-this-one,and-this-one")
	t.Setenv("EXTRA_SERVICES", "should-be-ignored")

	got := resolveServiceList()
	want := []string{"only-this-one", "and-this-one"}
	if len(got) != len(want) {
		t.Fatalf("resolveServiceList() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("resolveServiceList()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildTargetsUsesPlatformConvention(t *testing.T) {
	targets := buildTargets([]string{"iam-service"})
	if len(targets) != 1 {
		t.Fatalf("buildTargets() len = %d, want 1", len(targets))
	}
	want := "http://iam-service.mpp.svc:9090/readyz"
	if targets[0].URL != want {
		t.Errorf("buildTargets()[0].URL = %q, want %q", targets[0].URL, want)
	}
	if targets[0].Service != "iam-service" {
		t.Errorf("buildTargets()[0].Service = %q, want %q", targets[0].Service, "iam-service")
	}
}
