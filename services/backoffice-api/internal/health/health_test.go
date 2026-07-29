package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzAlwaysOk(t *testing.T) {
	state := &State{}
	srv := httptest.NewServer(Router(state))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
}

func TestReadyz503UntilReady(t *testing.T) {
	state := &State{}
	srv := httptest.NewServer(Router(state))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ожидали 503 до готовности, получили %d", resp.StatusCode)
	}

	state.SetReady(true)
	resp2, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200 после готовности, получили %d", resp2.StatusCode)
	}
}

func TestReadyz503WhenDependencyCheckFails(t *testing.T) {
	state := &State{}
	state.SetReady(true)
	state.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres": func(ctx context.Context) error { return nil },
		"kafka":    func(ctx context.Context) error { return errors.New("dial tcp: connection refused") },
	})
	srv := httptest.NewServer(Router(state))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ожидали 503 при неудачном dependency check, получили %d", resp.StatusCode)
	}
}

func TestReadyz200WhenAllDependencyChecksPass(t *testing.T) {
	state := &State{}
	state.SetReady(true)
	state.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres": func(ctx context.Context) error { return nil },
		"kafka":    func(ctx context.Context) error { return nil },
	})
	srv := httptest.NewServer(Router(state))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, когда все dependency checks проходят, получили %d", resp.StatusCode)
	}
}

func TestMetricsReturns200(t *testing.T) {
	state := &State{}
	srv := httptest.NewServer(Router(state))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
}