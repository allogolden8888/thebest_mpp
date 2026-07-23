package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzAlwaysOk(t *testing.T) {
	srv := httptest.NewServer(Router(&State{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("request failed: %v", err)
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
	resp, _ := http.Get(srv.URL + "/readyz")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ожидали 503, получили %d", resp.StatusCode)
	}
	state.SetReady(true)
	resp2, _ := http.Get(srv.URL + "/readyz")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp2.StatusCode)
	}
}

func TestMetricsReturns200(t *testing.T) {
	srv := httptest.NewServer(Router(&State{}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ожидали 200, получили %d", resp.StatusCode)
	}
}
