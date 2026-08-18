package readyz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newFakeServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestPollAllMarksHealthyServiceReady(t *testing.T) {
	srv := newFakeServer(t, http.StatusOK)
	poller := NewPoller(2*time.Second, 4)

	snap := poller.PollAll(context.Background(), []Target{{Service: "svc-a", URL: srv.URL}})

	if len(snap.Services) != 1 {
		t.Fatalf("ожидали 1 результат, получили %d", len(snap.Services))
	}
	r := snap.Services[0]
	if !r.Ready {
		t.Errorf("ожидали Ready=true, got false (error=%q)", r.Error)
	}
	if r.HTTPStatus != http.StatusOK {
		t.Errorf("HTTPStatus = %d, want 200", r.HTTPStatus)
	}
	if r.Error != "" {
		t.Errorf("ожидали пустую Error, got %q", r.Error)
	}
}

func TestPollAllMarksNon200AsNotReady(t *testing.T) {
	srv := newFakeServer(t, http.StatusServiceUnavailable)
	poller := NewPoller(2*time.Second, 4)

	snap := poller.PollAll(context.Background(), []Target{{Service: "svc-down", URL: srv.URL}})

	r := snap.Services[0]
	if r.Ready {
		t.Errorf("503 должен маппиться в Ready=false")
	}
	if r.HTTPStatus != http.StatusServiceUnavailable {
		t.Errorf("HTTPStatus = %d, want 503", r.HTTPStatus)
	}
}

// TestPollAllUnreachableServiceDoesNotPanicOrHang — down-сервис (порт
// закрыт, никто не слушает) должен дать Ready=false с непустой Error, не
// панику и не зависание.
func TestPollAllUnreachableServiceDoesNotPanicOrHang(t *testing.T) {
	poller := NewPoller(1*time.Second, 4)

	done := make(chan Snapshot, 1)
	go func() {
		// Порт, на котором заведомо никто не слушает в тестовом окружении.
		done <- poller.PollAll(context.Background(), []Target{{Service: "svc-unreachable", URL: "http://127.0.0.1:1"}})
	}()

	select {
	case snap := <-done:
		if len(snap.Services) != 1 {
			t.Fatalf("ожидали 1 результат, получили %d", len(snap.Services))
		}
		r := snap.Services[0]
		if r.Ready {
			t.Errorf("недостижимый сервис не должен быть Ready")
		}
		if r.Error == "" {
			t.Errorf("ожидали непустую Error для недостижимого сервиса")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PollAll завис на недостижимом сервисе — таймаут запроса не сработал")
	}
}

// TestPollAllSlowServiceTimesOutWithoutBlockingOthers — один зависший
// сервис не должен стопорить обход остальных дольше собственного
// таймаута: с request timeout=200мс и одним сервисом, который отвечает
// через 5с, весь PollAll должен завершиться в районе 200-300мс, а не 5с+.
func TestPollAllSlowServiceTimesOutWithoutBlockingOthers(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()
	fast := newFakeServer(t, http.StatusOK)

	poller := NewPoller(200*time.Millisecond, 4)

	start := time.Now()
	snap := poller.PollAll(context.Background(), []Target{
		{Service: "slow", URL: slow.URL},
		{Service: "fast", URL: fast.URL},
	})
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("PollAll заняло %s — зависший сервис заблокировал остальных дольше собственного таймаута", elapsed)
	}

	var slowResult, fastResult *Result
	for i := range snap.Services {
		switch snap.Services[i].Service {
		case "slow":
			slowResult = &snap.Services[i]
		case "fast":
			fastResult = &snap.Services[i]
		}
	}
	if slowResult == nil || fastResult == nil {
		t.Fatalf("ожидали результаты для обоих сервисов, got %+v", snap.Services)
	}
	if slowResult.Ready {
		t.Errorf("зависший сервис не должен быть Ready (истёк таймаут)")
	}
	if !fastResult.Ready {
		t.Errorf("быстрый сервис должен быть Ready, не должен пострадать от зависшего соседа")
	}
}

// TestPollAllBoundsConcurrency — с concurrency=2 и 10 целями, каждая из
// которых блокируется, пока не увидит освобождение семафора, число
// одновременно выполняющихся запросов не должно превышать 2 в любой
// момент времени.
func TestPollAllBoundsConcurrency(t *testing.T) {
	const concurrency = 2
	const numTargets = 10

	var current int32
	var maxObserved int32
	release := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&maxObserved)
			if n <= old || atomic.CompareAndSwapInt32(&maxObserved, old, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&current, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targets := make([]Target, numTargets)
	for i := range targets {
		targets[i] = Target{Service: "svc", URL: srv.URL}
	}

	poller := NewPoller(5*time.Second, concurrency)

	done := make(chan struct{})
	go func() {
		poller.PollAll(context.Background(), targets)
		close(done)
	}()

	// Дать горутинам время выйти на плато concurrency, затем отпустить всех разом.
	time.Sleep(300 * time.Millisecond)
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PollAll не завершился после освобождения семафора")
	}

	if atomic.LoadInt32(&maxObserved) > concurrency {
		t.Errorf("наблюдали %d одновременных запросов, лимит был %d", maxObserved, concurrency)
	}
}

func TestPollAllResultsAreSortedByServiceName(t *testing.T) {
	srv := newFakeServer(t, http.StatusOK)
	poller := NewPoller(2*time.Second, 8)

	snap := poller.PollAll(context.Background(), []Target{
		{Service: "zeta", URL: srv.URL},
		{Service: "alpha", URL: srv.URL},
		{Service: "mike", URL: srv.URL},
	})

	if len(snap.Services) != 3 {
		t.Fatalf("ожидали 3 результата, получили %d", len(snap.Services))
	}
	want := []string{"alpha", "mike", "zeta"}
	for i, w := range want {
		if snap.Services[i].Service != w {
			t.Errorf("snap.Services[%d].Service = %q, want %q", i, snap.Services[i].Service, w)
		}
	}
}
