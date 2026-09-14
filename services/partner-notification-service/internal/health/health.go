// Package health — /healthz//readyz//metrics на порту 9090, платформенная
// конвенция всех 32 сервисов.
package health

import (
	"net/http"
	"sync"
	"sync/atomic"
)

type State struct {
	ready atomic.Bool

	dependencyMu sync.RWMutex
	dependency   func() bool
}

func (s *State) SetReady(v bool) { s.ready.Store(v) }

// SetDependencyCheck installs a live readiness dependency. The callback is
// evaluated on every /readyz request: after the initial replay, a broken
// config mirror must remove the pod from service instead of leaving a
// permanently green startup latch.
func (s *State) SetDependencyCheck(check func() bool) {
	s.dependencyMu.Lock()
	s.dependency = check
	s.dependencyMu.Unlock()
}

func (s *State) Ready() bool {
	if !s.ready.Load() {
		return false
	}
	s.dependencyMu.RLock()
	check := s.dependency
	s.dependencyMu.RUnlock()
	return check == nil || check()
}

// Router — metricsHandler == nil сохраняет прежнюю заглушку ("_up 1") —
// тот же безопасный дефолт, что operator-http-gateway/internal/health
// (BACKOFFICE_ROADMAP.md P1 "Observability", main.go передаёт реальный
// promhttp.Handler(), см. internal/metrics).
func Router(state *State, metricsHandler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if state.Ready() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready or dependency unavailable"))
	})

	if metricsHandler != nil {
		mux.Handle("/metrics", metricsHandler)
	} else {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# HELP partner_notification_service_up Service liveness placeholder\n# TYPE partner_notification_service_up gauge\npartner_notification_service_up 1\n"))
		})
	}

	return mux
}
