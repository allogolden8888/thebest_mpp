// Package health — /healthz, /readyz, /metrics на HEALTH_PORT=9090,
// конвенция из k8s/generate_manifests.py (readiness/liveness probes,
// PodMonitor scrape target). Тот же паттерн, что во всех остальных Go
// сервисах этой сессии (execution-control-service/internal/health и др.).
package health

import (
	"net/http"
	"sync/atomic"
)

// State — общее состояние готовности сервиса. Zero value: не готов.
type State struct {
	ready atomic.Bool
}

func (s *State) SetReady(v bool) {
	s.ready.Store(v)
}

func (s *State) Ready() bool {
	return s.ready.Load()
}

// Router собирает http.ServeMux с тремя обязательными путями.
func Router(state *State) *http.ServeMux {
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
		_, _ = w.Write([]byte("not ready"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# HELP replay_service_up Service liveness placeholder\n# TYPE replay_service_up gauge\nreplay_service_up 1\n"))
	})

	return mux
}