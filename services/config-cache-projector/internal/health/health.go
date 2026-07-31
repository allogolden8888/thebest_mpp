// Package health — /healthz, /readyz, /metrics на HEALTH_PORT=9090,
// конвенция из k8s/generate_manifests.py. Тот же паттерн, что во всех
// остальных Go сервисах этой сессии (см. services/backoffice-api/internal/health/health.go).
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type State struct {
	ready  atomic.Bool
	checks map[string]func(context.Context) error
}

func (s *State) SetReady(v bool) {
	s.ready.Store(v)
}

func (s *State) Ready() bool {
	return s.ready.Load()
}

// SetDependencyChecks регистрирует реальные health-check функции для
// внешних зависимостей (Redis/Kafka).
// CODE_REVIEW.md finding: "/readyz never reflects real downstream health
// after startup" — раньше это был статический флаг, выставленный один раз
// при старте, до подтверждения, что подключение к брокеру вообще работает,
// и никогда не переоценивался — под мог оставаться "ready" и получать
// трафик от readiness probe, даже если Redis/Kafka давно недоступны.
func (s *State) SetDependencyChecks(checks map[string]func(context.Context) error) {
	s.checks = checks
}

func Router(state *State) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !state.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		failed := map[string]string{}
		for name, check := range state.checks {
			if err := check(ctx); err != nil {
				failed[name] = err.Error()
			}
		}
		if len(failed) > 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(struct {
				Ready  bool              `json:"ready"`
				Failed map[string]string `json:"failed_dependencies"`
			}{Ready: false, Failed: failed})
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# HELP config_cache_projector_up Service liveness placeholder\n# TYPE config_cache_projector_up gauge\nconfig_cache_projector_up 1\n"))
	})

	return mux
}
