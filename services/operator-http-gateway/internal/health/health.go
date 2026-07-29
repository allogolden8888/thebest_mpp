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

func (s *State) SetReady(v bool) { s.ready.Store(v) }
func (s *State) Ready() bool     { return s.ready.Load() }

// SetDependencyChecks регистрирует реальные health-check функции для
// внешних зависимостей (Redis route registry). CODE_REVIEW.md Low
// finding: /readyz раньше сообщал ready безусловно; неудачная
// register_route при старте только логировалась, не мешала readiness —
// под мог годами принимать трафик, ни разу не зарегистрировав маршрут.
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
		_, _ = w.Write([]byte("# HELP operator_http_gateway_up Service liveness placeholder\n# TYPE operator_http_gateway_up gauge\noperator_http_gateway_up 1\n"))
	})

	return mux
}
