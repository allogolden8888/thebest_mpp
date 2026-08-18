// Package health — /healthz, /readyz, /metrics на HEALTH_PORT=9090,
// конвенция из k8s/generate_manifests.py. Тот же паттерн, что во всех
// остальных Go сервисах этой сессии (iam-service, credential-issuer-service,
// config-cache-projector).
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
// внешних зависимостей этого сервиса (здесь — Kafka и Redis, оба —
// зависимости, от доступности которых зависит, есть ли смысл вообще
// показывать этот под как ready). Тот же принцип, что у остальных
// Go-сервисов этой сессии: /readyz отражает реальное текущее состояние
// зависимостей, не статический флаг, выставленный один раз при старте.
func (s *State) SetDependencyChecks(checks map[string]func(context.Context) error) {
	s.checks = checks
}

// Router возвращает *http.ServeMux, а не запускает сервер — main.go
// добавляет к нему ещё один маршрут (/snapshot) перед тем, как забиндить
// :9090, тот же паттерн расширения, что использовался бы для любого другого
// не-health эндпоинта на этом же порту.
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
		_, _ = w.Write([]byte("# HELP ops_visibility_service_up Service liveness placeholder\n# TYPE ops_visibility_service_up gauge\nops_visibility_service_up 1\n"))
	})

	return mux
}
