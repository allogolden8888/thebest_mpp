// Package readyz — bounded-concurrency опрос /readyz каждого сервиса
// платформы (Фаза 8 плана: единое место, где видно готовность всех ~34
// сервисов разом, вместо ручного обхода каждого).
package readyz

import (
	"context"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Target — один опрашиваемый эндпоинт. Разделение Service (имя для
// отображения/ключей) и URL (куда реально стучаться) — специально ради
// тестируемости: в проде URL строится по конвенции
// http://<service>.mpp.svc:9090/readyz, в тестах указывает на
// httptest.Server.
type Target struct {
	Service string
	URL     string
}

// Result — итог опроса одного сервиса.
type Result struct {
	Service    string `json:"service"`
	Ready      bool   `json:"ready"`
	HTTPStatus int    `json:"http_status,omitempty"`
	LatencyMS  int64  `json:"latency_ms"`
	Error      string `json:"error,omitempty"`
}

// Snapshot — результат одного цикла опроса всех сервисов, ровно то, что
// пишется в Redis (см. internal/store) под ключом ops:readyz:snapshot.
type Snapshot struct {
	GeneratedAt time.Time `json:"generated_at"`
	Services    []Result  `json:"services"`
}

// Poller — HTTP-клиент с коротким per-request таймаутом (одна повисшая
// цель не должна стопорить весь обход) и ограничением на число
// одновременных запросов (fan-out с семафором, не последовательный обход
// ~34 сервисов).
type Poller struct {
	client      *http.Client
	concurrency int
}

func NewPoller(requestTimeout time.Duration, concurrency int) *Poller {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Poller{
		client:      &http.Client{Timeout: requestTimeout},
		concurrency: concurrency,
	}
}

// PollAll опрашивает все targets параллельно, не более p.concurrency
// одновременно. Порядок результатов детерминирован (сортировка по имени
// сервиса) независимо от порядка завершения горутин — иначе Snapshot,
// сериализованный в JSON, менялся бы по порядку полей между циклами без
// изменения данных, что мешало бы дебагу diff'ом.
func (p *Poller) PollAll(ctx context.Context, targets []Target) Snapshot {
	results := make([]Result, len(targets))
	sem := make(chan struct{}, p.concurrency)
	var wg sync.WaitGroup

	for i, t := range targets {
		wg.Add(1)
		go func(i int, t Target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = p.pollOne(ctx, t)
		}(i, t)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].Service < results[j].Service })
	return Snapshot{GeneratedAt: time.Now().UTC(), Services: results}
}

func (p *Poller) pollOne(ctx context.Context, t Target) Result {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return Result{Service: t.Service, Ready: false, LatencyMS: time.Since(start).Milliseconds(), Error: err.Error()}
	}

	resp, err := p.client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		// Таймаут/отказ в соединении/DNS — сервис недоступен, не паника и
		// не "неизвестно": явный not-ready с причиной.
		return Result{Service: t.Service, Ready: false, LatencyMS: latency, Error: err.Error()}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	return Result{
		Service:    t.Service,
		Ready:      resp.StatusCode == http.StatusOK,
		HTTPStatus: resp.StatusCode,
		LatencyMS:  latency,
	}
}
