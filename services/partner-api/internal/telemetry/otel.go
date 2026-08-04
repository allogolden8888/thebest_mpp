// Package telemetry — OpenTelemetry (services_specifictaion.md §8.2 "Стек").
// Один span на HTTP-запрос, атрибуты http.method/http.route/http.status_code
// — минимальный, но реальный SDK (не заглушка): NewProvider возвращает
// настоящий *sdktrace.TracerProvider, который можно завершить exporter'ом
// по выбору вызывающего кода (см. cmd/partner-api/main.go).
package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// NewProvider — TracerProvider с переданным SpanProcessor (в проде —
// batch OTLP exporter; в тестах — SimpleSpanProcessor поверх in-memory
// exporter, чтобы проверить реально записанные spans).
func NewProvider(processor sdktrace.SpanProcessor) *sdktrace.TracerProvider {
	res := resource.NewSchemaless(semconv.ServiceName("partner-api"))
	return sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithResource(res),
	)
}

// Middleware — chi/net-http middleware: оборачивает каждый запрос в span
// "HTTP <method> <route>".
func Middleware(tp trace.TracerProvider, routePattern func(r *http.Request) string) func(http.Handler) http.Handler {
	tracer := tp.Tracer("mpp/partner-api")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := routePattern(r)
			ctx, span := tracer.Start(r.Context(), "HTTP "+r.Method+" "+route)
			defer span.End()

			span.SetAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.route", route),
			)

			rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r.WithContext(ctx))

			span.SetAttributes(attribute.Int("http.status_code", rw.status))
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Shutdown — best-effort остановка TracerProvider (флаш экспортёра).
func Shutdown(ctx context.Context, tp *sdktrace.TracerProvider) error {
	return tp.Shutdown(ctx)
}