package httpapi

import (
	"log"
	"net/http"
)

// internalError — CODE_REVIEW.md Low finding: голые SQL/gRPC/driver
// сообщения об ошибке утекали напрямую в HTTP-ответ (dlq.go, reconciliation.go,
// report.go и 502-пути в config.go/executioncontrol.go/replay.go). Полная
// ошибка идёт в лог сервиса (доступен оператору платформы), вызывающему —
// только generic-сообщение и статус.
func internalError(w http.ResponseWriter, status int, context string, err error) {
	log.Printf("%s: %v", context, err)
	http.Error(w, context, status)
}

// parseNonNegativeInt — CODE_REVIEW.md Low finding: отрицательный offset
// раньше не отклонялся и всплывал как сырая ошибка БД (через internalError
// теперь, но лучше отклонить на входе с понятным 400).
func parseNonNegativeInt(raw string) (int, bool) {
	n := 0
	if raw == "" {
		return 0, true
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
