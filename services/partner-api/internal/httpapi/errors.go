package httpapi

import (
	"log"
	"net/http"
)

// internalError — CODE_REVIEW.md Low finding: голые SQL/driver сообщения
// об ошибке утекали напрямую во внешний HTTP-ответ (status.go, search.go,
// report.go) — более чувствительно здесь, чем в Backoffice API, поскольку
// вызывающий находится вне организации. Полная ошибка идёт в лог сервиса,
// партнёру — только generic-сообщение и статус.
func internalError(w http.ResponseWriter, status int, context string, err error) {
	log.Printf("%s: %v", context, err)
	http.Error(w, context, status)
}

// parseNonNegativeInt — CODE_REVIEW.md Low finding: отрицательный offset
// раньше не отклонялся и всплывал как сырая ошибка БД.
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
