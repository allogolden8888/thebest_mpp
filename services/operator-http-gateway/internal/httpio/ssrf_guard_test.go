package httpio

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestNewClientRejectsLoopbackEndpoint — CODE_REVIEW.md MEDIUM finding:
// endpointURL раньше отправлялся в POST без единой проверки хоста; сервис
// внутри кластера мог быть перенаправлен на произвольный internal-адрес
// (loopback/private/link-local/metadata). NewClient (production-
// конструктор, в отличие от NewClientForTests) должен отвергать такие
// адреса на уровне реального TCP dial. SubmitSegment оборачивает сетевые
// ошибки в SubmitOutcome{Ambiguous} с nil Go-error (HLD §13 — "ответ не
// подтверждён", тот же путь, что таймаут/connection refused) — guard не
// исключение из этого правила, проверяем через ReasonCode, не err.
func TestNewClientRejectsLoopbackEndpoint(t *testing.T) {
	srv := httptest.NewServer(nil)
	defer srv.Close()

	client := NewClient(2 * time.Second)
	outcome, err := client.SubmitSegment(t.Context(), srv.URL, "998901234567", []byte("hello"), "GSM7", "queue-1")
	if err != nil {
		t.Fatalf("SubmitSegment вернул Go-error вместо Ambiguous outcome: %v", err)
	}
	if outcome.Status != OutcomeAmbiguous {
		t.Fatalf("ожидали Ambiguous для loopback-адреса (%s), получили %v", srv.URL, outcome.Status)
	}
	if !strings.Contains(outcome.ReasonCode, "ssrf guard") {
		t.Fatalf("ожидали, что причина отказа упоминает ssrf guard, получили %q", outcome.ReasonCode)
	}
}

// TestNewClientRejectsDisallowedScheme — checkScheme отвергает всё, кроме
// http/https, до попытки соединения; в отличие от dial-уровня, эта
// проверка происходит до httpClient.Do и возвращает настоящий Go-error.
func TestNewClientRejectsDisallowedScheme(t *testing.T) {
	client := NewClient(2 * time.Second)
	_, err := client.SubmitSegment(t.Context(), "file:///etc/passwd", "998901234567", []byte("hello"), "GSM7", "queue-1")
	if err == nil {
		t.Fatalf("ожидали ошибку ssrf guard для scheme file://")
	}
	if !strings.Contains(err.Error(), "ssrf guard") {
		t.Fatalf("ожидали упоминание ssrf guard в ошибке, получили %v", err)
	}
}
