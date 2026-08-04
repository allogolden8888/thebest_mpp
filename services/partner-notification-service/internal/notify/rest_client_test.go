package notify

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func lifecycleEvent() *eventsv1.MessageLifecycleEvent {
	return &eventsv1.MessageLifecycleEvent{
		EventId:          "e1",
		MessageId:        "m1",
		Status:           commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_DELIVERED,
		LifecycleVersion: 2,
		Terminal:         true,
		OccurredAt:       timestamppb.New(time.Now()),
	}
}

// TestSendCallbackAgainstRealHttpServer — реальный HTTP round-trip против
// httptest.Server (настоящий net/http сервер на локальном порту, не мок
// интерфейса) — доказывает, что тело запроса реально сериализуется и
// доходит в ожидаемом формате.
func TestSendCallbackAgainstRealHttpServer(t *testing.T) {
	var received CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("не удалось декодировать тело запроса: %v", err)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newRestClientWithoutSSRFGuard(2 * time.Second)
	outcome, err := client.SendCallback(context.Background(), server.URL, lifecycleEvent())
	if err != nil {
		t.Fatalf("SendCallback: %v", err)
	}
	if outcome != OutcomeDelivered {
		t.Fatalf("outcome = %v, want OutcomeDelivered", outcome)
	}
	if received.MessageID != "m1" {
		t.Errorf("MessageID = %q, want m1", received.MessageID)
	}
	if received.Status != "MESSAGE_LIFECYCLE_STATUS_DELIVERED" {
		t.Errorf("Status = %q, unexpected", received.Status)
	}
}

func TestSendCallback5xxIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newRestClientWithoutSSRFGuard(2 * time.Second)
	outcome, err := client.SendCallback(context.Background(), server.URL, lifecycleEvent())
	if err == nil {
		t.Fatal("ожидали ошибку на 500")
	}
	if outcome != OutcomeRetryable {
		t.Fatalf("outcome = %v, want OutcomeRetryable", outcome)
	}
}

func TestSendCallback429IsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := newRestClientWithoutSSRFGuard(2 * time.Second)
	outcome, _ := client.SendCallback(context.Background(), server.URL, lifecycleEvent())
	if outcome != OutcomeRetryable {
		t.Fatalf("outcome = %v, want OutcomeRetryable (429 — специальный случай, не как остальные 4xx)", outcome)
	}
}

func TestSendCallback4xxIsPermanentFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := newRestClientWithoutSSRFGuard(2 * time.Second)
	outcome, err := client.SendCallback(context.Background(), server.URL, lifecycleEvent())
	if err == nil {
		t.Fatal("ожидали ошибку на 400")
	}
	if outcome != OutcomePermanentFailure {
		t.Fatalf("outcome = %v, want OutcomePermanentFailure", outcome)
	}
}

func TestSendCallbackUnreachableServerIsRetryable(t *testing.T) {
	client := newRestClientWithoutSSRFGuard(500 * time.Millisecond)
	outcome, err := client.SendCallback(context.Background(), "http://127.0.0.1:1", lifecycleEvent())
	if err == nil {
		t.Fatal("ожидали ошибку сети")
	}
	if outcome != OutcomeRetryable {
		t.Fatalf("outcome = %v, want OutcomeRetryable (сетевая ошибка)", outcome)
	}
}

// Прямая проверка исправления HIGH находки кодревью (SSRF): production
// конструктор (NewRestClient, guard включён) должен отвергать оба класса
// адресов, через которые кодревью продемонстрировало риск — plain http и
// loopback/private IP (тот же класс, что 169.254.169.254 metadata-эндпоинт
// или internal-сервис в кластере) — permanent, не retryable, поскольку
// повтор того же forbidden URL никогда не поможет.

func TestSendCallbackRejectsNonHttpsSchemeAsPermanentFailure(t *testing.T) {
	client := NewRestClient(2 * time.Second)
	outcome, err := client.SendCallback(context.Background(), "http://example.com/webhook", lifecycleEvent())
	if err == nil {
		t.Fatal("ожидали ошибку — http scheme должен быть отвергнут SSRF guard'ом")
	}
	if outcome != OutcomePermanentFailure {
		t.Fatalf("outcome = %v, want OutcomePermanentFailure (запрещённый scheme — повтор не поможет)", outcome)
	}
	var ssrfErr *SSRFRejectedError
	if !errors.As(err, &ssrfErr) {
		t.Fatalf("ошибка должна быть *SSRFRejectedError (через wrapping), получили: %v", err)
	}
}

func TestSendCallbackRejectsLoopbackAddressEvenWithHttpsScheme(t *testing.T) {
	// httptest.NewTLSServer даёт настоящий https://127.0.0.1:PORT — валидный
	// scheme, но loopback IP. Dial-level Control-хук должен отвергнуть его
	// ДО TLS handshake (поэтому self-signed сертификат тестового сервера
	// здесь вообще не проблема — до него дело не доходит).
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewRestClient(2 * time.Second)
	outcome, err := client.SendCallback(context.Background(), server.URL, lifecycleEvent())
	if err == nil {
		t.Fatal("ожидали ошибку — loopback IP должен быть отвергнут SSRF guard'ом")
	}
	if outcome != OutcomePermanentFailure {
		t.Fatalf("outcome = %v, want OutcomePermanentFailure (запрещённый адрес — повтор не поможет)", outcome)
	}
	var ssrfErr *SSRFRejectedError
	if !errors.As(err, &ssrfErr) {
		t.Fatalf("ошибка должна быть *SSRFRejectedError (через wrapping net.OpError/url.Error), получили: %v", err)
	}
}

func TestIsPubliclyRoutableRejectsKnownUnsafeRanges(t *testing.T) {
	unsafe := []string{
		"127.0.0.1",       // loopback
		"169.254.169.254", // cloud metadata endpoint (AWS/GCP/Azure)
		"10.0.0.1",        // RFC1918
		"172.16.0.1",      // RFC1918
		"192.168.1.1",     // RFC1918
		"0.0.0.0",         // unspecified
		"::1",             // IPv6 loopback
		"fc00::1",         // IPv6 unique-local
	}
	for _, addr := range unsafe {
		ip := net.ParseIP(addr)
		if ip == nil {
			t.Fatalf("тестовый адрес %q не парсится как IP", addr)
		}
		if isPubliclyRoutable(ip) {
			t.Errorf("isPubliclyRoutable(%s) = true, want false", addr)
		}
	}
}

func TestIsPubliclyRoutableAllowsRealPublicAddress(t *testing.T) {
	// 8.8.8.8 — публичный, не привязан к сети сессии, просто известный
	// нейтральный пример публичного адреса, без реального сетевого запроса.
	ip := net.ParseIP("8.8.8.8")
	if !isPubliclyRoutable(ip) {
		t.Errorf("isPubliclyRoutable(8.8.8.8) = false, want true")
	}
}
