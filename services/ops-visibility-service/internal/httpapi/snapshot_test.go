package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mpp/ops-visibility-service/internal/kafkalag"
	"mpp/ops-visibility-service/internal/readyz"
)

type fakeStore struct {
	kafkaLag    *kafkalag.Snapshot
	kafkaLagErr error
	readyzSnap  *readyz.Snapshot
	readyzErr   error
}

func (f *fakeStore) ReadKafkaLag(ctx context.Context) (*kafkalag.Snapshot, error) {
	return f.kafkaLag, f.kafkaLagErr
}

func (f *fakeStore) ReadReadyz(ctx context.Context) (*readyz.Snapshot, error) {
	return f.readyzSnap, f.readyzErr
}

func doSnapshotRequest(t *testing.T, st SnapshotStore) SnapshotResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	w := httptest.NewRecorder()
	SnapshotHandler(st)(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var resp SnapshotResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v (body=%s)", err, w.Body.String())
	}
	return resp
}

func TestSnapshotHandlerBothAvailable(t *testing.T) {
	kl := &kafkalag.Snapshot{GeneratedAt: time.Now(), BootstrapServers: []string{"kafka-bootstrap.mpp.svc:9092"}}
	rz := &readyz.Snapshot{GeneratedAt: time.Now()}
	st := &fakeStore{kafkaLag: kl, readyzSnap: rz}

	resp := doSnapshotRequest(t, st)

	if !resp.KafkaLagAvailable {
		t.Errorf("KafkaLagAvailable = false, want true")
	}
	if resp.KafkaLag == nil {
		t.Errorf("KafkaLag = nil, want non-nil")
	}
	if !resp.ReadyzAvailable {
		t.Errorf("ReadyzAvailable = false, want true")
	}
	if resp.Readyz == nil {
		t.Errorf("Readyz = nil, want non-nil")
	}
}

// TestSnapshotHandlerMissingKeyIsAvailableFalseNot500 — отсутствующий ключ
// в Redis (TTL истёк, сервис только что стартовал) — это НЕ ошибка сервера,
// эндпоинт должен вернуть 200 с explicit *_available=false, не 500.
func TestSnapshotHandlerMissingKeyIsAvailableFalseNot500(t *testing.T) {
	st := &fakeStore{kafkaLag: nil, readyzSnap: nil}

	resp := doSnapshotRequest(t, st)

	if resp.KafkaLagAvailable {
		t.Errorf("KafkaLagAvailable = true для отсутствующего ключа, want false")
	}
	if resp.KafkaLag != nil {
		t.Errorf("KafkaLag должен быть nil, got %+v", resp.KafkaLag)
	}
	if resp.ReadyzAvailable {
		t.Errorf("ReadyzAvailable = true для отсутствующего ключа, want false")
	}
}

// TestSnapshotHandlerPartialFailureStillReturns200 — ошибка чтения ОДНОГО
// из двух ключей (например, транзиентная ошибка Redis на одном GET) не
// должна ронять весь ответ — второй ключ должен всё равно быть отдан.
func TestSnapshotHandlerPartialFailureStillReturns200(t *testing.T) {
	rz := &readyz.Snapshot{GeneratedAt: time.Now()}
	st := &fakeStore{kafkaLagErr: errors.New("redis timeout"), readyzSnap: rz}

	resp := doSnapshotRequest(t, st)

	if resp.KafkaLagAvailable {
		t.Errorf("KafkaLagAvailable = true при ошибке чтения, want false")
	}
	if !resp.ReadyzAvailable {
		t.Errorf("ReadyzAvailable = false — ошибка в kafka lag не должна была утащить readyz за собой")
	}
}
