// Package httpapi — GET /snapshot на том же mux, что /healthz|/readyz|/metrics
// (:9090). Отдаёт текущее состояние обоих снапшотов из Redis как один JSON,
// без необходимости читателю (curl, будущий backoffice-api) знать про Redis
// вообще — тот же контракт, что README.md документирует под "JSON-форма
// /snapshot".
package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"mpp/ops-visibility-service/internal/kafkalag"
	"mpp/ops-visibility-service/internal/readyz"
)

// SnapshotStore — то подмножество *store.Client, которое нужно этому
// хендлеру. Интерфейс, не конкретный тип — ради тестируемости с фейком, не
// завязываясь на реальный Redis в тестах пакета httpapi.
type SnapshotStore interface {
	ReadKafkaLag(ctx context.Context) (*kafkalag.Snapshot, error)
	ReadReadyz(ctx context.Context) (*readyz.Snapshot, error)
}

// SnapshotResponse — точная форма ответа GET /snapshot, задокументирована
// в README.md сервиса. KafkaLagAvailable/ReadyzAvailable — явные флаги, а
// не просто "поле равно null" — отсутствие данных (сервис только что
// стартовал, TTL истёк потому что цикл опроса подвис/сервис недавно упал)
// должно быть видно однозначно, не выводиться из null vs пустой структуры.
type SnapshotResponse struct {
	KafkaLag          *kafkalag.Snapshot `json:"kafka_lag"`
	KafkaLagAvailable bool               `json:"kafka_lag_available"`
	Readyz            *readyz.Snapshot   `json:"readyz"`
	ReadyzAvailable   bool               `json:"readyz_available"`
}

// SnapshotHandler читает оба ключа из Redis и отдаёт их одним ответом.
// Ошибка чтения одного из двух ключей не должна ронять весь ответ —
// частичный снапшот (например, kafka lag есть, а readyz Redis-ключ истёк)
// всё равно полезнее, чем 500 на весь эндпоинт.
func SnapshotHandler(st SnapshotStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		resp := SnapshotResponse{}

		if kl, err := st.ReadKafkaLag(ctx); err != nil {
			log.Printf("/snapshot: read kafka lag failed: %v", err)
		} else if kl != nil {
			resp.KafkaLag = kl
			resp.KafkaLagAvailable = true
		}

		if rz, err := st.ReadReadyz(ctx); err != nil {
			log.Printf("/snapshot: read readyz failed: %v", err)
		} else if rz != nil {
			resp.Readyz = rz
			resp.ReadyzAvailable = true
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
