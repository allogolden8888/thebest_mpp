package main

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/analytics-writer/internal/core"
)

// TestBufferRestorePreservesDataOnFailedWrite — CODE_REVIEW.md HIGH
// finding: раньше буфер очищался ДО подтверждения успешной записи в
// ClickHouse — сбой записи означал безвозвратную потерю уже накопленных
// строк. drain()+restore() — контракт, которым flush() пользуется, чтобы
// не терять данные при ошибке.
func TestBufferRestorePreservesDataOnFailedWrite(t *testing.T) {
	buf := &buffer{}
	buf.append(core.NormalizedRecord{MessageID: "m1"}, &kgo.Record{Topic: "incoming.messages"})

	rows, records := buf.drain()
	if buf.len() != 0 {
		t.Fatalf("после drain буфер должен быть пуст")
	}

	buf.restore(rows, records)

	if buf.len() != 1 {
		t.Fatalf("после restore ожидали 1 строку, получили %d", buf.len())
	}
	if buf.rows[0].MessageID != "m1" {
		t.Fatalf("данные не восстановились корректно: %+v", buf.rows)
	}
	if len(buf.records) != 1 {
		t.Fatalf("records (для будущего commit) не восстановились корректно")
	}
}

func TestBufferRestorePrependsBeforeNewlyAccumulatedData(t *testing.T) {
	buf := &buffer{}
	buf.append(core.NormalizedRecord{MessageID: "old"}, &kgo.Record{})

	rows, records := buf.drain()

	// Новые данные пришли, пока "запись" была в процессе.
	buf.append(core.NormalizedRecord{MessageID: "new"}, &kgo.Record{})

	buf.restore(rows, records)

	if buf.len() != 2 {
		t.Fatalf("ожидали 2 строки после restore+new, получили %d", buf.len())
	}
	if buf.rows[0].MessageID != "old" || buf.rows[1].MessageID != "new" {
		t.Fatalf("неверный порядок после restore: %+v", buf.rows)
	}
}

func TestBufferEmptyInitially(t *testing.T) {
	buf := &buffer{}
	if buf.len() != 0 {
		t.Fatalf("новый буфер должен быть пуст")
	}
}
